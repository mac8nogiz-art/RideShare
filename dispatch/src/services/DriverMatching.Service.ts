import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {ObjectId} from 'mongodb';
import {getMongoDB} from "../infrastructure/mongo";
import {MapboxService} from './MapboxService';

interface CategorizedDrivers {
    favDriver: string[];
    priorityDrivers: string[];
    newDrivers: string[];
    nonPriorityDrivers: string[];
    remainingDrivers: string[];
    busyDrivers: string[];
}

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;
    private mapboxService: MapboxService;
    private readonly MATCHED_DRIVERS_TTL = 900; // 15 min

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
        this.mapboxService = new MapboxService();
    }

    async findBestDrivers(job: Job, customerId: string): Promise<CategorizedDrivers | null> {
        const startTime = Date.now();

        try {
            const [zoneIds, {favoriteSet, blockedSet}] = await Promise.all([
                this.zoneService.getZoneForJob(job),
                this.getCustomerFavoritesAndBlocked(customerId)
            ]);

            if (!zoneIds || zoneIds.length === 0) {
                logger.warn(`No zone found for job ${job.id}`);
                return null;
            }

            logger.info(`Looking for drivers in zones: ${zoneIds.join(', ')}`);

            const radiusSteps = [2, 4, 6];

            for (const radius of radiusSteps) {
                const drivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zoneIds, radius);

                logger.info(`Found ${drivers.length} drivers within ${radius}km`);

                const eligibleDrivers = drivers.filter(d => !blockedSet.has(d.driverId));

                if (eligibleDrivers.length > 0) {
                    // Pass radius to categorization to apply filtering rules
                    const categorizedDrivers = await this.categorizeDrivers(
                        eligibleDrivers,
                        favoriteSet,
                        job.id,
                        radius,
                        job.pickupLat,
                        job.pickupLng
                    );

                    logger.info(`Matched ${eligibleDrivers.length} driver(s) at ${radius}km in ${Date.now() - startTime}ms`);

                    // @ts-ignore
                    return categorizedDrivers;
                }
            }

            logger.info(`No drivers found for job ${job.id} in ${Date.now() - startTime}ms`);
            return null;
        } catch (error: any) {
            logger.error(`Driver matching failed for job ${job.id}: ${error.message}`);
            return null;
        }
    }

    private async getNearbyDriversInZone(
        jobLat: number,
        jobLng: number,
        jobZoneIds: string[],
        radiusKm: number
    ): Promise<DriverWithDistance[]> {
        try {
            const result = (await redis.geosearch(
                'drivers:locations',
                'FROMLONLAT',
                jobLng,
                jobLat,
                'BYRADIUS',
                radiusKm,
                'km',
                'WITHDIST',
                'ASC'
            )) as any;

            const driverIds = result.map((item: any) => `driver:${item[0]}`);
            const distances = result.map((item: any) => parseFloat(item[1]));

            const jsonStrings: any = await redis.call('JSON.MGET', ...driverIds, "$");

            const drivers = jsonStrings
                .map((item: any, i: number) => {
                    const driverData = JSON.parse(item)?.[0];
                    if (!driverData) return null;

                    const driverApprovedZones = Array.isArray(driverData.approved_zones)
                        ? driverData.approved_zones.map(String)
                        : [];

                    if (driverApprovedZones.length > 0 &&
                        !jobZoneIds.some(zoneId => driverApprovedZones.includes(zoneId))) {
                        return null;
                    }

                    const [lng, lat] = driverData.location?.coordinates || [];

                    return {
                        driverId: driverIds[i].replaceAll('driver:',''),
                        lat,
                        lng,
                        iAmBusy: driverData.iAmBusy,
                        isNew: driverData.isNew || false,
                        distance: distances[i],
                        priorityScore: driverData.priorityScore
                    };
                })
                .filter((item: any) => item !== null);
            console.log("drivers----->", drivers);

            return drivers;
        } catch (error: any) {
            logger.error(`GEOSEARCH failed - Radius: ${radiusKm}km, Error: ${error.message}`);
            return [];
        }
    }

    private async getCustomerFavoritesAndBlocked(customerId: string): Promise<{
        favoriteSet: Set<string>;
        blockedSet: Set<string>;
    }> {
        try {
            const db = getMongoDB();
            const user = await db.collection('users').findOne(
                {_id: new ObjectId(customerId)},
                {projection: {favDrivers: 1, blockDrivers: 1}}
            );

            if (!user) {
                return {favoriteSet: new Set(), blockedSet: new Set()};
            }

            const favorites = Array.isArray(user?.favDrivers) ? user.favDrivers.map(String) : [];
            const blocked = Array.isArray(user?.blockDrivers) ? user.blockDrivers.map(String) : [];

            return {
                favoriteSet: new Set(favorites),
                blockedSet: new Set(blocked)
            };
        } catch (error: any) {
            logger.error(`Failed to fetch customer preferences: ${error.message}`);
            return {favoriteSet: new Set(), blockedSet: new Set()};
        }
    }

    private async categorizeDrivers(
        drivers: DriverWithDistance[],
        favoriteSet: Set<string>,
        jobId: string,
        radius: number,
        pickupLat: number,
        pickupLng: number
    ){
        const categories = {
            favDriver: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>,
            priorityDrivers: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>,
            newDrivers: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>,
            nonPriorityDrivers: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>,
            remainingDrivers: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>,
            busyDrivers: [] as Array<{ id: string; dist: number; category: string; lat: number; lng: number }>
        };

        console.log(drivers, "drivers-------->");

        for (const d of drivers) {
            const obj = { id: d.driverId, dist: d.distance, lat: d.lat, lng: d.lng };

            if (favoriteSet.has(d.driverId)) {
                categories.favDriver.push({...obj, category: 'favDriver'});
            } else if (d.priorityScore >= 80 && d.priorityScore <= 100) {
                categories.priorityDrivers.push({...obj, category: 'priorityDrivers'});
            } else if (d.isNew) {
                categories.newDrivers.push({...obj, category: 'newDrivers'});
            } else if (d.priorityScore >= 60 && d.priorityScore < 80) {
                categories.nonPriorityDrivers.push({...obj, category: 'nonPriorityDrivers'});
            } else {
                // Only add to remainingDrivers
                if (radius === 6) {
                    categories.remainingDrivers.push({...obj, category: 'remainingDrivers'});
                }
            }


        }

        const sortByDist = (arr: Array<{ id: string; dist: number }>) =>
            arr.sort((a, b) => a.dist - b.dist);

        const categorizedDrivers = {
            favDriver: sortByDist(categories.favDriver),
            priorityDrivers: sortByDist(categories.priorityDrivers),
            newDrivers: sortByDist(categories.newDrivers),
            nonPriorityDrivers: sortByDist(categories.nonPriorityDrivers),
            remainingDrivers: sortByDist(categories.remainingDrivers),
            busyDrivers: sortByDist(categories.busyDrivers)
        };

        console.log("-------->categorized", categorizedDrivers);

        let allDrivers: any = [
            ...categorizedDrivers.favDriver,
            ...categorizedDrivers.priorityDrivers,
            ...categorizedDrivers.newDrivers,
            ...categorizedDrivers.nonPriorityDrivers,
            ...categorizedDrivers.remainingDrivers,
            ...categorizedDrivers.busyDrivers
        ];

        // Sort first 4 drivers using Mapbox API
        if (allDrivers.length > 0) {
            const firstFourDrivers = allDrivers.slice(0, Math.min(4, allDrivers.length));
            const remainingDriversAfterFour = allDrivers.slice(4);

            logger.info(`Sorting first ${firstFourDrivers.length} drivers using Mapbox API`);

            const driversWithMapboxDistance = await Promise.all(
                firstFourDrivers.map(async (driver: any) => {
                    try {
                        const result = await this.mapboxService.getDistanceAndDuration(
                            pickupLat,
                            pickupLng,
                            driver.lat,
                            driver.lng
                        );
                        return {
                            ...driver,
                            mapboxDistance: result.distanceKm,
                            mapboxDuration: result.durationMin
                        };
                    } catch (error: any) {
                        logger.warn(`Failed to get Mapbox distance for driver ${driver.id}: ${error.message}, using straight-line distance`);
                        return {
                            ...driver,
                            mapboxDistance: driver.dist,
                            mapboxDuration: null
                        };
                    }
                })
            );

            driversWithMapboxDistance.sort((a, b) => a.mapboxDistance - b.mapboxDistance);

            logger.info(`Sorted first 4 drivers by Mapbox distance`);

            allDrivers = [...driversWithMapboxDistance, ...remainingDriversAfterFour];
        }

        console.log("-------->final allDrivers after Mapbox sorting", allDrivers);


        const driverQueueKey = `job:${jobId}:driver_queue`;
        const pipeline = redis.pipeline();
        pipeline.del(driverQueueKey);

        for (const driver of allDrivers) {
            const driverHashKey = `job:${jobId}:driver:${driver?.id || ''}`;

            pipeline.hset(driverHashKey, {
                driverId: driver.id,
                status: "pending",
                category: driver.category,
                mapboxDistance: driver.mapboxDistance?.toString() || driver.dist.toString(),
                mapboxDuration: driver.mapboxDuration?.toString() || ''
            });

            pipeline.rpush(driverQueueKey, driver.id);
        }

        logger.info(`Total drivers found: ${allDrivers.length} and stored in ${driverQueueKey}`);

        await pipeline.exec();

        return categorizedDrivers;
    }
}