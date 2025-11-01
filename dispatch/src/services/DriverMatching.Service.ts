import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {ObjectId} from 'mongodb';
import {getMongoDB} from "../infrastructure/mongo";

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;
    private readonly MATCHED_DRIVERS_TTL = 900; // 15 min

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
    }

    async findBestDrivers(job: Job, customerId: string): Promise<string[]> {
        const startTime = Date.now();

        try {
            const [zoneIds, {favoriteSet, blockedSet}] = await Promise.all([
                this.zoneService.getZoneForJob(job),
                this.getCustomerFavoritesAndBlocked(customerId)
            ]);

            if (!zoneIds || zoneIds.length === 0) {
                logger.warn(`No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                return [];
            }

            logger.info(`Looking for drivers approved for zones: ${zoneIds.join(', ')}`);

            const radiusSteps = [3, 6, 9, 12, 15];
            let bestMatches: string[] = [];

            for (const radius of radiusSteps) {
                const drivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zoneIds, radius);
                logger.info(`Found ${drivers.length} drivers within ${radius}km`);

                const eligibleDrivers = drivers.filter(d => !blockedSet.has(d.driverId));

                if (eligibleDrivers.length > 0) {
                    const categorizedDrivers = await this.processBestDrivers(eligibleDrivers, favoriteSet);

                    // Flatten categorized drivers into priority order
                    bestMatches = [
                        ...categorizedDrivers.favDriver,
                        ...categorizedDrivers.priorityDrivers,
                        ...categorizedDrivers.newDrivers,
                        ...categorizedDrivers.nonPriorityDrivers,
                        ...categorizedDrivers.remainingDrivers,
                        ...categorizedDrivers.busyDrivers
                    ];

                    logger.info(`Matched ${bestMatches.length} driver(s) at ${radius}km in ${Date.now() - startTime}ms`);

                    // Fetch priority scores and store
                    const rankingKey = `job:${job.id}:matched_drivers`;
                    const driverKeys = bestMatches.map(id => id);
                    const priorityScores: any = await redis.call('JSON.MGET', ...driverKeys, '$.priorityScore');
                    console.log("priorityScores----->", priorityScores);

                    const pipeline = redis.pipeline();

                    for (let i = 0; i < bestMatches.length; i++) {
                        const driverId = bestMatches[i];
                        let score = -1; // Default lowest priority

                        if (favoriteSet.has(driverId)) {
                            score = 10000; // Favorites
                        } else if (priorityScores[i]) {
                            const parsed = JSON.parse(priorityScores[i])?.[0];
                            score = parsed !== undefined && parsed !== null ? parsed : -1;
                        }

                        pipeline.zadd(rankingKey, score, driverId);
                    }

                    pipeline.expire(rankingKey, this.MATCHED_DRIVERS_TTL);
                    await pipeline.exec();

                    logger.info(`Saved ${bestMatches.length} matched drivers for job ${job.id}`);
                    break;
                }
            }

            logger.info(`Finished driver matching for job ${job.id} in ${Date.now() - startTime}ms`);
            return bestMatches;
        } catch (error: any) {
            logger.error(`Driver matching failed for job ${job.id}: ${error.message}`);
            logger.error(`Stack: ${error.stack}`);
            return [];
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

            const afterParse = jsonStrings
                ?.map((item: any, i: number) => {
                    const driverData = JSON.parse(item)?.[0];

                    const driverApprovedZones = Array.isArray(driverData.approved_zone)
                        ? driverData.approved_zone.map(String)
                        : [];

                    if (driverApprovedZones.length > 0 && !jobZoneIds.some(zoneId => driverApprovedZones.includes(zoneId))) {
                        return null;
                    }

                    const [lng, lat] = driverData.location?.coordinates || [];

                    return {
                        driverId: driverIds[i],
                        lat,
                        lng,
                        score: parseInt(driverData.score || '50'),
                        iAmBusy: driverData.iAmBusy,
                        isNew: driverData.isNew || false,
                        distance: distances[i],
                        priorityScore: driverData.priorityScore
                    };
                })
                .filter((item: any) => item !== null);

            logger.debug(`Filtered to ${afterParse.length} eligible drivers`);
            return afterParse;
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

            const cursor = db.collection('users').find({_id: new ObjectId(customerId)}, {
                projection: {
                    favDrivers: 1,
                    blockDrivers: 1
                }
            });
            const users = await cursor.toArray();

            if (!users || users.length === 0) {
                logger.warn(`Customer ${customerId} not found in database`);
                return {favoriteSet: new Set(), blockedSet: new Set()};
            }

            const user = users[0];
            const favorites = Array.isArray(user?.favDrivers) ? user.favDrivers.map(String) : [];
            const blocked = Array.isArray(user?.blockDrivers) ? user.blockDrivers.map(String) : [];

            logger.info(`Fetched favorites(${favorites.length}) & blocked(${blocked.length}) for customer ${customerId}`);

            return {
                favoriteSet: new Set(favorites),
                blockedSet: new Set(blocked)
            };
        } catch (error: any) {
            logger.error(`MongoDB fetch failed for favorites/blocked - Customer: ${customerId}, Error: ${error.message}`);
            return {favoriteSet: new Set(), blockedSet: new Set()};
        }
    }

    private async processBestDrivers(drivers: DriverWithDistance[], favoriteSet: Set<string>): Promise<{
        favDriver: string[];
        priorityDrivers: string[];
        newDrivers: string[];
        nonPriorityDrivers: string[];
        remainingDrivers: string[];
        busyDrivers: string[];
    }> {
        const favDriver: any[] = [];
        const priorityDrivers: any[] = [];
        const newDrivers: any[] = [];
        const nonPriorityDrivers: any[] = [];
        const remainingDrivers: any[] = [];
        const busyDrivers: any[] = [];

        for (const d of drivers) {
            const obj = {id: d.driverId, dist: d.distance};

            if (favoriteSet.has(d.driverId)) {
                favDriver.push(obj);
            } else if (d.score >= 80 && d.score <= 100) {
                priorityDrivers.push(obj);
            } else if (d.isNew) {
                newDrivers.push(obj);
            } else if (d.score >= 60 && d.score < 80) {
                nonPriorityDrivers.push(obj);
            } else if (d.iAmBusy) {
                busyDrivers.push(obj);
            } else {
                remainingDrivers.push(obj);
            }
        }

        // Sort each category by distance
        const sortByDist = (arr: any[]) => arr.sort((a, b) => a.dist - b.dist).map(x => x.id);

        return {
            favDriver: sortByDist(favDriver),
            priorityDrivers: sortByDist(priorityDrivers),
            newDrivers: sortByDist(newDrivers),
            nonPriorityDrivers: sortByDist(nonPriorityDrivers),
            remainingDrivers: sortByDist(remainingDrivers),
            busyDrivers: sortByDist(busyDrivers)
        };
    }
}