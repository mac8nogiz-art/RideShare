import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {ObjectId} from 'mongodb';
import {getMongoDB} from "../infrastructure/mongo";

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
    private readonly MATCHED_DRIVERS_TTL = 900; // 15 min

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
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

            const radiusSteps = [3, 6, 9, 12, 15];

            for (const radius of radiusSteps) {

                const drivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zoneIds, radius);

                logger.info(`Found ${drivers.length} drivers within ${radius}km`);

                const eligibleDrivers = drivers.filter(d => !blockedSet.has(d.driverId));

                if (eligibleDrivers.length > 0) {

                    const categorizedDrivers = this.categorizeDrivers(eligibleDrivers, favoriteSet);

                    await this.storeMatchedDrivers(job.id, categorizedDrivers);

                    logger.info(`Matched ${eligibleDrivers.length} driver(s) at ${radius}km in ${Date.now() - startTime}ms`);

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
                        driverId: driverIds[i],
                        lat,
                        lng,
                        iAmBusy: driverData.iAmBusy,
                        isNew: driverData.isNew || false,
                        distance: distances[i],
                        priorityScore: driverData.priorityScore
                    };
                })
                .filter((item: any) => item !== null);

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

    private categorizeDrivers(drivers: DriverWithDistance[], favoriteSet: Set<string>): CategorizedDrivers {
        const categories = {
            favDriver: [] as Array<{id: string; dist: number}>,
            priorityDrivers: [] as Array<{id: string; dist: number}>,
            newDrivers: [] as Array<{id: string; dist: number}>,
            nonPriorityDrivers: [] as Array<{id: string; dist: number}>,
            remainingDrivers: [] as Array<{id: string; dist: number}>,
            busyDrivers: [] as Array<{id: string; dist: number}>
        };

        for (const d of drivers) {
            const obj = {id: d.driverId, dist: d.distance};

            if (favoriteSet.has(d.driverId)) {
                categories.favDriver.push(obj);
            } else if (d.priorityScore >= 80 && d.priorityScore <= 100) {
                categories.priorityDrivers.push(obj);
            } else if (d.isNew) {
                categories.newDrivers.push(obj);
            } else if (d.priorityScore >= 60 && d.priorityScore < 80) {
                categories.nonPriorityDrivers.push(obj);
            } else if (d.isBusy) {
                categories.busyDrivers.push(obj);
            } else {
                categories.remainingDrivers.push(obj);
            }
        }

        const sortByDist = (arr: Array<{id: string; dist: number}>) =>
            arr.sort((a, b) => a.dist - b.dist).map(x => x.id);

        return {
            favDriver: sortByDist(categories.favDriver),
            priorityDrivers: sortByDist(categories.priorityDrivers),
            newDrivers: sortByDist(categories.newDrivers),
            nonPriorityDrivers: sortByDist(categories.nonPriorityDrivers),
            remainingDrivers: sortByDist(categories.remainingDrivers),
            busyDrivers: sortByDist(categories.busyDrivers)
        };
    }

    private async storeMatchedDrivers(jobId: string, categorizedDrivers: CategorizedDrivers): Promise<void> {
        const rankingKey = `job:${jobId}:matched_drivers`;
        const pipeline = redis.pipeline();

        const allDrivers = [
            ...categorizedDrivers.favDriver,
            ...categorizedDrivers.priorityDrivers,
            ...categorizedDrivers.newDrivers,
            ...categorizedDrivers.nonPriorityDrivers,
            ...categorizedDrivers.remainingDrivers,
            ...categorizedDrivers.busyDrivers
        ];

        const driverKeys = allDrivers.map(id => id);
        const priorityScores: any = await redis.call('JSON.MGET', ...driverKeys, '$.priorityScore');

        for (let i = 0; i < allDrivers.length; i++) {
            const driverId = allDrivers[i];
            let score = -1;

            if (categorizedDrivers.favDriver.includes(driverId)) {
                score = 110;
            } else {
                const parsed = priorityScores[i] ? JSON.parse(priorityScores[i])?.[0] : null;
                score = parsed !== undefined && parsed !== null ? parsed : -1;
            }

            pipeline.zadd(rankingKey, score, driverId);
        }

        pipeline.expire(rankingKey, this.MATCHED_DRIVERS_TTL);
        await pipeline.exec();

        logger.info(`Stored ${allDrivers.length} matched drivers for job ${jobId}`);
    }
}