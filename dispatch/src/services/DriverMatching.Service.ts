import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {MongoClient, ObjectId} from 'mongodb';

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;
    private readonly MATCHED_DRIVERS_TTL = 900; // 15 min
    private mongoClient: MongoClient;

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService, mongoClient: MongoClient) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
        this.mongoClient = mongoClient;
    }

    async findBestDrivers(job: Job, customerId: string): Promise<string[]> {
        const startTime = Date.now();

        try {
            const zone = await this.zoneService.getZoneForJob(job);
            if (!zone) {
                logger.warn(`No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                return [];
            }

            logger.info(`Job ${job.id} in zone ${zone.name} (${zone._id})`);

            const allMatchedDrivers = new Map<string, DriverWithDistance>();

            //  Get favorites & blocked drivers from MongoDB
            const {favoriteSet, blockedSet} = await this.getCustomerFavoritesAndBlocked(customerId);

            const radiusSteps = [3, 6, 9, 12, 15];
            let bestMatches: string[] = [];

            for (const radius of radiusSteps) {
                const drivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, radius);

                // Skip blocked drivers immediately
                const eligibleDrivers = drivers.filter(d => !blockedSet.has(d.driverId));

                for (const driver of eligibleDrivers) {
                    if (!allMatchedDrivers.has(driver.driverId)) {
                        allMatchedDrivers.set(driver.driverId, driver);
                    }
                }

                if (allMatchedDrivers.size > 0) {
                    bestMatches = this.processBestDrivers(Array.from(allMatchedDrivers.values()), favoriteSet);

                    logger.info(`Matched ${bestMatches.length} driver(s) at ${radius}km in ${Date.now() - startTime}ms`);

                    // Save ranked drivers in Redis
                    const rankingKey = `job:${job.id}:matched_drivers`;
                    const pipeline = redis.pipeline();

                    bestMatches.forEach((driverId) => {
                        const driverData = allMatchedDrivers.get(driverId);
                        const score = driverData?.score || 0;
                        pipeline.zadd(rankingKey, score, driverId);
                    });

                    pipeline.expire(rankingKey, this.MATCHED_DRIVERS_TTL);
                    await pipeline.exec();

                    logger.info(`Saved ${bestMatches.length} matched drivers for job ${job.id}`);
                    break; // stop after first successful radius
                }
            }

            logger.info(`Finished driver matching for job ${job.id} in ${Date.now() - startTime}ms`);
            return bestMatches;
        } catch (error) {
            logger.error(`Driver matching failed for job ${job.id}: ${error}`);
            return [];
        }
    }


    private async getNearbyDriversInZone(jobLat: number, jobLng: number, zoneId: string, radiusKm: number): Promise<DriverWithDistance[]> {
        const nearbyDrivers: DriverWithDistance[] = [];

        try {
            const result = (await redis.geosearch('drivers:locations', 'FROMLONLAT', jobLng, jobLat, 'BYRADIUS', radiusKm, 'km', 'WITHDIST', 'ASC')) as any;

            if (!result || result.length === 0) return nearbyDrivers;

            const driverIds: string[] = [];
            const distances: number[] = [];
            const isNestedArray = Array.isArray(result[0]);

            if (isNestedArray) {
                result.forEach((item: any) => {
                    if (Array.isArray(item) && item.length >= 2) {
                        const driverId = item[0];
                        const distance = parseFloat(item[1]);
                        if (driverId && !isNaN(distance) && distance <= radiusKm) {
                            driverIds.push(driverId);
                            distances.push(distance);
                        }
                    }
                });
            } else {
                for (let i = 0; i < result.length; i += 2) {
                    const driverId = result[i];
                    const distance = parseFloat(result[i + 1]);
                    if (driverId && !isNaN(distance) && distance <= radiusKm) {
                        driverIds.push(driverId);
                        distances.push(distance);
                    }
                }
            }

            const pipeline = redis.pipeline();
            driverIds.forEach((driverId) => pipeline.call('JSON.GET', `driver:${driverId}`));
            const results = await pipeline.exec();

            // Ensure we always have an array, even if null is returned
            if (!results || !Array.isArray(results)) {
                logger.warn('Redis pipeline returned null or invalid result');
                return nearbyDrivers;
            }

            for (let i = 0; i < driverIds.length; i++) {
                const driverId = driverIds[i];
                const distance = distances[i];
                const driverJson = results[i][1];

                if (!driverJson) continue;

                try {
                    const driverData = typeof driverJson === 'string' ? JSON.parse(driverJson) : driverJson;
                    const coordinates = driverData.location?.coordinates || [];
                    if (!coordinates.length) continue;

                    const [lng, lat] = coordinates;
                    const isBusy = driverData.iAmBusy === true;
                    const score = parseInt(driverData.score || '50');
                    const approvedZones = Array.isArray(driverData.approved_zone) ? driverData.approved_zone : [];

                    if (approvedZones.length && !approvedZones.includes(zoneId)) continue;

                    nearbyDrivers.push({
                        driverId,
                        lat,
                        lng,
                        score,
                        isBusy,
                        isNew: false,
                        lastUpdate: Date.now(),
                        approvedZones,
                        distance,
                        priority: 0
                    });
                } catch (err) {
                    logger.error(`${driverId}: JSON parse error - ${err}`);
                }
            }
            return nearbyDrivers;
        } catch (error) {
            logger.error(`GEOSEARCH failed - Radius: ${radiusKm}km, Error: ${error}`);
            return nearbyDrivers;
        }
    }

    private async getCustomerFavoritesAndBlocked(customerId: string): Promise<{
        favoriteSet: Set<string>;
        blockedSet: Set<string>;
    }> {
        try {
            const db = this.mongoClient.db('ridesharing_test');
            const cursor = db.collection('users').find(
                { _id: new ObjectId(customerId) },
                { projection: { favDriver: 1, blockedDrivers: 1 } }
            );
            const users = await cursor.toArray();
            const user = users[0];
            const favorites = Array.isArray(user?.favdriver)
                ? user.favdriver.map(String)
                : [];
            const blocked = Array.isArray(user?.blockedDrivers)
                ? user.blockedDrivers.map(String)
                : [];
            logger.info(
                `Fetched favorites(${favorites.length}) & blocked(${blocked.length}) for customer ${customerId}`
            );
            return {
                favoriteSet: new Set(favorites),
                blockedSet: new Set(blocked)
            };
        } catch (error) {
            logger.error(
                `MongoDB fetch failed for favorites/blocked - Customer: ${customerId}, Error: ${error}`
            );
            return { favoriteSet: new Set(), blockedSet: new Set() };
        }
    }

    private processBestDrivers(drivers: DriverWithDistance[], favoriteSet: Set<string>): string[] {
        return drivers
            .map((driver) => ({
                driverId: driver.driverId,
                priority: this.calculateDriverPriority(driver, favoriteSet.has(driver.driverId)),
                distance: driver.distance
            }))
            .filter((d) => d.priority > 0)
            .sort((a, b) => (b.priority !== a.priority ? b.priority - a.priority : a.distance - b.distance))
            .map((d) => d.driverId);
    }


    private calculateDriverPriority(driver: DriverWithDistance, isFavorite: boolean): number {
        const {distance, score, isNew, isBusy} = driver;
        let priority = 0;

        priority += Math.max(0, 15 - distance) * 30;
        priority += score * 5;
        if (isFavorite) priority += 300;
        if (isNew && distance <= 3) priority += 150;
        if (isBusy) priority -= 200;
        if (distance > 3) priority -= Math.min(300, (distance - 3) * 20);

        return Math.max(0, Math.round(priority));
    }
}





