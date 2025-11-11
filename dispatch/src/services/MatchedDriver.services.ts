import { logger } from '../logger';
import { Job } from '../types';
import { BusyDriverService } from './busyDriver.services';
import { FreeDriverService } from './freeDriverService';
import { OfferManagementService } from './OfferManagement.Service';
import { redis } from '../infrastructure/redis';
import { SpatialService } from '../infrastructure/spatial';

interface DriverWithDistance {
    driverId: string;
    distance: number;
    driverType: 'free' | 'busy';
    isSecondChance?: boolean;
    queueType: 'matched' | 'rejected';
}

interface DriverBatch {
    batchNumber: number;
    drivers: string[];
    distanceRange: string;
    sendTime: number;
    queueStats: {
        matched: number;
        rejected: number;
    };
}

interface DistanceBucket {
    rangeStart: number; // in meters
    rangeEnd: number; // in meters
    drivers: string[];
}

interface DriverEligibility {
    matchedQueue: string[];
    rejectedQueue: string[];
    finalDriverIds: string[];
}

interface RedisDriverData {
    location?: {
        coordinates: number[];
    };
}

export class MatchedDriverService {
    private readonly BATCH_TIME_INTERVAL = 45_000;
    private readonly BUCKET_DISTANCE_STEP_METERS = 500; // 500m buckets
    private readonly INITIAL_BATCH_SIZE = 3;
    private readonly SUBSEQUENT_BATCH_SIZE = 4;
    private readonly OFFER_EXPIRY_SECONDS = 15;
    private readonly MAX_DISTANCE_KM = 15;

    constructor(
        private readonly busyDriverService: BusyDriverService,
        private readonly freeDriverService: FreeDriverService,
        private readonly offerManagementService: OfferManagementService,
        private readonly spatialService: SpatialService = new SpatialService()
    ) {}

    /**
     * Main method to get matched drivers for a job with 500m distance buckets
     */
    async getMatchedDriversForJob(job: Job, customerId: string): Promise<DistanceBucket[]> {
        const startTime = performance.now();
        logger.info(`Searching matched drivers for job ${job.id}`);

        try {
            // Step 1: Get all potential drivers
            const allDriverIds = await this.getAllPotentialDrivers(job, customerId);
            if (!allDriverIds.length) {
                logger.warn(`No drivers found for matched driver search - Job ${job.id}`);
                return [];
            }

            // Step 2: Categorize drivers into matched and rejected queues
            const driverEligibility = await this.categorizeDrivers(job.id, allDriverIds);

            // Step 3: Check if we have any eligible drivers
            if (!driverEligibility.finalDriverIds.length) {
                logger.warn(`All drivers exhausted for matched driver search - Job ${job.id}`);
                return [];
            }

            // Step 4: Handle second chance drivers
            await this.markSecondChanceDrivers(job.id, driverEligibility.rejectedQueue);

            // Step 5: Enrich drivers with distance information
            const enrichedDrivers = await this.enrichDriversWithDistance(
                driverEligibility.finalDriverIds,
                job.pickupLat,
                job.pickupLng,
                driverEligibility.matchedQueue,
                driverEligibility.rejectedQueue
            );

            // Step 6: Create 500m distance buckets
            const buckets = this.createDistanceBuckets(enrichedDrivers);
            const matchedDriverIds = enrichedDrivers
                .filter(d => d.queueType === 'matched')
                .map(d => d.driverId);

            await this.pushMatchedDriversToQueue(job.id, matchedDriverIds);

            logger.info(`Created ${buckets.length} distance buckets (500m each) for job ${job.id} with ${enrichedDrivers.length} drivers in ${Math.round(performance.now() - startTime)}ms`);
            return buckets;

        } catch (error: any) {
            logger.error(`Matched driver search failed for job ${job.id}: ${error.message}`);
            return [];
        }
    }

    /**
     * Get all potential drivers (free + busy) for the job
     */
    private async getAllPotentialDrivers(job: Job, customerId: string): Promise<string[]> {
        const [freeDrivers, busyDrivers] = await Promise.all([
            this.freeDriverService.getFreeDriversForJob(job, customerId),
            this.busyDriverService.getBusyDriversForJob(job, customerId),
        ]);

        const allDrivers = [...(freeDrivers || []), ...(busyDrivers || [])];
        logger.info(`Matched driver pool: ${freeDrivers?.length || 0} free and ${busyDrivers?.length || 0} busy drivers`);

        return allDrivers;
    }

    /**
     * Categorize drivers into matched and rejected queues based on eligibility
     */
    private async categorizeDrivers(jobId: string, driverIds: string[]): Promise<DriverEligibility> {
        const [rejectedDrivers, expiredDrivers, secondChanceDrivers] = await Promise.all([
            redis.smembers(`job:${jobId}:rejected_drivers`),
            redis.smembers(`job:${jobId}:expired_drivers`),
            redis.smembers(`job:${jobId}:second_chance_given`)
        ]);

        const rejectedSet = new Set(rejectedDrivers);
        const expiredSet = new Set(expiredDrivers);
        const secondChanceSet = new Set(secondChanceDrivers);

        const matchedQueue: string[] = [];
        const rejectedQueue: string[] = [];

        for (const driverId of driverIds) {
            const isRejected = rejectedSet.has(driverId);
            const isExpired = expiredSet.has(driverId);
            const hasSecondChance = secondChanceSet.has(driverId);

            if (this.isEligibleForSecondChance(isRejected, isExpired, hasSecondChance)) {
                rejectedQueue.push(driverId);
            } else if (this.isRegularEligible(isRejected, isExpired, hasSecondChance)) {
                matchedQueue.push(driverId);
            }
        }

        logger.info(`Matched driver categorization - Fresh: ${matchedQueue.length}, Second-chance: ${rejectedQueue.length}`);

        return {
            matchedQueue,
            rejectedQueue,
            finalDriverIds: [...matchedQueue, ...rejectedQueue]
        };
    }

    /**
     * Check if driver is eligible for second chance
     */
    private isEligibleForSecondChance(isRejected: boolean, isExpired: boolean, hasSecondChance: boolean): boolean {
        return (isRejected || isExpired) && !hasSecondChance;
    }

    /**
     * Check if driver is regularly eligible
     */
    private isRegularEligible(isRejected: boolean, isExpired: boolean, hasSecondChance: boolean): boolean {
        return !isRejected && !isExpired && !hasSecondChance;
    }

    /**
     * Mark second chance drivers in Redis
     */
    private async markSecondChanceDrivers(jobId: string, rejectedQueue: string[]): Promise<void> {
        if (rejectedQueue.length > 0) {
            await redis.sadd(`job:${jobId}:second_chance_given`, ...rejectedQueue);
            logger.info(`Gave second chance to ${rejectedQueue.length} drivers in matched pool`);
        }
    }

    /**
     * Enrich drivers with distance information
     */
    private async enrichDriversWithDistance(
        driverIds: string[],
        jobLat: number,
        jobLng: number,
        matchedQueue: string[],
        rejectedQueue: string[]
    ): Promise<DriverWithDistance[]> {
        const enrichmentPromises = driverIds.map(async (driverId) => {
            const distance = await this.calculateDriverDistance(driverId, jobLat, jobLng);
            if (distance === null) return null;

            const isInMatchedQueue = matchedQueue.includes(driverId);

            return {
                driverId,
                distance,
                driverType: 'free' as const,
                isSecondChance: !isInMatchedQueue,
                queueType: isInMatchedQueue ? 'matched' as const : 'rejected' as const
            };
        });

        const results = await Promise.all(enrichmentPromises);
        // @ts-ignore
        const validDrivers = results.filter((driver): driver is DriverWithDistance => driver !== null);

        // @ts-ignore
        return this.sortDriversByDistance(validDrivers);
    }

    /**
     * Calculate distance between driver and job location
     */
    private async calculateDriverDistance(driverId: string, jobLat: number, jobLng: number): Promise<number | null> {
        try {
            const data = await redis.call('JSON.GET', `driver:${driverId}`, '$');
            if (!data) {
                logger.warn(`No data found for driver ${driverId}`);
                return null;
            }

            const parsedData = JSON.parse(data as string) as RedisDriverData[];
            const driverData = parsedData?.[0];

            if (!driverData?.location?.coordinates?.length) {
                logger.warn(`No location coordinates for driver ${driverId}`);
                return null;
            }

            const [lng, lat] = driverData.location.coordinates;
            const distance = this.spatialService.calculateDistance(lat, lng, jobLat, jobLng);

            return distance;

        } catch (error: any) {
            logger.warn(`Distance calculation failed for driver ${driverId}: ${error.message}`);
            return null;
        }
    }

    /**
     * Sort drivers by distance in ascending order
     */
    private sortDriversByDistance(drivers: DriverWithDistance[]): DriverWithDistance[] {
        return drivers.sort((a, b) => a.distance - b.distance);
    }


    private createDistanceBuckets(drivers: DriverWithDistance[]): DistanceBucket[] {
        if (!drivers.length) {
            logger.info('No drivers available for bucketing');
            return [];
        }

        const buckets: DistanceBucket[] = [];
        const maxDistanceMeters = this.MAX_DISTANCE_KM * 1000;

        this.logBucketStatistics(drivers);


        let currentBucketStart = 0;

        while (currentBucketStart <= maxDistanceMeters) {
            const bucketEnd = currentBucketStart + this.BUCKET_DISTANCE_STEP_METERS;

            const driversInBucket = drivers
                .filter(driver => {
                    const distanceMeters = driver.distance * 1000;
                    return distanceMeters >= currentBucketStart && distanceMeters < bucketEnd;
                })
                .map(d => d.driverId);

            if (driversInBucket.length > 0) {
                buckets.push({
                    rangeStart: currentBucketStart,
                    rangeEnd: bucketEnd,
                    drivers: driversInBucket
                });

                logger.info(`Bucket ${buckets.length}: ${driversInBucket.length} drivers (${currentBucketStart}-${bucketEnd}m)`);
            }

            currentBucketStart = bucketEnd;
        }

        // Handle drivers beyond max distance
        const remainingDrivers = drivers
            .filter(driver => driver.distance * 1000 >= maxDistanceMeters)
            .map(d => d.driverId);

        if (remainingDrivers.length > 0) {
            buckets.push({
                rangeStart: maxDistanceMeters,
                rangeEnd: Infinity,
                drivers: remainingDrivers
            });

            logger.info(`Bucket ${buckets.length} (overflow): ${remainingDrivers.length} drivers (>${maxDistanceMeters}m)`);
        }

        const totalDrivers = buckets.reduce((sum, bucket) => sum + bucket.drivers.length, 0);
        logger.info(`Created ${buckets.length} buckets with ${totalDrivers} total drivers`);

        return buckets;

    }
    /**
     * Push matched drivers to Redis queue
     */
    private async pushMatchedDriversToQueue(jobId: string, drivers: string[]): Promise<void> {
        if (!drivers.length) return;

        const queueKey = `job:${jobId}:matched_driver_queue`;

        try {
            await redis.del(queueKey); // clear any old data
            await redis.rpush(queueKey, ...drivers);
            logger.info(`Stored ${drivers.length} matched drivers in Redis queue: ${queueKey}`);
        } catch (error: any) {
            logger.error(`Failed to push matched drivers to queue ${queueKey}: ${error.message}`);
        }
    }


    /**
     * Log bucket creation statistics
     */
    private logBucketStatistics(drivers: DriverWithDistance[]): void {
        const matchedCount = drivers.filter(d => d.queueType === 'matched').length;
        const rejectedCount = drivers.filter(d => d.queueType === 'rejected').length;
        const totalDistance = drivers.reduce((sum, driver) => sum + driver.distance, 0);
        const avgDistance = totalDistance / drivers.length;

        logger.info(`Bucket statistics - Fresh: ${matchedCount}, Second-chance: ${rejectedCount}, Avg distance: ${avgDistance.toFixed(2)}km`);
    }
}