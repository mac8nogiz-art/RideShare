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
}

interface DriverBatch {
    batchNumber: number;
    drivers: string[];
    distanceRange: string;
    sendTime: number;
}

interface DistanceBucket {
    rangeStart: number;
    rangeEnd: number;
    drivers: string[];
}

interface RedisDriverData {
    location?: { coordinates: number[] };
}

export class MatchedDriverService {
    private readonly BATCH_TIME_INTERVAL = 45_000;
    private readonly BUCKET_DISTANCE_STEP_METERS = 500;
    private readonly MIN_BATCH_SIZE = 1;
    private readonly MAX_BATCH_SIZE = 5;
    private readonly MAX_DISTANCE_KM = 15;



    private activeBatchJobs = new Map<string, NodeJS.Timeout>();

    constructor(
        private readonly busyDriverService: BusyDriverService,
        private readonly freeDriverService: FreeDriverService,
        private readonly offerManagementService: OfferManagementService,
        private readonly spatialService: SpatialService = new SpatialService()
    ) {}

    async triggerMatchedDriverFlow(job: Job | null, jobId: string): Promise<void> {
        try {
            if (!job) {
                await this.triggerFallbackFlow(jobId);
                return;
            }

            logger.info(`Starting MATCHED DRIVER FLOW for Job ${job.id}`);

            const distanceBuckets = await this.getMatchedDriversForJob(job, job.customerId);

            if (!distanceBuckets.length) {
                logger.warn(`No matched drivers for Job ${job.id}`);
                await redis.del(`job:${job.id}:matched_flow_active`);
                return;
            }

            // Process batches from distance buckets
            await this.processBatchedOffers(job, distanceBuckets);
        } catch (error: any) {
            logger.error(`Matched driver flow failed for Job ${jobId}: ${error.message}`);
            throw error;
        }
    }

    private async triggerFallbackFlow(jobId: string): Promise<void> {
        logger.warn(` No booking data - Using FREE DRIVERS fallback for Job ${jobId}`);

        try {
            const freeDrivers = await this.getFreeDriversWithinRadius(jobId);
            if (!freeDrivers.length) {
                await redis.del(`job:${jobId}:matched_flow_active`);
                return;
            }

            const minimalJob: Job = {
                id: jobId,
                customerId: 'unknown',
                pickupLat: 0,
                pickupLng: 0,
                fare: 0,
                vehicleType: 'Unknown',
                timestamp: Date.now(),
                customer: { fullName: 'Customer', avatar: '', distance: 0, time: 'N/A' },
                rideDetails: { estimatedTime: 'N/A', estimatedDistance: 0 }
            };

            await this.sendBatchedOffersFallback(jobId, minimalJob, freeDrivers);
        } catch (error: any) {
            logger.error(`Fallback flow failed for Job ${jobId}: ${error.message}`);
            await redis.del(`job:${jobId}:matched_flow_active`);
        }
    }

    private async getFreeDriversWithinRadius(jobId: string): Promise<string[]> {
        try {
            const result = await redis.geosearch(
                'drivers:locations',
                'FROMLONLAT',
                76.6973,
                30.7178,
                'BYRADIUS',
                15,
                'km',
                'WITHDIST',
                'ASC'
            ) as any;

            if (!result?.length) return [];

            return result.map((item: any) => item[0]);
        } catch (error: any) {
            logger.error(`Failed to get free drivers: ${error.message}`);
            return [];
        }
    }

    private async sendBatchedOffersFallback(jobId: string, job: Job, drivers: string[]): Promise<void> {
        let currentIndex = 0;
        let batchNumber = 0;

        const sendNext = async () => {
            try {
                const jobStatus = await redis.get(`job:${jobId}:status`);
                if (jobStatus === 'assigned' || jobStatus === 'cancelled') {
                    await redis.del(`job:${jobId}:matched_flow_active`);
                    return;
                }

                if (currentIndex >= drivers.length) {
                    await redis.del(`job:${jobId}:matched_flow_active`);
                    return;
                }

                batchNumber++;
                const batchSize = Math.min(this.MAX_BATCH_SIZE, drivers.length - currentIndex);
                const batch = drivers.slice(currentIndex, currentIndex + batchSize);
                currentIndex += batchSize;

                logger.info(`Sending fallback batch ${batchNumber} with ${batch.length} drivers for Job ${jobId}`);
                await this.offerManagementService.sendOffers(job, batch);

                if (currentIndex < drivers.length) {
                    const timeout = setTimeout(sendNext, this.BATCH_TIME_INTERVAL);
                    this.activeBatchJobs.set(jobId, timeout);
                } else {
                    await redis.del(`job:${jobId}:matched_flow_active`);
                }
            } catch (error: any) {
                logger.error(`Fallback batch error for Job ${jobId}: ${error.message}`);
                await redis.del(`job:${jobId}:matched_flow_active`);
            }
        };

        await sendNext();
    }

    async getMatchedDriversForJob(job: Job, customerId: string): Promise<DistanceBucket[]> {
        try {
            // Get all potential drivers (free + busy)
            const allDriverIds = await this.getAllPotentialDrivers(job, customerId);
            if (!allDriverIds.length) {
                logger.warn(`No drivers available for Job ${job.id}`);
                return [];
            }

            logger.info(`Found ${allDriverIds.length} potential drivers for Job ${job.id}`);

            // Enrich with distances
            const enrichedDrivers = await this.enrichDriversWithDistance(
                allDriverIds,
                job.pickupLat,
                job.pickupLng
            );

            if (!enrichedDrivers.length) {
                logger.warn(`No drivers with valid locations for Job ${job.id}`);
                return [];
            }

            // Create distance buckets (500m intervals)
            const buckets = this.createDistanceBuckets(enrichedDrivers);

            logger.info(`Created ${buckets.length} distance buckets for Job ${job.id}`);

            return buckets;
        } catch (error: any) {
            logger.error(`Matched driver search failed for ${job.id}: ${error.message}`);
            return [];
        }
    }

    async processBatchedOffers(job: Job, buckets: DistanceBucket[]): Promise<void> {
        if (!buckets.length) {
            await redis.del(`job:${job.id}:matched_flow_active`);
            return;
        }

        await redis.setex(`job:${job.id}:matched_flow_active`, 3600, '1');

        let batchNumber = 0;
        let currentBucketIndex = 0;
        let usedFromBucket = 0;

        const processBatch = async () => {
            try {
                const status = await redis.get(`job:${job.id}:status`);
                if (status === 'assigned' || status === 'cancelled') {
                    logger.info(`Job ${job.id} ${status} - stopping matched flow`);
                    await this.cleanupMatchedFlow(job.id);
                    return;
                }

                if (currentBucketIndex >= buckets.length) {
                    logger.info(`All batches sent for Job ${job.id}`);
                    await this.cleanupMatchedFlow(job.id);
                    return;
                }

                batchNumber++;
                const batchDrivers: string[] = [];
                let remaining = this.MAX_BATCH_SIZE;

                while (remaining > 0 && currentBucketIndex < buckets.length) {
                    const bucket = buckets[currentBucketIndex];
                    const available = bucket.drivers.length - usedFromBucket;
                    const take = Math.min(remaining, available);

                    batchDrivers.push(...bucket.drivers.slice(usedFromBucket, usedFromBucket + take));

                    usedFromBucket += take;
                    remaining -= take;

                    if (usedFromBucket >= bucket.drivers.length) {
                        currentBucketIndex++;
                        usedFromBucket = 0;
                    }
                }

                if (!batchDrivers.length) {
                    await this.cleanupMatchedFlow(job.id);
                    return;
                }

                const currentBucket = buckets[Math.min(currentBucketIndex, buckets.length - 1)];
                const batch: DriverBatch = {
                    batchNumber,
                    drivers: batchDrivers,
                    distanceRange: `${currentBucket.rangeStart}-${currentBucket.rangeEnd}m`,
                    sendTime: Date.now(),
                };

                // --- Prepare tabular data ---
                const tableData = batch.drivers.map((driverId, idx) => ({
                    Row: idx + 1,
                    DriverID: driverId,
                    Batch: batch.batchNumber,
                    DistanceRange: batch.distanceRange,
                    JobID: job.id,
                    SentAt: new Date(batch.sendTime).toISOString(),
                }));


                console.table(tableData);


                const redisKey = `job:${job.id}:batch_table`;
                for (const row of tableData) {
                    await redis.rpush(redisKey, JSON.stringify(row));
                }
                await redis.expire(redisKey, 3600);
                await this.offerManagementService.sendOffers(job, batchDrivers);

                if (currentBucketIndex < buckets.length) {
                    const timeout = setTimeout(processBatch, this.BATCH_TIME_INTERVAL);
                    this.activeBatchJobs.set(job.id, timeout);
                } else {
                    await this.cleanupMatchedFlow(job.id);
                }
            } catch (error: any) {
                logger.error(`Batch processing error for Job ${job.id}: ${error.message}`);
                await this.cleanupMatchedFlow(job.id);
            }
        };

        await processBatch();
    }



    private async getAllPotentialDrivers(job: Job, customerId: string): Promise<string[]> {
        const [freeDrivers, busyDrivers] = await Promise.all([
            this.freeDriverService.getFreeDriversForJob(job, customerId),
            this.busyDriverService.getBusyDriversForJob(job, customerId),
        ]);

        const allDrivers = [...(freeDrivers || []), ...(busyDrivers || [])];

        logger.info(`Found ${freeDrivers?.length || 0} free drivers and ${busyDrivers?.length || 0} busy drivers for Job ${job.id}`);

        return allDrivers;
    }

    private async enrichDriversWithDistance(driverIds: string[], jobLat: number, jobLng: number): Promise<DriverWithDistance[]> {
        const results = await Promise.all(driverIds.map(async (driverId) => {
            const distance = await this.calculateDriverDistance(driverId, jobLat, jobLng);
            return distance !== null ? { driverId, distance } : null;
        }));

        return results
            .filter((d): d is DriverWithDistance => d !== null)
            .sort((a, b) => a.distance - b.distance);
    }

    private async calculateDriverDistance(driverId: string, jobLat: number, jobLng: number): Promise<number | null> {
        try {
            const data = await redis.call('JSON.GET', `driver:${driverId}`, '$');
            if (!data) return null;

            const parsed = JSON.parse(data as string) as RedisDriverData[];
            const driverData = parsed?.[0];
            if (!driverData?.location?.coordinates?.length) return null;

            const [lng, lat] = driverData.location.coordinates;
            return this.spatialService.calculateDistance(lat, lng, jobLat, jobLng);
        } catch {
            return null;
        }
    }

    private createDistanceBuckets(drivers: DriverWithDistance[]): DistanceBucket[] {
        if (!drivers.length) return [];

        const buckets: DistanceBucket[] = [];
        const maxDist = this.MAX_DISTANCE_KM * 1000;


        for (let start = 0; start <= maxDist; start += this.BUCKET_DISTANCE_STEP_METERS) {
            const end = start + this.BUCKET_DISTANCE_STEP_METERS;
            const inBucket = drivers
                .filter(d => {
                    const dist = d.distance * 1000;
                    return dist >= start && dist < end;
                })
                .map(d => d.driverId);

            if (inBucket.length) {
                buckets.push({ rangeStart: start, rangeEnd: end, drivers: inBucket });
            }
        }

        // Add drivers beyond max distance
        const farDrivers = drivers
            .filter(d => d.distance * 1000 >= maxDist)
            .map(d => d.driverId);

        if (farDrivers.length) {
            buckets.push({ rangeStart: maxDist, rangeEnd: Infinity, drivers: farDrivers });
        }

        // Log bucket distribution
        buckets.forEach((bucket, idx) => {
            logger.info(`Bucket ${idx + 1}: ${bucket.rangeStart}-${bucket.rangeEnd}m (${bucket.drivers.length} drivers)`);
        });

        return buckets;
    }

    private async cleanupMatchedFlow(jobId: string): Promise<void> {
        await redis.del(`job:${jobId}:matched_flow_active`);
        this.stopBatchProcessing(jobId);
        logger.info(`Cleaned up matched flow for Job ${jobId}`);
    }

    stopBatchProcessing(jobId: string): void {
        const timeout = this.activeBatchJobs.get(jobId);
        if (timeout) {
            clearTimeout(timeout);
            this.activeBatchJobs.delete(jobId);
            logger.info(`Stopped batch processing for Job ${jobId}`);
        }
    }

    cleanup(): void {
        for (const [jobId, timeout] of this.activeBatchJobs.entries()) {
            clearTimeout(timeout);
            logger.info(`Cleaned up batch processing for Job ${jobId}`);
        }
        this.activeBatchJobs.clear();
    }
}