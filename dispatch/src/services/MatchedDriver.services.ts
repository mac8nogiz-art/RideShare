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

interface FlowData {
    job: Job;
    batches: DriverBatch[];
    nextBatchIndex: number;
}

export class MatchedDriverService {
    private readonly BATCH_TIME_INTERVAL = 45_000; // 45 seconds
    private readonly BUCKET_DISTANCE_STEP_METERS = 500;
    private readonly MAX_BATCH_SIZE = 5;
    private readonly MAX_DISTANCE_KM = 15;

    constructor(
        private readonly busyDriverService: BusyDriverService,
        private readonly freeDriverService: FreeDriverService,
        private readonly offerManagementService: OfferManagementService,
        private readonly spatialService: SpatialService = new SpatialService()
    ) {}

    async triggerMatchedDriverFlow(job: Job, jobId: string): Promise<void> {
        try {
            logger.info(`Triggering MATCHED DRIVER FLOW for Job ${jobId}`);


            const alreadyActive = await redis.get(`job:${jobId}:matched_flow_active`);

            if (alreadyActive === '1') {
                logger.info(` Matched flow already active for Job ${jobId} - skipping duplicate trigger`);
                return;
            }


            if (!job.pickupLat || !job.pickupLng || !job.customerId) {
                logger.error(` Invalid job data for ${jobId} - missing required fields`);
                return;
            }

            const distanceBuckets = await this.getMatchedDriversForJob(job, job.customerId);

            if (!distanceBuckets.length) {
                logger.warn(`No matched drivers found for Job ${jobId}`);
                return;
            }

            logger.info(`Found ${distanceBuckets.flatMap(b => b.drivers).length} drivers across ${distanceBuckets.length} buckets for Job ${jobId}`);


            await this.startEventBasedBatchFlow(job, distanceBuckets);

        } catch (error: any) {
            logger.error(` Matched driver flow failed for Job ${jobId}: ${error.message}`);
            await this.cleanupMatchedFlow(jobId);
            throw error;
        }
    }

    private async startEventBasedBatchFlow(job: Job, buckets: DistanceBucket[]): Promise<void> {
        if (!buckets.length) return;


        await redis.setex(`job:${job.id}:matched_flow_active`, 360, '1');

        const allDrivers = buckets.flatMap(bucket => bucket.drivers);
        logger.info(` Starting event-based batch flow for Job ${job.id}: ${allDrivers.length} drivers`);

        const batches = this.createBatches(allDrivers, buckets);

        await this.updateFlowData(job.id, { job, batches, nextBatchIndex: 0 });

        logger.info(` Created ${batches.length} batches for Job ${job.id}`);

        await this.sendBatchWithTTLTrigger(job.id, 0);
    }

    private createBatches(allDrivers: string[], buckets: DistanceBucket[]): DriverBatch[] {
        const batches: DriverBatch[] = [];
        let currentIndex = 0;
        let batchNumber = 0;

        while (currentIndex < allDrivers.length) {
            batchNumber++;
            const endIndex = Math.min(currentIndex + this.MAX_BATCH_SIZE, allDrivers.length);
            const batchDrivers = allDrivers.slice(currentIndex, endIndex);
            const distanceRange = this.getDistanceRangeForDrivers(buckets, currentIndex, endIndex);

            batches.push({
                batchNumber,
                drivers: batchDrivers,
                distanceRange,
                sendTime: Date.now(),
            });

            currentIndex = endIndex;
        }

        return batches;
    }

    private async sendBatchWithTTLTrigger(jobId: string, batchIndex: number): Promise<void> {
        try {
            const status = await redis.get(`job:${jobId}:status`);
            if (status === 'assigned' || status === 'cancelled') {
                logger.info(` Job ${jobId} already ${status} - stopping batch flow`);
                await this.cleanupMatchedFlow(jobId);
                return;
            }

            const flowData = await this.getFlowData(jobId);
            if (!flowData || !flowData.batches.length) {
                logger.warn(`No flow data found for Job ${jobId}`);
                await this.cleanupMatchedFlow(jobId);
                return;
            }

            if (batchIndex >= flowData.batches.length) {
                logger.info(`All ${flowData.batches.length} batches sent for Job ${jobId}`);
                await this.cleanupMatchedFlow(jobId);
                return;
            }

            const currentBatch = flowData.batches[batchIndex];
            const isLastBatch = batchIndex === flowData.batches.length - 1;

            logger.info(`Sending Batch ${currentBatch.batchNumber}/${flowData.batches.length} for Job ${jobId}: ${currentBatch.drivers.length} drivers (${currentBatch.distanceRange})`);

            // Send offers to all drivers in current batch
            await this.offerManagementService.sendOffers(flowData.job, currentBatch.drivers);

            if (!isLastBatch) {
                const nextBatchIndex = batchIndex + 1;

                // Update flow data with next batch index
                flowData.nextBatchIndex = nextBatchIndex;
                await this.updateFlowData(jobId, flowData);


                const batchTriggerKey = `job:${jobId}:batch_trigger:${currentBatch.batchNumber}`;
                const batchTTL = Math.ceil(this.BATCH_TIME_INTERVAL / 1000); // 45 seconds

                await redis.setex(
                    batchTriggerKey,
                    batchTTL,
                    JSON.stringify({
                        batchNumber: currentBatch.batchNumber,
                        nextBatchIndex: nextBatchIndex,
                        drivers: currentBatch.drivers,
                        sentAt: Date.now()
                    })
                );

                logger.info(` Batch ${currentBatch.batchNumber} TTL set - expires in ${batchTTL}s, will trigger Batch ${nextBatchIndex}`);
            } else {
                logger.info(` Last batch sent for Job ${jobId} - matched flow complete`);
                await this.cleanupMatchedFlow(jobId);
            }

        } catch (error: any) {
            logger.error(`Batch sending error for Job ${jobId}: ${error.message}`);
            await this.cleanupMatchedFlow(jobId);
        }
    }


    async handleBatchTriggerExpiry(jobId: string, batchNumber: number): Promise<void> {
        try {
            logger.info(` Batch ${batchNumber} expired for Job ${jobId} - triggering next batch`);

            const status = await redis.get(`job:${jobId}:status`);
            if (status === 'assigned' || status === 'cancelled') {
                logger.info(` Job ${jobId} already ${status} - ignoring batch expiry`);
                await this.cleanupMatchedFlow(jobId);
                return;
            }

            const flowActive = await redis.get(`job:${jobId}:matched_flow_active`);

            if (flowActive !== '1') {
                logger.info(`Matched flow not active for Job ${jobId} - ignoring trigger`);
                return;
            }

            const flowData = await this.getFlowData(jobId);
            if (!flowData) {
                logger.warn(` No flow data found for Job ${jobId} - cannot proceed`);
                await this.cleanupMatchedFlow(jobId);
                return;
            }
            logger.info(` Proceeding to send Batch ${flowData.nextBatchIndex} for Job ${jobId}`);
            await this.sendBatchWithTTLTrigger(jobId, flowData.nextBatchIndex);

        } catch (error: any) {
            logger.error(` Batch trigger handling error for Job ${jobId}: ${error.message}`);
            await this.cleanupMatchedFlow(jobId);
        }
    }



    async getMatchedDriversForJob(job: Job, customerId: string): Promise<DistanceBucket[]> {
        try {

            const allDriverIds = await this.getAllPotentialDrivers(job, customerId);
            if (!allDriverIds.length) {
                logger.warn(`No drivers available for Job ${job.id}`);
                return [];
            }

            logger.info(`Found ${allDriverIds.length} potential drivers for Job ${job.id}`);

            const enrichedDrivers = await this.enrichDriversWithDistance(
                allDriverIds,
                job.pickupLat,
                job.pickupLng
            );

            if (!enrichedDrivers.length) {
                logger.warn(`No drivers with valid locations for Job ${job.id}`);
                return [];
            }


            const buckets = this.createDistanceBuckets(enrichedDrivers);
            logger.info(`Created ${buckets.length} distance buckets for Job ${job.id}`);

            return buckets;
        } catch (error: any) {
            logger.error(`Matched driver search failed for ${job.id}: ${error.message}`);
            return [];
        }
    }

    private async getAllPotentialDrivers(job: Job, customerId: string): Promise<string[]> {
        const [freeDrivers, busyDrivers] = await Promise.all([
            this.freeDriverService.getFreeDriversForJob(job, customerId),
            this.busyDriverService.getBusyDriversForJob(job, customerId),
        ]);

        const allDrivers = [...(freeDrivers || []), ...(busyDrivers || [])];
        logger.info(`Found ${freeDrivers?.length || 0} free + ${busyDrivers?.length || 0} busy = ${allDrivers.length} total drivers`);

        return allDrivers;
    }

    private async enrichDriversWithDistance(
        driverIds: string[],
        jobLat: number,
        jobLng: number
    ): Promise<DriverWithDistance[]> {
        const results = await Promise.all(
            driverIds.map(async (driverId) => {
                try {

                    const data = await redis.call('JSON.GET', `driver:${driverId}`, '$');
                    if (!data) return null;
                    const parsed = JSON.parse(data as string) as RedisDriverData[];
                    const driverData = parsed?.[0];
                    if (!driverData?.location?.coordinates?.length) return null;
                    const [lng, lat] = driverData.location.coordinates;
                    const distance = this.spatialService.calculateDistance(lat, lng, jobLat, jobLng);
                    return { driverId, distance };
                } catch {
                    return null;
                }
            })
        );


        return results
            .filter((d): d is DriverWithDistance => d !== null)
            .sort((a, b) => a.distance - b.distance);
    }


    private createDistanceBuckets(drivers: DriverWithDistance[]): DistanceBucket[] {
        if (!drivers.length) return [];

        const buckets: DistanceBucket[] = [];
        const maxDist = this.MAX_DISTANCE_KM * 1000;

        for (let start = 0; start <= maxDist; start += this.BUCKET_DISTANCE_STEP_METERS) {
            const end = start + this.BUCKET_DISTANCE_STEP_METERS;
            const inBucket = drivers
                .filter(d => d.distance * 1000 >= start && d.distance * 1000 < end)
                .map(d => d.driverId);

            if (inBucket.length) {
                buckets.push({ rangeStart: start, rangeEnd: end, drivers: inBucket });
            }
        }
        buckets.forEach((bucket, idx) => {
            logger.info(`Bucket ${idx + 1}: ${bucket.rangeStart}-${bucket.rangeEnd}m (${bucket.drivers.length} drivers)`);
        });

        return buckets;
    }

    private getDistanceRangeForDrivers(
        buckets: DistanceBucket[],
        startIdx: number,
        endIdx: number
    ): string {
        let driverCount = 0;
        let firstBucket: DistanceBucket | null = null;
        let lastBucket: DistanceBucket | null = null;

        for (const bucket of buckets) {
            const bucketStart = driverCount;
            const bucketEnd = driverCount + bucket.drivers.length;

            if (bucketEnd > startIdx && bucketStart < endIdx) {
                if (!firstBucket) firstBucket = bucket;
                lastBucket = bucket;
            }

            driverCount += bucket.drivers.length;
            if (driverCount >= endIdx) break;
        }

        if (!firstBucket || !lastBucket) return '0-500m';
        if (firstBucket === lastBucket) {
            return `${firstBucket.rangeStart}-${firstBucket.rangeEnd}m`;
        }
        return `${firstBucket.rangeStart}-${lastBucket.rangeEnd}m`;
    }


    private async getFlowData(jobId: string): Promise<FlowData | null> {
        try {
            const jobKey = `job:${jobId}:flow_data`;
            const data = await redis.get(jobKey);
            if (!data) return null;
            return JSON.parse(data);
        } catch (error: any) {
            logger.error(`Failed to retrieve flow data: ${error.message}`);
            return null;
        }
    }

    private async updateFlowData(jobId: string, flowData: FlowData): Promise<void> {
        try {
            const jobKey = `job:${jobId}:flow_data`;
            await redis.setex(jobKey, 3600, JSON.stringify(flowData));
        } catch (error: any) {
            logger.error(`Failed to update flow data: ${error.message}`);
        }
    }


    private async cleanupMatchedFlow(jobId: string): Promise<void> {
        try {

            await redis.del(`job:${jobId}:matched_flow_active`);
            await redis.del(`job:${jobId}:flow_data`);

            const triggerKeys = await redis.keys(`job:${jobId}:batch_trigger:*`);
            if (triggerKeys.length > 0) {
                await redis.del(...triggerKeys);
            }
            logger.info(`Cleaned up matched flow for Job ${jobId}`);
        } catch (error: any) {
            logger.error(`Failed to cleanup matched flow: ${error.message}`);
        }
    }
}