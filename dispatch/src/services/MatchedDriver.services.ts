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

interface DistanceBucket {
    rangeStart: number;
    rangeEnd: number;
    drivers: string[];
}

interface RedisDriverData {
    location?: { coordinates: number[] };
}

export class MatchedDriverService {
    private readonly BUCKET_DISTANCE_STEP_METERS = 500;
    private readonly MAX_DISTANCE_KM = 15;

    constructor(
        private readonly busyDriverService: BusyDriverService,
        private readonly freeDriverService: FreeDriverService,
        public  offerManagementService: OfferManagementService,
        private readonly spatialService: SpatialService = new SpatialService()
    ) {}

    async triggerMatchedDriverFlow(job: Job, jobId: string): Promise<void> {
        try {
            logger.info(`Triggering MATCHED DRIVER FLOW for Job ${jobId}`);

            const alreadyActive = await redis.get(`job:${jobId}:matched_flow_active`);
            if (alreadyActive === '1') {
                logger.info(`Matched flow already active for Job ${jobId} - skipping duplicate trigger`);
                return;
            }

            if (!job.pickupLat || !job.pickupLng || !job.customerId) {
                logger.error(`Invalid job data for ${jobId} - missing required fields`);
                return;
            }

            const distanceBuckets = await this.getMatchedDriversForJob(job, job.customerId);

            if (!distanceBuckets.length) {
                logger.warn(` No matched drivers found for Job ${jobId}`);
                return;
            }

            logger.info(`Found ${distanceBuckets.flatMap(b => b.drivers).length} drivers across ${distanceBuckets.length} buckets for Job ${jobId}`);

            const bucketsKey = `job:${jobId}:matched_drivers_buckets`;
            await redis.del(bucketsKey);

            for (const bucket of distanceBuckets) {
                await redis.rpush(bucketsKey, JSON.stringify(bucket));
            }
            await redis.expire(bucketsKey, 3600);

            await redis.set(`job:${jobId}:matched_flow_active`, '1', 'EX', 3600);

            logger.info(`Stored ${distanceBuckets.length} buckets as Redis LIST for Job ${jobId}`);

            await this.offerManagementService.sendMatchedDriverOffers(job, distanceBuckets);

        } catch (error: any) {
            logger.error(`Matched driver flow failed for Job ${jobId}: ${error.message}`);
            await this.cleanupMatchedFlow(jobId);
            throw error;
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
            console.log("enrichedDrivers", enrichedDrivers)

            if (!enrichedDrivers.length) {
                logger.warn(` No drivers with valid locations for Job ${job.id}`);
                return [];
            }

            const buckets = this.createDistanceBuckets(enrichedDrivers);
            logger.info(`Created ${buckets.length} distance buckets for Job ${job.id}`);

            return buckets;
        } catch (error: any) {
            logger.error(` Matched driver search failed for ${job.id}: ${error.message}`);
            return [];
        }
    }

    private async getAllPotentialDrivers(job: Job, customerId: string): Promise<string[]> {
        const [freeDrivers, busyDrivers] = await Promise.all([
            this.freeDriverService.getFreeDriversForJob(job, customerId),
            this.busyDriverService.getBusyDriversForJob(job, customerId),
        ]);

        const allDrivers = [...(freeDrivers || []), ...(busyDrivers || [])];
        logger.info(` Found ${freeDrivers?.length || 0} free + ${busyDrivers?.length || 0} busy = ${allDrivers.length} total drivers`);

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
            logger.info(` Bucket ${idx + 1}: ${bucket.rangeStart}-${bucket.rangeEnd}m (${bucket.drivers.length} drivers)`);
        });

        return buckets;
    }


    async cleanupMatchedFlow(jobId: string): Promise<void> {
        try {
            logger.info(`Cleaning up matched flow for Job ${jobId}`);

            await redis.del(`job:${jobId}:matched_flow_active`);
            await redis.del(`job:${jobId}:matched_drivers_buckets`);
            await redis.del(`job:${jobId}:current_bucket_index`);
            await redis.del(`job:${jobId}:matched_bucket`);

            logger.info(`Cleanup completed for Job ${jobId}`);
        } catch (error: any) {
            logger.error(` Cleanup failed for Job ${jobId}: ${error.message}`);
        }
    }
}