import { logger } from '../logger';
import { Job } from '../types';
import { BusyDriverService } from './busyDriver.services';
import { FreeDriverService } from './freeDriverService';
import { redis } from '../infrastructure/redis';
import { SpatialService } from '../infrastructure/spatial';

interface DriverWithDistance {
    driverId: string;
    distance: number;
    driverType: 'free' | 'busy';
}

interface DriverBatch {
    batchNumber: number;
    drivers: string[];
    distanceRange: string;
    sendTime: number;
}

export class MatchedDriverService {
    private readonly BATCH_TIME_INTERVAL = 45_000;
    private readonly BATCH_DISTANCE_STEP_KM = 0.5; // 500 meters
    private readonly INITIAL_BATCH_SIZE = 3;
    private readonly SUBSEQUENT_BATCH_SIZE = 4;

    private readonly busyDriverService: BusyDriverService;
    private readonly freeDriverService: FreeDriverService;
    private readonly spatialService = new SpatialService();

    constructor(busyDriverService: BusyDriverService, freeDriverService: FreeDriverService) {
        this.busyDriverService = busyDriverService;
        this.freeDriverService = freeDriverService;
    }


    async getMatchedDriversForJob(job: Job, customerId: string): Promise<DriverBatch[]> {
        const start = performance.now();
        logger.info(` Searching matched drivers for job ${job.id}`);

        try {
            const [free, busy] = await Promise.all([
                this.freeDriverService.getFreeDriversForJob(job, customerId),
                this.busyDriverService.getBusyDriversForJob(job, customerId),
            ]);

            const allDriverIds = [...(free || []), ...(busy || [])];
            logger.info(`Found ${free?.length || 0} free and ${busy?.length || 0} busy drivers.`);

            if (!allDriverIds.length) return [];

            const drivers = await this.enrichDriversWithDistance(allDriverIds, job.pickupLat, job.pickupLng);
            const batches = this.createDistanceBatches(drivers);

            logger.info(
                `Created ${batches.length} batches for job ${job.id} in ${Math.round(performance.now() - start)}ms`
            );
            return batches;
        } catch (err: any) {
            logger.error(`Matched driver search failed for job ${job.id}: ${err.message}`);
            return [];
        }
    }

    private async enrichDriversWithDistance(
        driverIds: string[],
        jobLat: number,
        jobLng: number
    ): Promise<DriverWithDistance[]> {
        const results = await Promise.all(
            driverIds.map(async (driverId) => {
                const distance = await this.calculateDriverDistance(driverId, jobLat, jobLng);
                return distance !== null ? { driverId, distance, driverType: 'free' as const } : null;
            })
        );

        // @ts-ignore
        const validDrivers = results.filter((d): d is DriverWithDistance => !!d);
        // @ts-ignore
        return validDrivers.sort((a, b) => a.distance - b.distance);
    }


    private async calculateDriverDistance(driverId: string, jobLat: number, jobLng: number): Promise<number | null> {
        try {
            const data = await redis.call('JSON.GET', `driver:${driverId}`, '$');
            if (!data) return null;

            const parsed = JSON.parse(data as string)?.[0];
            const coords = parsed?.location?.coordinates;
            if (!coords?.length) return null;

            const [lng, lat] = coords;
            return this.spatialService.calculateDistance(lat, lng, jobLat, jobLng);
        } catch (err: any) {
            logger.warn(` Distance calc failed for driver ${driverId}: ${err.message}`);
            return null;
        }
    }


    private createDistanceBatches(drivers: DriverWithDistance[]): DriverBatch[] {
        if (!drivers.length) return [];

        const batches: DriverBatch[] = [];
        let distanceCursor = 0;
        let batchNumber = 1;
        let timeOffset = 0;

        while (drivers.length && distanceCursor <= 15) {
            const size = batchNumber === 1 ? this.INITIAL_BATCH_SIZE : this.SUBSEQUENT_BATCH_SIZE;
            const nextRange = distanceCursor + this.BATCH_DISTANCE_STEP_KM;

            const driversInRange = drivers
                .filter(d => d.distance >= distanceCursor && d.distance < nextRange)
                .slice(0, size);

            if (driversInRange.length) {
                const ids = driversInRange.map(d => d.driverId);
                drivers = drivers.filter(d => !ids.includes(d.driverId));

                batches.push({
                    batchNumber,
                    drivers: ids,
                    distanceRange: `${(distanceCursor * 1000).toFixed(0)}-${(nextRange * 1000).toFixed(0)}m`,
                    sendTime: timeOffset,
                });

                logger.info(` Batch ${batchNumber}: ${ids.length} drivers (${distanceCursor.toFixed(1)}-${nextRange.toFixed(1)}km)`);

                batchNumber++;
                timeOffset += this.BATCH_TIME_INTERVAL;
            }
            distanceCursor = nextRange;
        }

        if (drivers.length) {
            const remainingIds = drivers.map(d => d.driverId);
            while (remainingIds.length) {
                const ids = remainingIds.splice(0, this.SUBSEQUENT_BATCH_SIZE);
                batches.push({
                    batchNumber,
                    drivers: ids,
                    distanceRange: `>${(distanceCursor * 1000).toFixed(0)}m`,
                    sendTime: timeOffset,
                });

                logger.info(` Extra Batch ${batchNumber}: ${ids.length} drivers (>${distanceCursor.toFixed(1)}km)`);
                batchNumber++;
                timeOffset += this.BATCH_TIME_INTERVAL;
            }
        }

        return batches;
    }


    async sendBatchRequests(batches: DriverBatch[], job: Job): Promise<void> {
        logger.info(`Sending ${batches.length} batches for job ${job.id}`);

        for (const batch of batches) {
            if (batch.sendTime > 0) await this.delay(batch.sendTime);
            logger.info(`Sending batch ${batch.batchNumber} (${batch.drivers.length} drivers, ${batch.distanceRange})`);
            await this.sendRequestsToDrivers(batch.drivers, job);
        }

        logger.info(`All batch requests sent for job ${job.id}`);
    }

    private async sendRequestsToDrivers(driverIds: string[], job: Job): Promise<void> {
        await Promise.all(
            driverIds.map(async (driverId) => {
                try {
                    logger.info(` Job ${job.id} → Driver ${driverId}`);

                } catch (err: any) {
                    logger.error(` Failed to send job ${job.id} to driver ${driverId}: ${err.message}`);
                }
            })
        );
    }

    private delay(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }


}
