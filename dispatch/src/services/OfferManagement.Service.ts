
import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Job } from '../types';

export class OfferManagementService {
    async sendOffers(job: Job, driverIds: string[]): Promise<{ successful: number; failed: number }> {
        logger.info(`Sending Offers - JobId: ${job.id}, Drivers: ${driverIds.length}`);

        const offerPromises = driverIds.map(driverId => this.sendOfferToDriver(job, driverId));
        const results = await Promise.allSettled(offerPromises);

        const successful = results.filter(r => r.status === 'fulfilled').length;
        const failed = results.filter(r => r.status === 'rejected').length;

        logger.info(`Offers Sent - JobId: ${job.id}, Total: ${driverIds.length}, Successful: ${successful}, Failed: ${failed}`);
        return { successful, failed };
    }

    private async sendOfferToDriver(job: Job, driverId: string): Promise<void> {
        const offerKey = `offer:${job.id}:${driverId}`;
        const offerData = {
            jobId: job.id,
            driverId,
            customerId: job.customerId,
            pickupLat: job.pickupLat,
            pickupLng: job.pickupLng,
            fare: job.fare,
            status: 'pending',
            sentAt: new Date().toISOString(),
            expiresAt: Date.now() + 30000
        };

        await redis.setex(offerKey, 30, JSON.stringify(offerData));
        logger.debug(`Offer Sent - JobId: ${job.id}, Driver: ${driverId}`);
    }

    async assignDriverToJob(jobId: string, driverId: string): Promise<void> {
        logger.info(`Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);

        await redis.hset(`job:${jobId}`, {
            assignedDriver: driverId,
            status: 'accepted',
            assignedAt: new Date().toISOString()
        });

        await redis.hset(`driver:${driverId}:profile`, 'isBusy', 'true');
        await this.cancelOtherOffers(jobId, driverId);

        logger.info(`Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async handleDriverRejection(jobId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(`Handling Driver Rejection - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        await redis.del(`offer:${jobId}:${driverId}`);
        await redis.srem(`driver:${driverId}:offers`, jobId);

        logger.debug(`Driver Offer Removed - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async findAlternativeDrivers(jobId: string): Promise<Job | null> {
        logger.info(`Finding Alternative Drivers - JobId: ${jobId}`);

        const jobData = await redis.hgetall(`job:${jobId}`);
        if (!jobData.pickupLat || !jobData.pickupLng) {
            logger.warn(`Job Location Not Found - JobId: ${jobId}`);
            return null;
        }

        const job: Job = {
            id: jobId,
            customerId: jobData.customerId,
            pickupLat: parseFloat(jobData.pickupLat),
            pickupLng: parseFloat(jobData.pickupLng),
            fare: parseFloat(jobData.fare),
            timestamp: Date.now()
        };

        logger.info(`Job Queued for Re-matching - JobId: ${jobId}`);
        return job;
    }

    private async cancelOtherOffers(jobId: string, acceptedDriverId: string): Promise<void> {
        logger.debug(`Cancelling Other Offers - JobId: ${jobId}, AcceptedDriver: ${acceptedDriverId}`);

        const offerKeys = await redis.keys(`offer:${jobId}:*`);
        const cancelPromises = offerKeys.map(async (key) => {
            const driverId = key.split(':')[2];
            if (driverId !== acceptedDriverId) {
                await redis.del(key);
                await redis.srem(`driver:${driverId}:offers`, jobId);
            }
        });

        await Promise.allSettled(cancelPromises);
        logger.debug(`Other Offers Cancelled - JobId: ${jobId}, Cancelled: ${offerKeys.length - 1}`);
    }
}