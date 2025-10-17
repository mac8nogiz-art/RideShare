import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Job } from '../types';
import { producer, isKafkaConnected, checkProducerHealth, reconnectKafka } from '../infrastructure/kafka';

export class OfferManagementService {
    private readonly OFFER_EXPIRY_SECONDS = 120;
    private readonly KAFKA_TOPIC_ASSIGNMENTS = 'driver-assignments';
    private readonly MAX_KAFKA_RETRIES = 2;

    async sendOffers(job: Job, driverIds: string[]): Promise<{ successful: number; failed: number }> {
        logger.info(`Sending Offers - JobId: ${job.id}, Drivers: ${driverIds.length}`);

        const pipeline = redis.pipeline();
        const sentAt = new Date().toISOString();
        const expiresAt = Date.now() + (this.OFFER_EXPIRY_SECONDS * 1000);

        for (const driverId of driverIds) {
            const offerKey = `offer:${job.id}:${driverId}`;
            const offerData = {
                jobId: job.id,
                driverId,
                customerId: job.customerId,
                pickupLat: job.pickupLat,
                pickupLng: job.pickupLng,
                fare: job.fare,
                vehicleType: job.vehicleType,
                status: 'pending',
                sentAt,
                expiresAt
            };

            pipeline.setex(offerKey, this.OFFER_EXPIRY_SECONDS, JSON.stringify(offerData));
            pipeline.sadd(`driver:${driverId}:offers`, job.id);
            pipeline.sadd(`job:${job.id}:pending_drivers`, driverId);
        }

        try {
            const results = await pipeline.exec();
            const successful = results?.filter(r => r[0] === null).length || 0;
            const failed = driverIds.length * 3 - successful;

            logger.info(`Offers Sent - JobId: ${job.id}, Successful: ${Math.floor(successful / 3)}, Failed: ${Math.floor(failed / 3)}`);
            return { successful: Math.floor(successful / 3), failed: Math.floor(failed / 3) };
        } catch (error) {
            logger.error(`Send Offers Error - JobId: ${job.id}, Error: ${error}`);
            return { successful: 0, failed: driverIds.length };
        }
    }

    async assignDriverToJob(jobId: string, driverId: string): Promise<void> {
        logger.info(`Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);

        const pipeline = redis.pipeline();

        // Update job with assigned driver
        pipeline.hset(`job:${jobId}`, {
            assignedDriver: driverId,
            status: 'accepted',
            assignedAt: new Date().toISOString()
        });

        // Mark driver as busy
        pipeline.hset(`driver:${driverId}:profile`, 'isBusy', 'true');

        // Clean up the accepted offer
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);

        await pipeline.exec();

        // Publish assignment event with retry logic
        await this.publishAssignmentEventWithRetry(jobId, driverId);

        // Cancel other offers (non-blocking)
        this.cancelOtherOffers(jobId, driverId).catch(err => {
            logger.error(`Cancel Other Offers Error - JobId: ${jobId}, Error: ${err}`);
        });

        logger.info(`Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async handleDriverRejection(jobId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(`Driver Rejected - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        const pipeline = redis.pipeline();

        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        pipeline.srem(`job:${jobId}:pending_drivers`, driverId);
        pipeline.sadd(`job:${jobId}:rejected_drivers`, driverId);

        await pipeline.exec();

        logger.debug(`Driver Rejection Processed - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async findAlternativeDrivers(jobId: string, nearbyDrivers: string[]): Promise<string[]> {
        logger.info(`Finding Alternative Drivers - JobId: ${jobId}`);

        try {
            const rejectedDrivers = await redis.smembers(`job:${jobId}:rejected_drivers`);
            const availableDrivers = nearbyDrivers.filter(d => !rejectedDrivers.includes(d));

            logger.info(`Alternative Drivers Found - JobId: ${jobId}, Count: ${availableDrivers.length}`);
            return availableDrivers;
        } catch (error) {
            logger.error(`Find Alternative Drivers Error - JobId: ${jobId}, Error: ${error}`);
            return [];
        }
    }

    private async cancelOtherOffers(jobId: string, acceptedDriverId: string): Promise<void> {
        logger.debug(`Cancelling Other Offers - JobId: ${jobId}, AcceptedDriver: ${acceptedDriverId}`);

        try {
            const pendingDrivers = await redis.smembers(`job:${jobId}:pending_drivers`);

            if (pendingDrivers.length === 0) {
                return;
            }

            const pipeline = redis.pipeline();

            for (const driverId of pendingDrivers) {
                if (driverId !== acceptedDriverId) {
                    pipeline.del(`offer:${jobId}:${driverId}`);
                    pipeline.srem(`driver:${driverId}:offers`, jobId);
                }
            }

            pipeline.del(`job:${jobId}:pending_drivers`);
            await pipeline.exec();

            const cancelledCount = pendingDrivers.filter(id => id !== acceptedDriverId).length;
            logger.debug(`Other Offers Cancelled - JobId: ${jobId}, Cancelled: ${cancelledCount}`);
        } catch (error) {
            logger.error(`Cancel Other Offers Error - JobId: ${jobId}, Error: ${error}`);
        }
    }

    private async publishAssignmentEventWithRetry(jobId: string, driverId: string): Promise<void> {
        for (let attempt = 1; attempt <= this.MAX_KAFKA_RETRIES; attempt++) {
            try {
                await this.publishAssignmentEvent(jobId, driverId);
                logger.debug(`✅ Kafka Assignment Event Published - JobId: ${jobId}, Driver: ${driverId}`);
                return; // Success, exit retry loop
            } catch (error: any) {
                logger.warn(`⚠️ Kafka Assignment Event Failed (Attempt ${attempt}/${this.MAX_KAFKA_RETRIES}) - JobId: ${jobId}, Error: ${error.message}`);

                if (attempt === this.MAX_KAFKA_RETRIES) {
                    logger.error(`❌ Kafka Assignment Event Failed After ${this.MAX_KAFKA_RETRIES} Attempts - JobId: ${jobId}`);
                    break; // Max retries reached
                }

                // Try to reconnect before next attempt
                if (error.message.includes('disconnected')) {
                    logger.info(`🔄 Attempting Kafka reconnection before retry...`);
                    await reconnectKafka();
                }

                // Wait before retry (exponential backoff)
                const backoffTime = Math.min(200 * Math.pow(2, attempt - 1), 2000);
                await new Promise(resolve => setTimeout(resolve, backoffTime));
            }
        }
    }

    private async publishAssignmentEvent(jobId: string, driverId: string): Promise<void> {
        // Check Kafka connection status
        if (!isKafkaConnected()) {
            throw new Error('Kafka producer is disconnected');
        }

        // Verify producer health
        const isHealthy = await checkProducerHealth();
        if (!isHealthy) {
            throw new Error('Kafka producer health check failed');
        }

        const event = {
            type: 'driver.assigned',
            jobId,
            driverId,
            timestamp: new Date().toISOString(),
            success: true
        };

        await producer.send({
            topic: this.KAFKA_TOPIC_ASSIGNMENTS,
            messages: [{
                key: driverId,
                value: JSON.stringify(event),
                headers: {
                    'job-id': jobId,
                    'event-type': 'assignment',
                    'timestamp': Date.now().toString()
                }
            }]
        });
    }
}