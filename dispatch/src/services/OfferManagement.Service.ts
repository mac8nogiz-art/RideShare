// import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Job } from '../types';
import { producer, isKafkaConnected, checkProducerHealth, reconnectKafka } from '../infrastructure/kafka';
import { redis, redisSubscriber } from '../infrastructure/redis';

export class OfferManagementService {
    private readonly OFFER_EXPIRY_SECONDS = 15;
    private readonly KAFKA_TOPIC_ASSIGNMENTS = 'driver-assignments';
    private readonly KAFKA_TOPIC_OFFERS = 'driver-offers';
    private readonly MAX_KAFKA_RETRIES = 2;

    async sendOffers(job: Job, driverIds: string[]): Promise<{ successful: number; failed: number }> {
        logger.info(`Sequential Offer Dispatch Started — JobId: ${job.id}, Total Drivers: ${driverIds.length}`);
        let successful = 0;
        let failed = 0;

        const currentAskDrivers = await this.getAskDriversFromBooking(job.id, job.customerId);

        for (const driverId of driverIds) {
            logger.info(` Sending Offer to Driver: ${driverId}`);
            try {
                await this.sendSingleOffer(job, driverId, currentAskDrivers);
                successful++;

                logger.info(`Offer Sent - JobId: ${job.id}, Driver: ${driverId}`);
                const accepted = await this.waitForDriverResponseOrTimeout(job.id, driverId, this.OFFER_EXPIRY_SECONDS * 1000);

                if (accepted) {
                    logger.info(` Driver ${driverId} accepted Job ${job.id}. Stopping offer cycle.`);
                    this.cancelOtherOffers(job.id, driverId).catch(err => {
                        logger.error(`Cancel Other Offers Error - JobId: ${job.id}, Error: ${err}`);
                    });
                    break;
                } else {
                    logger.warn(` Driver ${driverId} did not respond in time for Job ${job.id}. Auto-rejecting.`);
                    await this.handleDriverRejection(job.id, job.customerId, driverId, 'Offer timed out');
                }

                await new Promise(res => setTimeout(res, 200));
            } catch (error: any) {
                failed++;
                logger.error(`Failed to send offer to Driver ${driverId}: ${error?.message || error}`);
            }

            const status = await redis.hget(`job:${job.id}`, 'status');
            const assignedDriver = await redis.hget(`job:${job.id}`, 'assignedDriver');
            if (status === 'accepted' || assignedDriver) {
                logger.info(` Job already assigned (${assignedDriver || 'unknown'}) — stopping offer cycle.`);
                break;
            }
        }
        logger.info(`Offer Dispatch Complete — JobId: ${job.id}, Successful: ${successful}, Failed: ${failed}`);
        return { successful, failed };
    }

    private async sendSingleOffer(job: Job, driverId: string, currentAskDrivers: string[]): Promise<void> {
        const expiryTime = this.OFFER_EXPIRY_SECONDS + Math.floor(Math.random() * 3);
        const driverObjectId = driverId.startsWith('driver:') ? driverId.split(':')[1] : driverId;

        const offerData = {
            id: job.id,
            driverId: driverObjectId,
            customerId: job.customerId,
            pickupLat: job.pickupLat,
            pickupLng: job.pickupLng,
            driverEarning: job.fare,
            vehicleType: job.vehicleType,
            status: 'pending',
            tripAddress: job.tripAddress,
            sentAt: new Date().toISOString(),
            askDriver: { expTime: expiryTime },
            rideDetails: job.rideDetails || null,
            customer: {
                fullName: (job as any).customer?.fullName || "",
                avatar: (job as any).customer?.avatar || "",
                distance: (job as any).customer?.distance || "",
                time: (job as any).customer?.time || "",
            }
        };

        await redis.setex(`offer:${job.id}:${driverId}`, this.OFFER_EXPIRY_SECONDS, JSON.stringify(offerData));
        await this.updateBookingInRedis(job.id, job.customerId, driverObjectId, expiryTime, currentAskDrivers);

        // Save job notification for driver
        await this.saveJobNotification(job, driverObjectId, expiryTime);

        await this.publishAssignmentEventWithRetry('new_job.offer_sent', this.KAFKA_TOPIC_OFFERS, job.id, driverId, offerData);
    }

    /**
     * Saves job notification to Redis using JSON data type
     * Key format: jobnotification:{driverId}
     */
    private async saveJobNotification(job: Job, driverId: string, expiryTime: number): Promise<void> {
        try {
            const notificationKey = `jobnotification:${driverId}`;

            // Calculate expiry timestamp
            const expiryTimestamp = new Date();
            expiryTimestamp.setSeconds(expiryTimestamp.getSeconds() + expiryTime);

            const notificationData = {
                _id: job.id,
                tripAddress: job.tripAddress || [],
                rideDetails: job.rideDetails || {
                    estimatedTime: "",
                    estimatedDistance: 0,
                },
                askDriver: {
                    expTime: expiryTimestamp.toISOString()
                },
                driverEarning: job.fare || 0,
                customer: {
                    fullName: (job as any).customer?.fullName || "",
                    avatar: (job as any).customer?.avatar || "",
                    distance: (job as any).customer?.distance || 0,
                    time: (job as any).customer?.time || "0 mins"
                }
            };

            // Use JSON.SET to store the notification
            await redis.call('JSON.SET', notificationKey, '$', JSON.stringify(notificationData));

            // Set TTL for the notification (slightly longer than offer expiry)
            await redis.expire(notificationKey, this.OFFER_EXPIRY_SECONDS + 5);

            logger.info(`Job Notification Saved - Driver: ${driverId}, JobId: ${job.id}, TTL: ${this.OFFER_EXPIRY_SECONDS + 5}s`);
        } catch (error) {
            logger.error(`Save Job Notification Error - Driver: ${driverId}, JobId: ${job.id}, Error: ${error}`);
            // Don't throw - notification failure shouldn't block the offer
        }
    }

    /**
     * Deletes job notification when offer is accepted, rejected, or cancelled
     */
    private async deleteJobNotification(driverId: string): Promise<void> {
        try {
            const notificationKey = `jobnotification:${driverId}`;
            await redis.del(notificationKey);
            logger.debug(`Job Notification Deleted - Driver: ${driverId}`);
        } catch (error) {
            logger.error(`Delete Job Notification Error - Driver: ${driverId}, Error: ${error}`);
        }
    }

    /**
     * Finds booking key using pattern: booking:{jobId}-{customerId}-*
     * Returns the askDrivers array from the booking (stored as JSON)
     */
    private async getAskDriversFromBooking(jobId: string, customerId: string): Promise<string[]> {
        try {
            const pattern = `booking:${jobId}-${customerId}-*`;
            const bookingKeys = await redis.keys(pattern);

            if (bookingKeys.length === 0) {
                logger.warn(`No booking found for JobId: ${jobId}, CustomerId: ${customerId}`);
                return [];
            }

            const bookingKey = bookingKeys[0];

            // Use JSON.GET to retrieve the askDrivers field
            const askDrivers = await redis.call('JSON.GET', bookingKey, '$.askDrivers') as any;

            // JSON.GET with JSONPath returns an array with the result
            if (askDrivers && typeof askDrivers === 'string') {
                const parsed = JSON.parse(askDrivers);
                const driversArray = Array.isArray(parsed) ? parsed[0] : parsed;
                logger.debug(`Found ${driversArray?.length || 0} drivers already asked for Job ${jobId}`);
                return Array.isArray(driversArray) ? driversArray : [];
            }

            return [];
        } catch (error) {
            logger.error(`Get AskDrivers Error - JobId: ${jobId}, CustomerId: ${customerId}, Error: ${error}`);
            return [];
        }
    }

    /**
     * Updates booking in Redis using pattern: booking:{jobId}-{customerId}-*
     * Adds driver to askDrivers array and updates booking metadata using JSON.SET
     */
    private async updateBookingInRedis(jobId: string, customerId: string, driverId: string, expiryTime: number, currentAskDrivers: string[]): Promise<void> {
        try {
            const pattern = `booking:${jobId}-${customerId}-*`;
            const bookingKeys = await redis.keys(pattern);

            if (bookingKeys.length === 0) {
                logger.warn(`No booking found for JobId: ${jobId}, CustomerId: ${customerId}`);
                return;
            }

            const bookingKey = bookingKeys[0];

            // Add new driver to askDrivers array (avoid duplicates)
            const updatedAskDrivers = [...new Set([...currentAskDrivers, driverId])];

            // Update multiple fields using JSON.SET
            await redis.call('JSON.SET', bookingKey, '$.askDrivers', JSON.stringify(updatedAskDrivers));
            await redis.call('JSON.SET', bookingKey, '$.askDriver', JSON.stringify({ driver: driverId, expTime: expiryTime }));
            // await redis.expire(bookingKey, this.OFFER_EXPIRY_SECONDS);

            logger.info(`Booking Updated - JobId: ${jobId}, Driver: ${driverId}, AskDrivers: ${updatedAskDrivers.length}`);
        } catch (error) {
            logger.error(`Update Booking Error - JobId: ${jobId}, CustomerId: ${customerId}, Driver: ${driverId}, Error: ${error}`);
        }
    }

    private async waitForDriverResponseOrTimeout(jobId: string, driverId: string, timeoutMs: number): Promise<boolean> {
        return new Promise(async (resolve) => {
            const channel = `driver_response:${jobId}:${driverId}`;

            const onMessage = (channelName: string, message: string) => {
                if (channelName === channel) {
                    try {
                        const data = JSON.parse(message);
                        redisSubscriber.off("message", onMessage);
                        redisSubscriber.unsubscribe(channel);
                        clearTimeout(timeoutId);
                        resolve(data.type === "offer_accepted");
                    } catch (error) {
                        logger.error(`Parse driver response error: ${error}`);
                        resolve(false);
                    }
                }
            };

            redisSubscriber.on("message", onMessage);
            await redisSubscriber.subscribe(channel);

            const timeoutId = setTimeout(() => {
                redisSubscriber.off("message", onMessage);
                redisSubscriber.unsubscribe(channel);
                resolve(false);
            }, timeoutMs);
        });
    }

    async assignDriverToJob(jobId: string, driverId: string, customerId?: string): Promise<void> {
        logger.info(`Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);

        const pipeline = redis.pipeline();
        pipeline.hset(`job:${jobId}`, {
            assignedDriver: driverId,
            status: 'accepted',
            assignedAt: new Date().toISOString()
        });
        pipeline.hset(`driver:${driverId}:profile`, 'isBusy', 'true');
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        await pipeline.exec();

        // Delete job notification after assignment
        await this.deleteJobNotification(driverId);

        await this.publishAssignmentEventWithRetry('new_job.assigned', this.KAFKA_TOPIC_ASSIGNMENTS, jobId, driverId);
        this.cancelOtherOffers(jobId, driverId).catch(err =>
            logger.error(`Cancel Other Offers Error - JobId: ${jobId}, Error: ${err}`)
        );

        logger.info(`Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async handleDriverRejection(jobId: string, customerId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(`Driver Rejected - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        const pipeline = redis.pipeline();
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        pipeline.srem(`job:${jobId}:pending_drivers`, driverId);
        pipeline.sadd(`job:${jobId}:rejected_drivers`, driverId);
        await pipeline.exec();

        // Delete job notification after rejection
        await this.deleteJobNotification(driverId);

        logger.debug(`Driver Rejection Processed - JobId: ${jobId}, Driver: ${driverId}`);
    }

    private async cancelOtherOffers(jobId: string, acceptedDriverId: string): Promise<void> {
        try {
            const pendingDrivers = await redis.smembers(`job:${jobId}:pending_drivers`);
            if (pendingDrivers.length === 0) return;

            const pipeline = redis.pipeline();
            for (const driverId of pendingDrivers) {
                if (driverId !== acceptedDriverId) {
                    pipeline.del(`offer:${jobId}:${driverId}`);
                    pipeline.srem(`driver:${driverId}:offers`, jobId);
                    // Delete notifications for cancelled offers
                    this.deleteJobNotification(driverId).catch(err =>
                        logger.error(`Delete Notification Error - Driver: ${driverId}, Error: ${err}`)
                    );
                }
            }
            pipeline.del(`job:${jobId}:pending_drivers`);
            await pipeline.exec();

            logger.debug(`Other Offers Cancelled - JobId: ${jobId}`);
        } catch (error) {
            logger.error(`Cancel Other Offers Error - JobId: ${jobId}, Error: ${error}`);
        }
    }

    async findAlternativeDrivers(jobId: string, nearbyDrivers: string[]): Promise<string[]> {
        try {
            const rejectedDrivers = await redis.smembers(`job:${jobId}:rejected_drivers`);
            const alternatives = nearbyDrivers.filter(d => !rejectedDrivers.includes(d));

            logger.debug(`Found ${alternatives.length} alternative drivers for Job ${jobId}`);
            return alternatives;
        } catch (error) {
            logger.error(`Find Alternative Drivers Error - JobId: ${jobId}, Error: ${error}`);
            return [];
        }
    }

    private async publishAssignmentEventWithRetry(eventType: string, topic: string, jobId: string, driverId: string, payload?: any): Promise<void> {
        for (let attempt = 1; attempt <= this.MAX_KAFKA_RETRIES; attempt++) {
            try {
                await this.publishAssignmentEvent(eventType, topic, jobId, driverId, payload);
                logger.debug(`Kafka Event Published — ${eventType} — JobId: ${jobId}, Driver: ${driverId}`);
                return;
            } catch (error: any) {
                logger.warn(`Kafka Publish Failed (Attempt ${attempt}/${this.MAX_KAFKA_RETRIES}) - ${error.message}`);

                if (attempt === this.MAX_KAFKA_RETRIES) {
                    throw new Error(`Kafka publish failed after ${this.MAX_KAFKA_RETRIES} retries`);
                }

                if (error.message.includes('disconnected')) {
                    await reconnectKafka();
                }

                await new Promise(res => setTimeout(res, 200 * Math.pow(2, attempt - 1)));
            }
        }
    }

    private async publishAssignmentEvent(eventType: string, topic: string, jobId: string, driverId: string, payload?: any): Promise<void> {
        if (!isKafkaConnected()) {
            throw new Error('Kafka producer disconnected');
        }

        const healthy = await checkProducerHealth();
        if (!healthy) {
            throw new Error('Kafka producer unhealthy');
        }

        const event = {
            type: eventType,
            jobId,
            driverId,
            timestamp: new Date().toISOString(),
            ...(payload ? { offer: payload } : {})
        };

        await producer.send({
            topic,
            messages: [{
                key: driverId,
                value: JSON.stringify(event),
                headers: {
                    'job-id': jobId,
                    'event-type': eventType,
                    'timestamp': Date.now().toString(),
                },
            }],
        });
    }
}