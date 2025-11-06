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
        logger.info(`Event-Based Offer Dispatch Started — JobId: ${job.id}, Total Drivers: ${driverIds.length}`);

        return new Promise(async (resolve) => {
            let successful = 0;
            let failed = 0;
            let currentDriverIndex = 0;
            let jobAssigned = false;

            const processNextDriver = async (): Promise<void> => {
                if (jobAssigned || currentDriverIndex >= driverIds.length) {
                    logger.info(`Offer Dispatch Complete — JobId: ${job.id}, Successful: ${successful}, Failed: ${failed}`);
                    resolve({ successful, failed });
                    return;
                }

                const driverId = driverIds[currentDriverIndex];
                currentDriverIndex++;

                try {
                    await this.sendSingleOffer(job, driverId);
                    successful++;
                    await this.updateDriverQueueStatus(job.id, driverId, 'offered');
                    logger.info(`Offer Sent - JobId: ${job.id}, Driver: ${driverId}`);

                    // Wait for driver response or expiry
                    const accepted = await this.waitForDriverResponseOrExpiry(job.id, driverId);

                    if (accepted) {
                        logger.info(`Driver ${driverId} accepted Job ${job.id}. Stopping offer cycle.`);
                        jobAssigned = true;
                        await this.updateDriverQueueStatus(job.id, driverId, 'accepted');
                        await this.assignDriverToJob(job.id, driverId, job.customerId);
                        this.cancelOtherOffers(job.id, driverId).catch(err => {
                            logger.error(`Cancel Other Offers Error - JobId: ${job.id}, Error: ${err}`);
                        });
                    } else {
                        logger.warn(`Driver ${driverId} did not respond in time for Job ${job.id}. Auto-rejecting.`);
                        await this.updateDriverQueueStatus(job.id, driverId, 'timeout');
                        await this.handleDriverRejection(job.id, job.customerId, driverId, 'Offer timed out');
                        setTimeout(processNextDriver, 200);
                    }

                } catch (error: any) {
                    failed++;
                    await this.updateDriverQueueStatus(job.id, driverId, 'failed').catch(() => {});
                    logger.error(`Failed to send offer to Driver ${driverId}: ${error?.message || error}`);
                    setTimeout(processNextDriver, 200);
                }
            };

            // Set up Redis event listeners
            await this.setupEventListeners(job.id, driverIds);

            // Start processing drivers
            processNextDriver();
        });
    }

    private async setupEventListeners(jobId: string, driverIds: string[]): Promise<void> {
        // Subscribe to driver response channels
        const subscribePromises = driverIds.map(driverId =>
            redisSubscriber.subscribe(`driver_response:${jobId}:${driverId}`)
        );
        await Promise.all(subscribePromises);

        // Subscribe to key expiry events for auto-rejection
        await redisSubscriber.subscribe('__keyevent@0__:expired');
    }

    private async waitForDriverResponseOrExpiry(jobId: string, driverId: string): Promise<boolean> {
        return new Promise((resolve) => {
            const responseChannel = `driver_response:${jobId}:${driverId}`;
            const offerKey = `offer:${jobId}:${driverId}`;

            const onMessage = (channel: string, message: string) => {
                try {
                    if (channel === responseChannel) {
                        const data = JSON.parse(message);
                        cleanup();
                        resolve(data.action === 'accept');
                    } else if (channel === '__keyevent@0__:expired' && message === offerKey) {
                        cleanup();
                        resolve(false);
                    }
                } catch (error) {
                    logger.error(`Parse event error: ${error}`);
                    cleanup();
                    resolve(false);
                }
            };

            const cleanup = () => {
                redisSubscriber.off('message', onMessage);
                redisSubscriber.unsubscribe(responseChannel);
            };

            // Set timeout as fallback
            const timeoutId = setTimeout(() => {
                cleanup();
                resolve(false);
            }, this.OFFER_EXPIRY_SECONDS * 1000);

            redisSubscriber.on('message', onMessage);
        });
    }

    private async sendSingleOffer(job: Job, driverId: string): Promise<void> {
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
                distance: (job as any).customer?.distance ,
                time: (job as any).customer?.time || "",
            }
        };

        await redis.setex(`offer:${job.id}:${driverId}`, this.OFFER_EXPIRY_SECONDS, JSON.stringify(offerData));
        await this.saveJobNotification(job, driverObjectId, expiryTime);
        await this.publishAssignmentEventWithRetry('new_job.offer_sent', this.KAFKA_TOPIC_OFFERS, job.id, driverId, offerData);

    }

    private async updateDriverQueueStatus(jobId: string, driverId: string, status: string): Promise<void> {
        try {
            const driverHashKey = `job:${jobId}:driver:${driverId}`;

            const exists = await redis.exists(driverHashKey);
            if (!exists) {
                logger.warn(`Driver hash key not found - JobId: ${jobId}, Driver: ${driverId}`);
                return;
            }

            const pipeline = redis.pipeline();

            pipeline.hset(driverHashKey, 'status', status);

            pipeline.hset(driverHashKey, `${status}At`, new Date().toISOString());

            await pipeline.exec();

            // const statusEvent = {
            //     jobId,
            //     driverId,
            //     status,
            //     timestamp: new Date().toISOString()
            // };

            // await redis.publish(
            //     `job:${jobId}:driver_status_updates`,
            //     JSON.stringify(statusEvent)
            // // );

            logger.debug(`Driver Queue Status Updated - JobId: ${jobId}, Driver: ${driverId}, Status: ${status}`);
        } catch (error) {
            logger.error(`Update Driver Queue Status Error - JobId: ${jobId}, Driver: ${driverId}, Status: ${status}, Error: ${error}`);
            throw error;
        }
    }
    private async saveJobNotification(job: Job, driverId: string, expiryTime: number): Promise<void> {
        try {
            const notificationKey = `jobnotification:${driverId}`;

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

            await redis.call('JSON.SET', notificationKey, '$', JSON.stringify(notificationData));
            await redis.expire(notificationKey, this.OFFER_EXPIRY_SECONDS + 5);

            logger.info(`Job Notification Saved - Driver: ${driverId}, JobId: ${job.id}, TTL: ${this.OFFER_EXPIRY_SECONDS + 5}s`);
        } catch (error) {
            logger.error(`Save Job Notification Error - Driver: ${driverId}, JobId: ${job.id}, Error: ${error}`);
        }
    }

    private async deleteJobNotification(driverId: string): Promise<void> {
        try {
            const notificationKey = `jobnotification:${driverId}`;
            await redis.del(notificationKey);
            logger.debug(`Job Notification Deleted - Driver: ${driverId}`);
        } catch (error) {
            logger.error(`Delete Job Notification Error - Driver: ${driverId}, Error: ${error}`);
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
                        resolve(data.type === "accepted");
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

        await this.updateDriverQueueStatus(jobId, driverId, 'accepted');

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

        // Update driver queue status to rejected
        await this.updateDriverQueueStatus(jobId, driverId, 'rejected');

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


                    this.updateDriverQueueStatus(jobId, driverId, 'cancelled').catch(err =>
                        logger.error(`Update Queue Status Error - Driver: ${driverId}, Error: ${err}`)
                    );

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