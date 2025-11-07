import { logger } from '../logger';
import { Job } from '../types';
import { producer, isKafkaConnected, reconnectKafka, checkProducerHealth } from '../infrastructure/kafka';
import { redis } from '../infrastructure/redis';
import RedisSubscriber from "../redisConnection/subscriber";

export class OfferManagementService {
    private readonly OFFER_EXPIRY_SECONDS = 15;
    private readonly KAFKA_TOPIC_OFFERS = 'driver-offers';
    private readonly KAFKA_TOPIC_ASSIGNMENTS = 'driver-assignments';
    private readonly MAX_KAFKA_RETRIES = 2;
    private processedOffers = new Set<string>();



    // ---------------- Expiry Listener Setup ----------------


    // ---------------- Core Offer Flow ----------------
    async sendOffers(job: Job, driverIds: string[]) {
        logger.info(`Event-driven Offer Flow Started — Job ${job.id}`);
        if (!driverIds.length) return logger.warn(`No drivers available for Job ${job.id}`);

        let index = 0;
        let jobAssigned = false;

        const processNextDriver = (driverId: string) => {
            this.updateDriverQueueStatus(job.id, driverId, "sent").catch(err =>
                logger.error(`Queue Status Error (sent) - ${driverId}`, err)
            );

            this.sendSingleOffer(job, driverId)
                .then(() => this.waitForDriverResponseOrExpiry(job.id, driverId, async (accepted) => {
                    if (jobAssigned) return;

                    if (accepted) {
                        jobAssigned = true;
                        await this.updateDriverQueueStatus(job.id, driverId, "accepted");
                        logger.info(`Driver ${driverId} accepted Job ${job.id}`);
                        await this.assignDriverToJob(job.id, driverId, job.customerId);
                        return;
                    }

                    await this.updateDriverQueueStatus(job.id, driverId, "expired");

                    if (index < driverIds.length) {
                        const nextDriver = driverIds[index++];
                        this.updateDriverQueueStatus(job.id, nextDriver, "waiting")
                            .catch(err => logger.error(`Queue Status Error (waiting)`, err));
                        processNextDriver(nextDriver);
                    }
                }))
                .catch(async () => {
                    logger.error(`Offer sending failed to driver ${driverId}`);
                    await this.updateDriverQueueStatus(job.id, driverId, "failed");

                    if (index < driverIds.length) {
                        const nextDriver = driverIds[index++];
                        this.updateDriverQueueStatus(job.id, nextDriver, "waiting")
                            .catch(err => logger.error(`Queue Status Error (waiting)`, err));
                        processNextDriver(nextDriver);
                    }
                });
        };

        await this.updateDriverQueueStatus(job.id, driverIds[0], "waiting");
        processNextDriver(driverIds[index++]);
    }

    // ---------------- Expiry Handler ----------------
    // private async handleOfferExpired(jobId: string, driverId: string) {
    //     const offerKey = `offer:${jobId}:${driverId}`;
    //     if (this.processedOffers.has(offerKey)) return;
    //     this.processedOffers.add(offerKey);
    //
    //     try {
    //         logger.info(`Handling expired offer — Job ${jobId}, Driver ${driverId}`);
    //
    //         await redis.del(offerKey);
    //         await redis.srem(`driver:${driverId}:offers`, jobId);
    //         await redis.srem(`job:${jobId}:pending_drivers`, driverId);
    //         await redis.sadd(`job:${jobId}:rejected_drivers`, driverId);
    //         await this.updateDriverQueueStatus(jobId, driverId, 'expired');
    //         await this.deleteJobNotification(driverId);
    //
    //         const pendingDrivers = await redis.smembers(`job:${jobId}:pending_drivers`);
    //         if (pendingDrivers.length > 0) {
    //             const nextDriver = pendingDrivers[0];
    //             await this.updateDriverQueueStatus(jobId, nextDriver, 'waiting');
    //             this.sendSingleOffer({ id: jobId } as Job, nextDriver)
    //                 .catch(err => logger.error(`Failed to send offer to next driver ${nextDriver}`, err));
    //         }
    //
    //         logger.info(`Expired offer processed — Job ${jobId}, Driver ${driverId}`);
    //     } catch (err) {
    //         logger.error(`Error handling expired offer — Job ${jobId}, Driver ${driverId}`, err);
    //     } finally {
    //         setTimeout(() => this.processedOffers.delete(offerKey), 5000);
    //     }
    // }

    // ---------------- Driver Response ----------------
    // private async waitForDriverResponseOrExpiry(jobId: string, driverId: string, cb: (accepted: boolean) => void) {
    //     const responseChannel = `offer.response.${jobId}.${driverId}`;
    //     const offerKey = `offer:${jobId}:${driverId}`;
    //     let resolved = false;
    //
    //     const cleanup = async () => {
    //         await this.subscriber.unsubscribe(responseChannel);
    //     };
    //
    //     await this.subscriber.subscribe(responseChannel, async (msg) => {
    //         if (resolved) return;
    //         if (msg?.action === "accept") {
    //             resolved = true;
    //             await cleanup();
    //             cb(true);
    //         }
    //     });
    //
    //     this.subscriber.addExpiryHandler(async (expiredKey: string) => {
    //         if (resolved) return;
    //         if (expiredKey !== offerKey) return;
    //
    //         resolved = true;
    //         logger.info(`⏳ Offer expired — Job ${jobId}, Driver ${driverId}`);
    //         await cleanup();
    //         cb(false);
    //     });
    // }

    private async handleOfferExpired(jobId: string, driverId: string): Promise<void> {
        const offerKey = `offer:${jobId}:${driverId}`;
        if (this.processedOffers.has(offerKey)) return; // Already handled
        this.processedOffers.add(offerKey);

        try {
            logger.info(`Handling expired offer — Job ${jobId}, Driver ${driverId}`);

            // Remove offer key
            await redis.del(offerKey);
            await redis.srem(`driver:${driverId}:offers`, jobId);
            await redis.srem(`job:${jobId}:pending_drivers`, driverId);
            await redis.sadd(`job:${jobId}:rejected_drivers`, driverId);

            // Update queue status
            await this.updateDriverQueueStatus(jobId, driverId, 'expired');

            // Delete job notification
            await this.deleteJobNotification(driverId);

            // Optionally trigger next driver
            const pendingDrivers = await redis.smembers(`job:${jobId}:pending_drivers`);
            if (pendingDrivers.length > 0) {
                const nextDriver = pendingDrivers[0];
                await this.updateDriverQueueStatus(jobId, nextDriver, 'waiting');
                this.sendSingleOffer({ id: jobId } as Job, nextDriver).catch(err =>
                    logger.error(`Failed to send offer to next driver ${nextDriver}`, err)
                );
            }

            logger.info(`Expired offer processed — Job ${jobId}, Driver ${driverId}`);
        } catch (err) {
            logger.error(`Error handling expired offer — Job ${jobId}, Driver ${driverId}`, err);
        } finally {
            // Remove from processedOffers after a short delay if you want retries possible
            setTimeout(() => this.processedOffers.delete(offerKey), 5000);
        }
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
                distance: (job as any).customer?.distance,
                time: (job as any).customer?.time || "",
            }
        };

        await redis.setex(`offer:${job.id}:${driverId}`, this.OFFER_EXPIRY_SECONDS, JSON.stringify(offerData));

        await this.saveJobNotification(job, driverObjectId, expiryTime);

        await this.publishAssignmentEventWithRetry(
            'new_job.offer_sent',
            this.KAFKA_TOPIC_OFFERS,
            job.id,
            driverId,
            offerData
        );

        logger.info(`Offer sent to driver ${driverId} for Job ${job.id}`);
    }

    private async waitForDriverResponseOrExpiry(
        jobId: string,
        driverId: string,
        cb: (accepted: boolean) => void,
    ) {


        const responseChannel = `offer.response.${jobId}.${driverId}`;
        const offerKey = `offer:${jobId}:${driverId}`;
        let resolved = false;

        const cleanup = async () => {
            await this.subscriber.unsubscribe(responseChannel);
        };

        await this.subscriber.subscribe(responseChannel, async (msg) => {
            if (resolved) return;

            if (msg?.action === "accept") {
                resolved = true;
                await cleanup();
                cb(true);
            }
        });


        this.subscriber.addExpiryHandler(async (expiredKey: string) => {
            if (resolved) return;
            if (expiredKey !== offerKey) return;

            resolved = true;
            logger.info(`⏳ Offer expired — Job ${jobId}, Driver ${driverId}`);
            await cleanup();
            cb(false);
        });
    }

    private async updateDriverQueueStatus(jobId: string, driverId: string, status: string): Promise<void> {
        try {
            const driverHashKey = `job:${jobId}:driver:${driverId}`;
            const exists = await redis.exists(driverHashKey);
            if (!exists) return;

            await redis.hset(driverHashKey, {
                status,
                [`${status}At`]: new Date().toISOString(),
            });
        } catch (error) {
            logger.error(`Update Driver Queue Status Error`, error);
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
                rideDetails: job.rideDetails || { estimatedTime: "", estimatedDistance: 0 },
                askDriver: { expTime: expiryTimestamp.toISOString() },
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
        } catch (error) {
            logger.error(`Save Job Notification Error`, error);
        }
    }

    private async deleteJobNotification(driverId: string): Promise<void> {
        try {
            await redis.del(`jobnotification:${driverId}`);
        } catch (err) {
            logger.error(`Delete Job Notification Error`, err);
        }
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

    async handleDriverRejection(jobId: string, customerId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(`Driver Rejected - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        const pipeline = redis.pipeline();
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        pipeline.srem(`job:${jobId}:pending_drivers`, driverId);
        pipeline.sadd(`job:${jobId}:rejected_drivers`, driverId);
        await pipeline.exec();


        await this.updateDriverQueueStatus(jobId, driverId, 'rejected');

        await this.deleteJobNotification(driverId);

        logger.debug(`Driver Rejection Processed - JobId: ${jobId}, Driver: ${driverId}`);
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
