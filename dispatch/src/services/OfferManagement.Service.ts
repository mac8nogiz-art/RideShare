import { logger } from '../logger';
import { Job } from '../types';
import { producer, isKafkaConnected, reconnectKafka, checkProducerHealth } from '../infrastructure/kafka';
import { redis } from '../infrastructure/redis';
import {
    scheduleMatchedBucketExpiry,
    scheduleOfferExpiry,
    cancelBucketExpiry,
    cancelOfferExpiry,
    cancelAllOffersForJob,
    cancelAllBucketsForJob
} from '../infrastructure/bullmq';

interface DistanceBucket {
    rangeStart: number;
    rangeEnd: number;
    drivers: string[];
}

export class OfferManagementService {
    private readonly OFFER_EXPIRY_SECONDS = 17;
    private readonly MAX_OFFERS_PER_JOB = 100;
    private readonly KAFKA_TOPIC_OFFERS = 'driver-offers';
    private readonly KAFKA_TOPIC_ASSIGNMENTS = 'driver-assignments';
    private readonly MAX_KAFKA_RETRIES = 2;
    private processedOffers = new Set<string>();

    async sendOffers(job: Job, driverIds: string[]) {
        try {
            const isMatchedFlow = await redis.get(`job:${job.id}:matched_flow_active`);
            const flowType = isMatchedFlow === '1' ? 'matched' : 'regular';

            logger.info(` Offer Flow Started — Job ${job.id} (${flowType.toUpperCase()} FLOW)`);

            if (flowType === 'matched') {
                const bucketsKey = `job:${job.id}:matched_drivers_buckets`;
                const bucketStrings = await redis.lrange(bucketsKey, 0, -1);

                if (!bucketStrings || bucketStrings.length === 0) {
                    logger.error(` No matched buckets found for Job ${job.id}`);
                    return;
                }

                const distanceBuckets: DistanceBucket[] = bucketStrings.map(str => JSON.parse(str));
                await this.sendMatchedDriverOffers(job, distanceBuckets);
                return;
            }

            const driverQueueKey = `job:${job.id}:driver_queue`;
            const queueLength = await redis.llen(driverQueueKey);

            if (queueLength === 0) {
                logger.warn(` No drivers in queue for Job ${job.id}`);
                return;
            }

            logger.info(`Found ${queueLength} drivers in queue for Job ${job.id}`);

            this.sendOfferToDriver(job.id, job).catch((error) => {
                logger.error(`Error sending queue offer: ${error.message}`);
            });
        } catch (error: any) {
            logger.error(` Offer flow failed for Job ${job.id}: ${error.message}`);
        }
    }

    async sendMatchedDriverOffers(job: Job, distanceBuckets: DistanceBucket[]): Promise<void> {
        if (!distanceBuckets.length) {
            logger.warn(`No matched driver buckets available for Job ${job.id}`);
            return;
        }

        logger.info(`Starting matched driver offers for Job ${job.id} - ${distanceBuckets.length} buckets`);

        await redis.set(`job:${job.id}:current_bucket_index`, '0', 'EX', 3600);

        try {
            const matchedQueuePayload = {
                jobId: job.id,
                totalBuckets: distanceBuckets.length,
                totalDrivers: distanceBuckets.reduce((sum, bucket) => sum + bucket.drivers.length, 0),
                buckets: distanceBuckets.map((bucket, index) => ({
                    index: index,
                    range: `${bucket.rangeStart}-${bucket.rangeEnd}m`,
                    driverCount: bucket.drivers.length,
                    drivers: bucket.drivers,
                })),
                timestamp: new Date().toISOString(),
            };

            await this.publishAssignmentEventWithRetry(
                'matched_queue.created',
                this.KAFKA_TOPIC_OFFERS,
                job.id,
                'system',
                matchedQueuePayload
            );

            logger.info(` Published matched queue: ${matchedQueuePayload.totalDrivers} drivers in ${matchedQueuePayload.totalBuckets} buckets`);
        } catch (err: any) {
            logger.error(`Failed to publish matched queue: ${err.message}`);
        }

        await this.sendNextMatchedBucket(job.id, job);
    }

    async sendNextMatchedBucket(jobId: string, job: Job): Promise<void> {
        try {
            const bucketsKey = `job:${jobId}:matched_drivers_buckets`;
            const bucketStrings = await redis.lrange(bucketsKey, 0, -1);

            if (!bucketStrings || bucketStrings.length === 0) {
                logger.warn(`⚠ No matched buckets data found for Job ${jobId}`);
                return;
            }

            const buckets: DistanceBucket[] = bucketStrings.map(str => JSON.parse(str));
            const currentIndex = parseInt(await redis.get(`job:${jobId}:current_bucket_index`) || '0');

            if (currentIndex >= buckets.length) {
                logger.warn(`All matched driver buckets exhausted for Job ${jobId}`);
                await this.cleanupMatchedFlowData(jobId);
                return;
            }

            const currentBucket = buckets[currentIndex];
            const isLastBucket = currentIndex === buckets.length - 1;

            logger.info(`📤 Sending Bucket ${currentIndex + 1}/${buckets.length} (${currentBucket.rangeStart}-${currentBucket.rangeEnd}m) - ${currentBucket.drivers.length} drivers for Job ${jobId}`);

            const bucketKey = `job:${jobId}:matched_bucket`;

            await redis.hset(bucketKey, {
                bucketIndex: currentIndex,
                rangeStart: currentBucket.rangeStart,
                rangeEnd: currentBucket.rangeEnd,
                driverCount: currentBucket.drivers.length,
                drivers: JSON.stringify(currentBucket.drivers),
                isLastBucket: isLastBucket.toString(),
                sentAt: new Date().toISOString()
            });

            await redis.expire(bucketKey, this.OFFER_EXPIRY_SECONDS);

            // Schedule bucket expiry with BullMQ instead of setInterval
            await scheduleMatchedBucketExpiry(jobId, currentIndex, this.OFFER_EXPIRY_SECONDS);

            logger.info(` Bucket expiry scheduled via BullMQ for ${this.OFFER_EXPIRY_SECONDS}s`);

            const offerPromises = currentBucket.drivers.map(driverId =>
                this.sendSingleMatchedOffer(job, driverId, currentIndex, buckets.length)
            );
            console.log("offerprmosises", offerPromises)

            const results = await Promise.allSettled(offerPromises);
            const successful = results.filter(r => r.status === 'fulfilled').length;

            logger.info(` Sent ${successful}/${currentBucket.drivers.length} offers for Bucket ${currentIndex + 1}`);

        } catch (error: any) {
            logger.error(` Error sending matched bucket: ${error.message}`);
        }
    }

    /**
     * Called by BullMQ worker when bucket expires
     */
    async handleMatchedBucketExpiry(jobId: string, bucketIndex: number): Promise<void> {
        try {
            logger.info(`🕐 Bucket ${bucketIndex + 1} expired for Job ${jobId}`);

            // Check if job was already accepted
            const jobData = await redis.hgetall(`job:${jobId}`);
            if (jobData.status === 'accepted' || jobData.assignedDriver) {
                logger.info(`Job ${jobId} already accepted, skipping bucket expiry`);
                return;
            }

            // Get bucket data
            const bucketKey = `job:${jobId}:matched_bucket`;
            const bucketData = await redis.hgetall(bucketKey);

            if (!bucketData || Object.keys(bucketData).length === 0) {
                logger.warn(`No bucket data found for Job ${jobId}, Bucket ${bucketIndex}`);
                return;
            }

            const drivers = JSON.parse(bucketData.drivers || '[]');

            // Handle expired offers for all drivers in bucket
            for (const driverId of drivers) {
                await this.handleOfferExpired(jobId, driverId);
            }

            // Move to next bucket
            const bucketsKey = `job:${jobId}:matched_drivers_buckets`;
            const bucketStrings = await redis.lrange(bucketsKey, 0, -1);
            const buckets: DistanceBucket[] = bucketStrings.map(str => JSON.parse(str));

            const nextIndex = bucketIndex + 1;

            if (nextIndex < buckets.length) {
                logger.info(`Moving to next bucket ${nextIndex + 1}/${buckets.length} for Job ${jobId}`);
                await redis.set(`job:${jobId}:current_bucket_index`, nextIndex.toString());

                // Get job data to send next bucket
                const job = await this.getJobData(jobId);
                if (job) {
                    await this.sendNextMatchedBucket(jobId, job);
                }
            } else {
                logger.warn(`All matched driver buckets exhausted for Job ${jobId}`);
                await this.cleanupMatchedFlowData(jobId);
            }

        } catch (error: any) {
            logger.error(`Error handling matched bucket expiry: ${error.message}`);
        }
    }

    private async sendSingleMatchedOffer(job: Job, driverId: string, bucketIndex: number, totalBuckets: number): Promise<void> {
        try {
            const expiryTime = this.OFFER_EXPIRY_SECONDS;
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
                },
                isMatchedDriver: true,
                bucketInfo: `${bucketIndex + 1}/${totalBuckets}`
            };

            await redis.setex(`offer:matched:${job.id}:${driverId}`, this.OFFER_EXPIRY_SECONDS, JSON.stringify(offerData));
            await this.saveJobNotification(job, driverObjectId, expiryTime);

            await this.publishAssignmentEventWithRetry(
                'matched_job.offer_sent',
                this.KAFKA_TOPIC_OFFERS,
                job.id,
                driverId,
                offerData
            );

            logger.debug(` Matched offer sent to ${driverId}`);
        } catch (error: any) {
            logger.error(` Failed to send matched offer to ${driverId}: ${error.message}`);
            throw error;
        }
    }

    private async sendOfferToDriver(jobId: string, job: Job): Promise<boolean> {
        const driverQueueKey = `job:${jobId}:driver_queue`;
        const queueLength = await redis.llen(driverQueueKey);
        const nextDriverId = await redis.lindex(driverQueueKey, 0);

        if (queueLength === 1 && nextDriverId) {
            logger.info(` Last driver in queue for Job ${jobId} - Will trigger matched drivers if offer fails`);
            await redis.set(`job:${jobId}:last_driver_triggered`, '1', 'EX', 300);
        }

        if (!nextDriverId) {
            logger.info(`No more drivers in queue for Job ${jobId}`);

            const shouldTriggerMatched = await redis.get(`job:${jobId}:trigger_matched_drivers`);
            if (shouldTriggerMatched === '1') {
                logger.info(` Triggering matched driver flow for Job ${jobId}`);
                await redis.set(`job:${jobId}:queue_exhausted`, '1', 'EX', 300);
            }

            return false;
        }

        try {
            await this.updateDriverQueueStatus(jobId, nextDriverId, "sent");
            await this.sendSingleOffer(job, nextDriverId);

            // Schedule offer expiry with BullMQ instead of setInterval
            await scheduleOfferExpiry(jobId, nextDriverId, this.OFFER_EXPIRY_SECONDS);

            return true;
        } catch (error: any) {
            logger.error(` Failed to send offer to driver ${nextDriverId}: ${error.message}`);
            await this.updateDriverQueueStatus(jobId, nextDriverId, "failed");
            await redis.lpop(driverQueueKey);
            return this.sendOfferToDriver(jobId, job);
        }
    }

    async handleRegularOfferExpiry(jobId: string, driverId: string): Promise<void> {
        try {
            logger.info(`⏰ Regular offer expiry triggered - Job: ${jobId}, Driver: ${driverId}`);

            // ✅ FIRST: Check if job is already assigned
            const jobData = await redis.hgetall(`job:${jobId}`);
            if (jobData.status === 'accepted' || jobData.assignedDriver) {
                logger.info(`Job ${jobId} already accepted by ${jobData.assignedDriver}, skipping expiry`);
                return;
            }

            // ✅ SECOND: Check if this was the last driver (BEFORE checking offer existence)
            const wasLastDriver = await redis.get(`job:${jobId}:last_driver_triggered`);
            const shouldTriggerMatched = wasLastDriver === '1';

            // ✅ THIRD: Check if offer still exists
            const offerKey = `offer:${jobId}:${driverId}`;
            const exists = await redis.exists(offerKey);

            if (!exists) {
                logger.info(`Offer already expired/processed - Job: ${jobId}, Driver: ${driverId}`);

                // ✅ Still trigger matched flow if this was the last driver
                if (shouldTriggerMatched) {
                    logger.info(`🚨 Last driver expired - Triggering matched driver flow for Job ${jobId}`);
                    await redis.set(`job:${jobId}:trigger_matched_drivers`, '1', 'EX', 300);
                    await redis.set(`job:${jobId}:queue_exhausted`, '1', 'EX', 300);
                }
                return;
            }

            // ✅ Check if already accepted (for offers that still exist)
            const responseKey = `offer:response:${jobId}:${driverId}`;
            const responseData = await redis.get(responseKey);

            if (responseData) {
                const response = JSON.parse(responseData);
                if (response.action === 'accept') {
                    logger.info(`Offer already accepted, skipping expiry: Job ${jobId}, Driver ${driverId}`);
                    return;
                }
            }

            logger.info(`❌ Offer expired — Job ${jobId}, Driver ${driverId}`);

            // Handle the expired offer
            await this.handleOfferExpired(jobId, driverId);

            // Remove from queue
            const driverQueueKey = `job:${jobId}:driver_queue`;
            await redis.lpop(driverQueueKey);

            // ✅ Trigger matched flow if last driver
            if (shouldTriggerMatched) {
                logger.info(`🚨 Last driver expired - Triggering matched driver flow for Job ${jobId}`);
                await redis.set(`job:${jobId}:trigger_matched_drivers`, '1', 'EX', 300);
                await redis.set(`job:${jobId}:queue_exhausted`, '1', 'EX', 300);
                return;
            }

            // Send to next driver in queue
            const job = await this.getJobData(jobId);
            if (job) {
                await this.sendOfferToDriver(jobId, job);
            }

        } catch (error: any) {
            logger.error(`Error handling regular offer expiry: ${error.message}`);
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

        logger.info(` Offer sent to driver ${driverId} for Job ${job.id}`);
    }

    async assignDriverToJob(jobId: string, driverId: string, customerId?: string): Promise<void> {
        logger.info(` Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);

        // Cancel all pending offers and buckets for this job
        await Promise.all([
            cancelAllOffersForJob(jobId),
            cancelAllBucketsForJob(jobId)
        ]);

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
            logger.error(` Cancel Other Offers Error - JobId: ${jobId}, Error: ${err}`)
        );

        // Cleanup matched flow if active
        await this.cleanupMatchedFlowData(jobId);

        logger.info(` Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
    }

    private async cancelOtherOffers(jobId: string, acceptedDriverId: string): Promise<void> {
        try {
            const driverQueueKey = `job:${jobId}:driver_queue`;
            const remainingDrivers = await redis.lrange(driverQueueKey, 0, -1);

            if (remainingDrivers.length === 0) return;

            const pipeline = redis.pipeline();
            for (const driverId of remainingDrivers) {
                if (driverId !== acceptedDriverId) {
                    pipeline.del(`offer:${jobId}:${driverId}`);
                    pipeline.srem(`driver:${driverId}:offers`, jobId);

                    // Cancel BullMQ scheduled expiry
                    await cancelOfferExpiry(jobId, driverId);

                    this.updateDriverQueueStatus(jobId, driverId, 'cancelled').catch(err =>
                        logger.error(` Update Queue Status Error - Driver: ${driverId}, Error: ${err}`)
                    );

                    this.deleteJobNotification(driverId).catch(err =>
                        logger.error(` Delete Notification Error - Driver: ${driverId}, Error: ${err}`)
                    );
                }
            }

            pipeline.del(driverQueueKey);
            pipeline.del(`job:${jobId}:pending_drivers`);

            await pipeline.exec();

            logger.debug(`Other Offers Cancelled - JobId: ${jobId}`);
        } catch (error) {
            logger.error(` Cancel Other Offers Error - JobId: ${jobId}, Error: ${error}`);
        }
    }

    async handleDriverRejection(jobId: string, customerId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(` Driver Rejected - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        // Cancel scheduled offer expiry
        await cancelOfferExpiry(jobId, driverId);

        const pipeline = redis.pipeline();
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        pipeline.srem(`job:${jobId}:pending_drivers`, driverId);
        pipeline.sadd(`job:${jobId}:rejected_drivers`, driverId);
        pipeline.lrem(`job:${jobId}:driver_queue`, 0, driverId);
        await pipeline.exec();

        await this.updateDriverQueueStatus(jobId, driverId, 'rejected');
        await this.deleteJobNotification(driverId);

        logger.debug(` Driver Rejection Processed - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async findAlternativeDrivers(jobId: string, nearbyDrivers: string[]): Promise<string[]> {
        try {
            const rejectedDrivers = await redis.smembers(`job:${jobId}:rejected_drivers`);
            const alternatives = nearbyDrivers.filter(d => !rejectedDrivers.includes(d));

            logger.debug(`Found ${alternatives.length} alternative drivers for Job ${jobId}`);
            return alternatives;
        } catch (error) {
            logger.error(` Find Alternative Drivers Error - JobId: ${jobId}, Error: ${error}`);
            return [];
        }
    }

    private async handleOfferExpired(jobId: string, driverId: string): Promise<void> {
        const offerKey = `offer:${jobId}:${driverId}`;
        if (this.processedOffers.has(offerKey)) return;
        this.processedOffers.add(offerKey);

        try {
            logger.info(`Handling expired offer — Job ${jobId}, Driver ${driverId}`);

            await redis.del(offerKey);
            await redis.srem(`driver:${driverId}:offers`, jobId);
            await redis.srem(`job:${jobId}:pending_drivers`, driverId);
            await redis.sadd(`job:${jobId}:expired_drivers`, driverId);
            await this.updateDriverQueueStatus(jobId, driverId, 'expired');
            await this.deleteJobNotification(driverId);

            logger.debug(`Expired offer processed — Job ${jobId}, Driver ${driverId}`);
        } catch (err) {
            logger.error(`Error handling expired offer — Job ${jobId}, Driver ${driverId}`, err);
        } finally {
            setTimeout(() => this.processedOffers.delete(offerKey), 5000);
        }
    }

    private async cleanupMatchedFlowData(jobId: string): Promise<void> {
        try {
            await redis.del(`job:${jobId}:matched_flow_active`);
            await redis.del(`job:${jobId}:matched_drivers_buckets`);
            await redis.del(`job:${jobId}:current_bucket_index`);
            await redis.del(`job:${jobId}:matched_bucket`);
            await redis.del(`job:${jobId}:trigger_matched_drivers`);
            await redis.del(`job:${jobId}:last_driver_triggered`);
            await redis.del(`job:${jobId}:queue_exhausted`);

            logger.debug(`Cleaned up matched flow data for Job ${jobId}`);
        } catch (error: any) {
            logger.error(`Error cleaning up matched flow: ${error.message}`);
        }
    }

    private async getJobData(jobId: string): Promise<Job | null> {
        try {
            const jobData = await redis.hgetall(`job:${jobId}`);
            if (!jobData || Object.keys(jobData).length === 0) {
                return null;
            }
            return jobData as any as Job;
        } catch (error: any) {
            logger.error(`Error fetching job data: ${error.message}`);
            return null;
        }
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
            logger.error(` Save Job Notification Error`, error);
        }
    }

    private async deleteJobNotification(driverId: string): Promise<void> {
        try {
            await redis.del(`jobnotification:${driverId}`);
        } catch (err) {
            logger.error(` Delete Job Notification Error`, err);
        }
    }

    private async publishAssignmentEventWithRetry(eventType: string, topic: string, jobId: string, driverId: string, payload?: any): Promise<void> {
        for (let attempt = 1; attempt <= this.MAX_KAFKA_RETRIES; attempt++) {
            try {
                await this.publishAssignmentEvent(eventType, topic, jobId, driverId, payload);
                logger.debug(` Kafka Event Published — ${eventType} — JobId: ${jobId}, Driver: ${driverId}`);
                return;
            } catch (error: any) {
                logger.warn(` Kafka Publish Failed (Attempt ${attempt}/${this.MAX_KAFKA_RETRIES}) - ${error.message}`);

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