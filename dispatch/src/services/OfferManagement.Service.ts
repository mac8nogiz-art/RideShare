import { logger } from '../logger';
import { Job } from '../types';
import { producer, isKafkaConnected, reconnectKafka, checkProducerHealth } from '../infrastructure/kafka';
import { redis } from '../infrastructure/redis';
import {
    scheduleMatchedBucketExpiry,
    scheduleOfferExpiry,
    cancelBucketExpiry,
    processDriverQueue,
    cancelOfferExpiry,
    cancelAllOffersForJob,
    cancelAllBucketsForJob,
    cancelAllDriversForJob
} from '../infrastructure/bullmq';

interface DistanceBucket {
    rangeStart: number;
    rangeEnd: number;
    drivers: string[];
}

export class OfferManagementService {
    private readonly OFFER_EXPIRY_SECONDS = 17;
    private readonly KAFKA_TOPIC_OFFERS = 'driver-offers';
    private readonly KAFKA_TOPIC_ASSIGNMENTS = 'driver-assignments';
    private readonly MAX_KAFKA_RETRIES = 2;
    private processedOffers = new Set<string>();

    private matchedDriverService: any;

    constructor(matchedDriverService?: any) {
        this.matchedDriverService = matchedDriverService;
    }

    async sendOffers(job: Job, driverIds: string[]) {
        try {
            const isMatchedFlow = await redis.get(`job:${job.id}:matched_flow_active`);
            const flowType = isMatchedFlow === '1' ? 'matched' : 'regular';

            logger.info(`Offer Flow Started — Job ${job.id} (${flowType.toUpperCase()} FLOW)`);

            if (flowType === 'matched') {
                const bucketsKey = `job:${job.id}:matched_drivers_buckets`;
                const bucketStrings = await redis.lrange(bucketsKey, 0, -1);

                if (!bucketStrings || bucketStrings.length === 0) {
                    logger.error(`No matched buckets found for Job ${job.id}`);
                    return;
                }

                const distanceBuckets: DistanceBucket[] = bucketStrings.map(str => JSON.parse(str));
                await this.sendMatchedDriverOffers(job, distanceBuckets);
                return;
            }

            const driverQueueKey = `job:${job.id}:driver_queue`;
            const queueLength = await redis.llen(driverQueueKey);

            if (queueLength === 0) {
                logger.warn(`No drivers in queue for Job ${job.id}`);
                return;
            }

            logger.info(`Found ${queueLength} drivers in queue for Job ${job.id}`);
            await processDriverQueue(job.id, driverIds, job);

            this.sendOfferToDriver(job.id, job).catch((error) => {
                logger.error(`Error sending queue offer: ${error.message}`);
            });

        } catch (error: any) {
            logger.error(`Offer flow failed for Job ${job.id}: ${error.message}`);
        }
    }

    async sendOfferToNextDriver(jobId: string, job: Job): Promise<boolean> {
        return this.sendOfferToDriver(jobId, job);
    }

    private async sendOfferToDriver(jobId: string, job: Job): Promise<boolean> {
        const driverQueueKey = `job:${jobId}:driver_queue`;
        const queueLength = await redis.llen(driverQueueKey);
        const nextDriverId = await redis.lindex(driverQueueKey, 0);

        if (queueLength === 1 && nextDriverId) {
            logger.info(`Last driver in queue for Job ${jobId} - Will trigger matched drivers if offer fails`);
            await redis.set(`job:${jobId}:last_driver_triggered`, '1', 'EX', 300);
        }

        if (!nextDriverId) {
            logger.info(`No more drivers in queue for Job ${jobId}`);
            return false;
        }

        logger.info(`Driver ${nextDriverId} queued for Job ${jobId} - BullMQ will process`);
        return true;
    }

    async processDriverOffer(jobId: string, driverId: string, jobData: Job): Promise<any> {
        try {
            logger.info(`Processing driver ${driverId} for Job ${jobId}`);

            const jobStatus = await redis.hget(`job:${jobId}`, 'status');
            if (jobStatus === 'accepted' || jobStatus === 'cancelled') {
                logger.info(`Job ${jobId} already ${jobStatus}, skipping driver ${driverId}`);
                return { success: false, reason: `job_${jobStatus}` };
            }



            await this.sendSingleOffer(jobData, driverId);
            await scheduleOfferExpiry(jobId, driverId, this.OFFER_EXPIRY_SECONDS);
            await this.updateDriverQueueStatus(jobId, driverId, "sent");

            logger.info(`Offer sent to driver ${driverId} for Job ${jobId}`);

            return { success: true, driverId };

        } catch (error: any) {
            logger.error(`Failed to process driver offer: ${error.message}`);
            await this.updateDriverQueueStatus(jobId, driverId, "failed");
            return { success: false, reason: 'processing_error', error: error.message };
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

            logger.info(`Published matched queue: ${matchedQueuePayload.totalDrivers} drivers in ${matchedQueuePayload.totalBuckets} buckets`);
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
                logger.warn(`No matched buckets data found for Job ${jobId}`);
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
            await scheduleMatchedBucketExpiry(jobId, currentIndex, this.OFFER_EXPIRY_SECONDS);

            logger.info(`Bucket expiry scheduled via BullMQ for ${this.OFFER_EXPIRY_SECONDS}s`);

            const offerPromises = currentBucket.drivers.map(driverId =>
                this.sendSingleMatchedOffer(job, driverId, currentIndex, buckets.length)
            );

            const results = await Promise.allSettled(offerPromises);
            const successful = results.filter(r => r.status === 'fulfilled').length;

            logger.info(`✅ Sent ${successful}/${currentBucket.drivers.length} offers for Bucket ${currentIndex + 1}`);

        } catch (error: any) {
            logger.error(`Error sending matched bucket: ${error.message}`);
        }
    }

    async handleRegularOfferExpiry(jobId: string, driverId: string): Promise<void> {
        try {
            logger.info(`[OfferService] Regular offer expiry triggered - Job: ${jobId}, Driver: ${driverId}`);

            const jobData = await redis.hgetall(`job:${jobId}`);
            if (jobData.status === 'accepted' || jobData.assignedDriver) {
                logger.info(`[OfferService] Job ${jobId} already accepted by ${jobData.assignedDriver}, skipping expiry`);
                return;
            }

            const offerKey = `offer:${jobId}:${driverId}`;
            const exists = await redis.exists(offerKey);

            if (!exists) {
                logger.info(`[OfferService] Offer already expired/processed - Job: ${jobId}, Driver: ${driverId}`);
            } else {
                const responseKey = `offer:response:${jobId}:${driverId}`;
                const responseData = await redis.get(responseKey);

                if (responseData) {
                    const response = JSON.parse(responseData);
                    if (response.action === 'accept') {
                        logger.info(`[OfferService] Offer already accepted, skipping expiry: Job ${jobId}, Driver ${driverId}`);
                        return;
                    }
                }

                logger.info(`[OfferService] ❌ Offer expired — Job ${jobId}, Driver: ${driverId}`);
                await this.handleOfferExpired(jobId, driverId);
            }

            // Remove the expired driver from the queue
            const driverQueueKey = `job:${jobId}:driver_queue`;
            await redis.lrem(driverQueueKey, 1, driverId);

            // Check remaining queue after removal
            const queueLength = await redis.llen(driverQueueKey);
            logger.info(`[OfferService] ${queueLength} drivers remaining in queue for Job ${jobId}`);

            if (queueLength === 0) {
                logger.warn(`[OfferService] Queue exhausted for Job ${jobId} - Sending matched offers directly`);

                const job = await this.getJobData(jobId);

                if (job) {
                    logger.info(`[OfferService] Sending matched driver offers for Job ${jobId}`);

                    await this.regenerateMatchedDrivers(jobId, job);
                } else {
                    logger.error(`[OfferService] Could not get job data for ${jobId}`);
                }
            }

        } catch (error: any) {
            logger.error(`[OfferService] Error handling regular offer expiry: ${error.message}`);
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

            logger.debug(`✅ Matched offer sent to ${driverId}`);
        } catch (error: any) {
            logger.error(`❌ Failed to send matched offer to ${driverId}: ${error.message}`);
            throw error;
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

        logger.info(`✅ Offer sent to driver ${driverId} for Job ${job.id}`);
    }

    async assignDriverToJob(jobId: string, driverId: string, customerId?: string): Promise<void> {
        logger.info(`🎯 Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);

        await Promise.all([
            cancelAllOffersForJob(jobId),
            cancelAllBucketsForJob(jobId),
            cancelAllDriversForJob(jobId),
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

        await this.cleanupMatchedFlowData(jobId);

        logger.info(`✅ Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
    }

    async handleDriverRejection(jobId: string, customerId: string, driverId: string, reason?: string): Promise<void> {
        logger.info(`❌ Driver Rejected - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);

        await cancelOfferExpiry(jobId, driverId);

        const pipeline = redis.pipeline();
        pipeline.del(`offer:${jobId}:${driverId}`);
        pipeline.srem(`driver:${driverId}:offers`, jobId);
        pipeline.srem(`job:${jobId}:pending_drivers`, driverId);
        pipeline.sadd(`job:${jobId}:rejected_drivers`, driverId);
        await pipeline.exec();

        await this.updateDriverQueueStatus(jobId, driverId, 'rejected');
        await this.deleteJobNotification(driverId);

        logger.info(`✅ Driver rejection processed - Next driver will be handled by BullMQ for Job ${jobId}`);
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

    private async handleOfferExpired(jobId: string, driverId: string): Promise<void> {
        const offerKey = `offer:${jobId}:${driverId}`;
        if (this.processedOffers.has(offerKey)) return;
        this.processedOffers.add(offerKey);

        try {
            logger.info(`⏱️ Handling expired offer — Job ${jobId}, Driver ${driverId}`);

            await redis.del(offerKey);
            await redis.srem(`driver:${driverId}:offers`, jobId);
            await redis.srem(`job:${jobId}:pending_drivers`, driverId);
            await redis.sadd(`job:${jobId}:expired_drivers`, driverId);
            await this.updateDriverQueueStatus(jobId, driverId, 'expired');
            await this.deleteJobNotification(driverId);

            logger.debug(`✅ Expired offer processed — Job ${jobId}, Driver ${driverId}`);
        } catch (err) {
            logger.error(`❌ Error handling expired offer — Job ${jobId}, Driver ${driverId}`, err);
        } finally {
            setTimeout(() => this.processedOffers.delete(offerKey), 5000);
        }
    }


    private async regenerateMatchedDrivers(jobId: string, job: Job): Promise<void> {
        try {
            if (!this.matchedDriverService) {
                logger.error(`[OfferService] MatchedDriverService not initialized for Job ${jobId}`);
                return;
            }

            logger.info(`[OfferService] 🔍 Finding fresh matched drivers for Job ${jobId}`);

            const distanceBuckets = await this.matchedDriverService.getMatchedDriversForJob(job, job.customerId);

            if (!distanceBuckets || distanceBuckets.length === 0) {
                logger.warn(`[OfferService] ⚠️ No matched drivers found for Job ${jobId}`);
                await this.cleanupMatchedFlowData(jobId);
                return;
            }

            logger.info(`[OfferService] Found ${distanceBuckets.flatMap((b: any) => b.drivers).length} drivers across ${distanceBuckets.length} buckets for Job ${jobId}`);

            // Store buckets in Redis
            const bucketsKey = `job:${jobId}:matched_drivers_buckets`;
            await redis.del(bucketsKey);

            for (const bucket of distanceBuckets) {
                await redis.rpush(bucketsKey, JSON.stringify(bucket));
            }
            await redis.expire(bucketsKey, 3600);

            // Set matched flow as active
            await redis.set(`job:${jobId}:matched_flow_active`, '1', 'EX', 3600);
            await redis.set(`job:${jobId}:current_bucket_index`, '0', 'EX', 3600);

            logger.info(`[OfferService] ✅ Stored ${distanceBuckets.length} buckets for Job ${jobId}`);

            // Send matched driver offers
            await this.sendMatchedDriverOffers(job, distanceBuckets);

        } catch (error: any) {
            logger.error(`[OfferService] Error regenerating matched drivers: ${error.message}`);
            await this.cleanupMatchedFlowData(jobId);
        }
    }

    private async cleanupMatchedFlowData(jobId: string): Promise<void> {
        try {
            await redis.del(`job:${jobId}:matched_flow_active`);
            await redis.del(`job:${jobId}:matched_drivers_buckets`);
            await redis.del(`job:${jobId}:current_bucket_index`);
            await redis.del(`job:${jobId}:matched_bucket`);
            await redis.del(`job:${jobId}:last_driver_triggered`);

            logger.debug(`🧹 Cleaned up matched flow data for Job ${jobId}`);
        } catch (error: any) {
            logger.error(`❌ Error cleaning up matched flow: ${error.message}`);
        }
    }

    async getJobData(jobId: string): Promise<Job | null> {
        try {
            const patterns = [
                `booking:${jobId}-*`,
                `booking:${jobId}`,
                `booking-${jobId}*`,
                `job:${jobId}`
            ];

            let bookingKey: string | null = null;
            for (const pattern of patterns) {
                const keys = await redis.keys(pattern);
                if (keys.length > 0) {
                    bookingKey = keys[0];
                    break;
                }
            }

            if (!bookingKey) {
                logger.warn(`No booking found for Job ${jobId}`);
                return null;
            }

            const bookingDataJson = await redis.call('JSON.GET', bookingKey) as any;
            if (!bookingDataJson) {
                logger.warn(`No JSON data found for key ${bookingKey}`);
                return null;
            }

            const payload = typeof bookingDataJson === 'string'
                ? JSON.parse(bookingDataJson)
                : bookingDataJson;

            return this.buildJobFromPayload(payload, jobId);
        } catch (error: any) {
            logger.error(`Error fetching job data: ${error.message}`);
            return null;
        }
    }

    private buildJobFromPayload(payload: any, jobId: string): Job | null {
        try {
            const customer = payload.customer;
            const pickupData = payload.tripAddress?.[0];
            const dropData = payload.tripAddress?.[payload.tripAddress.length - 1];

            if (!pickupData?.location || !customer?._id) {
                logger.error(`Invalid payload data for Job ${jobId}`);
                return null;
            }

            return {
                id: jobId,
                customerId: customer._id,
                pickupLat: pickupData.location.latitude,
                pickupLng: pickupData.location.longitude,
                dropLat: dropData?.location?.latitude,
                dropLng: dropData?.location?.longitude,
                fare: payload.grandTotal || 0,
                vehicleType: payload.selectedVehicle?.name || 'Unknown',
                tripAddress: payload.tripAddress || [],
                timestamp: payload.createdAt ? new Date(payload.createdAt).getTime() : Date.now(),
                customer: {
                    fullName: customer.fullName || 'Unknown',
                    avatar: customer.avatar || '',
                    distance: payload.expectedBilling?.kmText || 0,
                    time: payload.expectedBilling?.durationText || 'N/A'
                },
                rideDetails: payload.rideDetails || {
                    estimatedTime: payload.expectedBilling?.durationText || 'N/A',
                    estimatedDistance: payload.expectedBilling?.km || 0
                }
            };
        } catch (error: any) {
            logger.error(`Failed to build job from payload: ${error.message}`);
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

            await redis.call('JSON.SET', notificationKey, JSON.stringify(notificationData));
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