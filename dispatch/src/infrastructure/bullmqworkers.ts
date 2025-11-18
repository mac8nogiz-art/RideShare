// infrastructure/workers.ts
import { Worker, Job } from 'bullmq';
import { logger } from '../logger';
import {
    QUEUE_NAMES,
    MatchedBucketExpiryJob,
    OfferExpiryJob,
    NextBucketTriggerJob,
    DriverQueueJob,
} from './bullmq';

const connection = {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    password: process.env.REDIS_PASSWORD,
    db: parseInt(process.env.REDIS_DB || '0'),
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
};

let matchedBucketExpiryWorker: Worker | null = null;
let offerExpiryWorker: Worker | null = null;
let nextBucketTriggerWorker: Worker | null = null;
let driverQueueProcessorWorker: Worker | null = null;

let offerManagementService: any = null;

export function initializeWorkers(serviceInstance: any) {
    offerManagementService = serviceInstance;

    // Matched Bucket Expiry Worker
    matchedBucketExpiryWorker = new Worker<MatchedBucketExpiryJob>(
        QUEUE_NAMES.MATCHED_BUCKET_EXPIRY,
        async (job: Job<MatchedBucketExpiryJob>) => {
            const { jobId, bucketIndex, timestamp } = job.data;

            logger.info(`🔄 Processing bucket expiry: Job ${jobId}, Bucket ${bucketIndex}`);

            try {
                if (!offerManagementService) {
                    throw new Error('OfferManagementService not initialized');
                }

                await offerManagementService.handleMatchedBucketExpiry(jobId, bucketIndex);

                logger.info(`✅ Bucket expiry processed: Job ${jobId}, Bucket ${bucketIndex}`);
                return { success: true, jobId, bucketIndex };
            } catch (error: any) {
                logger.error(`❌ Bucket expiry processing failed: ${error.message}`);
                throw error;
            }
        },
        {
            connection,
            prefix: 'bullmq',
            concurrency: 1000,
            limiter: {
                max: 50,
                duration: 1000,
            },
        }
    );

    // Offer Expiry Worker
    offerExpiryWorker = new Worker<OfferExpiryJob>(
        QUEUE_NAMES.OFFER_EXPIRY,
        async (job: Job<OfferExpiryJob>) => {
            const { jobId, driverId, timestamp } = job.data;

            logger.info(`🔄 Processing offer expiry: Job ${jobId}, Driver ${driverId}`);

            try {
                if (!offerManagementService) {
                    throw new Error('OfferManagementService not initialized');
                }

                await offerManagementService.handleRegularOfferExpiry(jobId, driverId);
                
                logger.info(`[BullMQ] Regular offer expiry completed - Job: ${jobId}, Driver: ${driverId}`);

                // Queue exhaustion is now handled in the service method

                logger.info(`✅ Offer expiry processed: Job ${jobId}, Driver ${driverId}`);
                return { success: true, jobId, driverId };
            } catch (error: any) {
                logger.error(`❌ Offer expiry processing failed: ${error.message}`);
                throw error;
            }
        },
        {
            connection,
            prefix: 'bullmq',
            concurrency: 20,
            limiter: {
                max: 100,
                duration: 1000,
            },
        }
    );

    // Next Bucket Trigger Worker
    nextBucketTriggerWorker = new Worker<NextBucketTriggerJob>(
        QUEUE_NAMES.NEXT_BUCKET_TRIGGER,
        async (job: Job<NextBucketTriggerJob>) => {
            const { jobId, nextBucketIndex, timestamp } = job.data;

            logger.info(`🔄 Processing next bucket trigger: Job ${jobId}, Bucket ${nextBucketIndex}`);

            try {
                if (!offerManagementService) {
                    throw new Error('OfferManagementService not initialized');
                }

                const jobData = await offerManagementService.getJobData(jobId);
                if (!jobData) {
                    throw new Error(`Job data not found for ${jobId}`);
                }

                await offerManagementService.sendNextMatchedBucket(jobId, jobData);

                logger.info(`✅ Next bucket triggered: Job ${jobId}, Bucket ${nextBucketIndex}`);
                return { success: true, jobId, nextBucketIndex };
            } catch (error: any) {
                logger.error(`❌ Next bucket trigger failed: ${error.message}`);
                throw error;
            }
        },
        {
            connection,
            prefix: 'bullmq',
            concurrency: 5,
            limiter: {
                max: 20,
                duration: 1000,
            },
        }
    );

    // NEW: Driver Queue Processor Worker
    driverQueueProcessorWorker = new Worker<DriverQueueJob>(
        QUEUE_NAMES.DRIVER_QUEUE_PROCESSOR,
        async (job: Job<DriverQueueJob>) => {
            const { jobId, driverId, queuePosition, jobData } = job.data;

            logger.info(`🚗 Processing driver queue: Job ${jobId}, Driver ${driverId}, Position ${queuePosition}`);

            try {
                if (!offerManagementService) {
                    throw new Error('OfferManagementService not initialized');
                }

                // Process the driver offer
                const result = await offerManagementService.processDriverOffer(jobId, driverId, jobData);
                console.log(result, " sheer")

                if (result.success) {
                    logger.info(`✅ Driver offer processed: Job ${jobId}, Driver ${driverId}`);
                } else {
                    logger.warn(`⚠️ Driver offer skipped: Job ${jobId}, Driver ${driverId} - ${result.reason}`);
                }

                return { success: true, jobId, driverId, queuePosition, ...result };
            } catch (error: any) {
                logger.error(`❌ Driver queue processing failed: ${error.message}`);
                throw error;
            }
        },
        {
            connection,
            prefix: 'bullmq',
            concurrency: 10, // Process up to 10 driver offers simultaneously
            limiter: {
                max: 50, // Max 50 offers per second
                duration: 1000,
            },
        }
    );

    setupWorkerEventListeners();
    logger.info('✅ All BullMQ workers initialized successfully');
}



function setupWorkerEventListeners() {

    if (matchedBucketExpiryWorker) {
        matchedBucketExpiryWorker.on('completed', (job) => {
            logger.debug(` Bucket expiry job completed: ${job.id}`);
        });

        matchedBucketExpiryWorker.on('failed', (job, err) => {
            logger.error(`Bucket expiry job failed: ${job?.id} - ${err.message}`);
        });

        matchedBucketExpiryWorker.on('error', (err) => {
            logger.error(` Bucket expiry worker error: ${err.message}`);
        });

        matchedBucketExpiryWorker.on('stalled', (jobId) => {
            logger.warn(`Bucket expiry job stalled: ${jobId}`);
        });
    }

    // Offer Expiry Worker Events
    if (offerExpiryWorker) {
        offerExpiryWorker.on('completed', (job) => {
            logger.debug(`Offer expiry job completed: ${job.id}`);
        });

        offerExpiryWorker.on('failed', (job, err) => {
            logger.error(`Offer expiry job failed: ${job?.id} - ${err.message}`);
        });

        offerExpiryWorker.on('error', (err) => {
            logger.error(` Offer expiry worker error: ${err.message}`);
        });

        offerExpiryWorker.on('stalled', (jobId) => {
            logger.warn(` Offer expiry job stalled: ${jobId}`);
        });
    }

    // Next Bucket Trigger Worker Events
    if (nextBucketTriggerWorker) {
        nextBucketTriggerWorker.on('completed', (job) => {
            logger.info(`Next bucket trigger completed: ${job.id}`);
        });

        nextBucketTriggerWorker.on('failed', (job, err) => {
            logger.error(`Next bucket trigger failed: ${job?.id} - ${err.message}`);
        });

        nextBucketTriggerWorker.on('error', (err) => {
            logger.error(` Next bucket trigger worker error: ${err.message}`);
        });

        nextBucketTriggerWorker.on('stalled', (jobId) => {
            logger.warn(` Next bucket trigger stalled: ${jobId}`);
        });
    }

    // NEW: Driver Queue Processor Worker Events
    if (driverQueueProcessorWorker) {
        driverQueueProcessorWorker.on('completed', (job) => {
            logger.debug(`Driver queue job completed: ${job.id}`);
        });

        driverQueueProcessorWorker.on('failed', (job, err) => {
            logger.error(` Driver queue job failed: ${job?.id} - ${err.message}`);
        });

        driverQueueProcessorWorker.on('error', (err) => {
            logger.error(` Driver queue worker error: ${err.message}`);
        });

        driverQueueProcessorWorker.on('stalled', (jobId) => {
            logger.warn(` Driver queue job stalled: ${jobId}`);
        });

        driverQueueProcessorWorker.on('active', (job) => {
            logger.debug(`Driver queue job active: ${job.id}`);
        });

        driverQueueProcessorWorker.on('progress', (job, progress) => {
            logger.debug(`Driver queue job progress: ${job.id} - ${progress}%`);
        });
    }
}

export function areWorkersRunning(): boolean {
    return !!(
        matchedBucketExpiryWorker?.isRunning() &&
        offerExpiryWorker?.isRunning() &&
        nextBucketTriggerWorker?.isRunning() &&
        driverQueueProcessorWorker?.isRunning()
    );
}

export async function pauseAllWorkers(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryWorker?.pause(),
            offerExpiryWorker?.pause(),
            nextBucketTriggerWorker?.pause(),
            driverQueueProcessorWorker?.pause(),
        ]);
        logger.info('All workers paused');
    } catch (error: any) {
        logger.error(`Failed to pause workers: ${error.message}`);
    }
}


export async function resumeAllWorkers(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryWorker?.resume(),
            offerExpiryWorker?.resume(),
            nextBucketTriggerWorker?.resume(),
            driverQueueProcessorWorker?.resume(),
        ]);
        logger.info('All workers resumed');
    } catch (error: any) {
        logger.error(`Failed to resume workers: ${error.message}`);
    }
}


export async function getWorkerMetrics() {
    try {
        const metrics = {
            matchedBucketExpiry: {
                isRunning: matchedBucketExpiryWorker?.isRunning() || false,
                isPaused: matchedBucketExpiryWorker?.isPaused() || false,
            },
            offerExpiry: {
                isRunning: offerExpiryWorker?.isRunning() || false,
                isPaused: offerExpiryWorker?.isPaused() || false,
            },
            nextBucketTrigger: {
                isRunning: nextBucketTriggerWorker?.isRunning() || false,
                isPaused: nextBucketTriggerWorker?.isPaused() || false,
            },
            driverQueueProcessor: {
                isRunning: driverQueueProcessorWorker?.isRunning() || false,
                isPaused: driverQueueProcessorWorker?.isPaused() || false,
            },
            timestamp: new Date().toISOString(),
        };

        return metrics;
    } catch (error: any) {
        logger.error(`Failed to get worker metrics: ${error.message}`);
        return null;
    }
}

export async function closeAllWorkers(): Promise<void> {
    try {
        logger.info('🔄 Closing all workers...');

        await Promise.all([
            matchedBucketExpiryWorker?.close(),
            offerExpiryWorker?.close(),
            nextBucketTriggerWorker?.close(),
            driverQueueProcessorWorker?.close(),
        ]);

        logger.info('✅ All workers closed successfully');
    } catch (error: any) {
        logger.error(`Error closing workers: ${error.message}`);
        throw error;
    }
}

export {
    matchedBucketExpiryWorker,
    offerExpiryWorker,
    nextBucketTriggerWorker,
    driverQueueProcessorWorker,
};



process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, closing workers...');
    await closeAllWorkers();
});

process.on('SIGINT', async () => {
    logger.info('SIGINT received, closing workers...');
    await closeAllWorkers();
});