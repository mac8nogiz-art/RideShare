// infrastructure/bullmq.ts
import { Queue, QueueEvents, ConnectionOptions } from 'bullmq';
import { logger } from '../logger';

// ===========================
// QUEUE NAMES
// ===========================
export const QUEUE_NAMES = {
    MATCHED_BUCKET_EXPIRY: 'matched-bucket-expiry',
    OFFER_EXPIRY: 'offer-expiry',
    NEXT_BUCKET_TRIGGER: 'next-bucket-trigger',
} as const;

// ===========================
// JOB DATA INTERFACES
// ===========================
export interface MatchedBucketExpiryJob {
    jobId: string;
    bucketIndex: number;
    timestamp: string;
}

export interface OfferExpiryJob {
    jobId: string;
    driverId: string;
    timestamp: string;
}

export interface NextBucketTriggerJob {
    jobId: string;
    nextBucketIndex: number;
    timestamp: string;
}

// ===========================
// CONNECTION OPTIONS
// ===========================
const connection: ConnectionOptions = {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    password: process.env.REDIS_PASSWORD,
    db: parseInt(process.env.REDIS_DB || '0'),
    maxRetriesPerRequest: null, // Required for BullMQ
    enableReadyCheck: false,
};

const queueOptions = {
    connection,
    prefix: 'bullmq',
};

// ===========================
// QUEUE INSTANCES
// ===========================

// Matched Bucket Expiry Queue
export const matchedBucketExpiryQueue = new Queue<MatchedBucketExpiryJob>(
    QUEUE_NAMES.MATCHED_BUCKET_EXPIRY,
    {
        ...queueOptions,
        defaultJobOptions: {
            removeOnComplete: {
                count: 100,
                age: 3600, // 1 hour
            },
            removeOnFail: {
                count: 500,
                age: 7200, // 2 hours
            },
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 2000,
            },
        },
    }
);

// Offer Expiry Queue
export const offerExpiryQueue = new Queue<OfferExpiryJob>(
    QUEUE_NAMES.OFFER_EXPIRY,
    {
        ...queueOptions,
        defaultJobOptions: {
            removeOnComplete: {
                count: 1000,
                age: 1800, // 30 minutes
            },
            removeOnFail: {
                count: 500,
                age: 3600, // 1 hour
            },
            attempts: 2,
            backoff: {
                type: 'exponential',
                delay: 1000,
            },
        },
    }
);

// Next Bucket Trigger Queue
export const nextBucketTriggerQueue = new Queue<NextBucketTriggerJob>(
    QUEUE_NAMES.NEXT_BUCKET_TRIGGER,
    {
        ...queueOptions,
        defaultJobOptions: {
            removeOnComplete: {
                count: 100,
                age: 3600,
            },
            removeOnFail: {
                count: 200,
                age: 7200,
            },
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 1500,
            },
        },
    }
);

// ===========================
// QUEUE EVENTS (MONITORING)
// ===========================
export const matchedBucketExpiryEvents = new QueueEvents(
    QUEUE_NAMES.MATCHED_BUCKET_EXPIRY,
    queueOptions
);

export const offerExpiryEvents = new QueueEvents(
    QUEUE_NAMES.OFFER_EXPIRY,
    queueOptions
);

export const nextBucketTriggerEvents = new QueueEvents(
    QUEUE_NAMES.NEXT_BUCKET_TRIGGER,
    queueOptions
);

// ===========================
// HELPER FUNCTIONS
// ===========================

/**
 * Schedule a matched bucket to expire after specified seconds
 */
export async function scheduleMatchedBucketExpiry(
    jobId: string,
    bucketIndex: number,
    delaySeconds: number
): Promise<void> {
    try {
        const jobKey = `bucket-expiry-${jobId}-${bucketIndex}`;

        await matchedBucketExpiryQueue.add(
            'bucket-expiry',
            {
                jobId,
                bucketIndex,
                timestamp: new Date().toISOString(),
            },
            {
                delay: delaySeconds * 1000,
                jobId: jobKey,
                removeOnComplete: true,
                removeOnFail: false,
            }
        );

        logger.info(`Scheduled bucket expiry: Job ${jobId}, Bucket ${bucketIndex}, Delay ${delaySeconds}s`);
    } catch (error: any) {
        logger.error(`Failed to schedule bucket expiry: ${error.message}`);
        throw error;
    }
}

/**
 * Schedule an offer to expire after specified seconds
 */
export async function scheduleOfferExpiry(
    jobId: string,
    driverId: string,
    delaySeconds: number
): Promise<void> {
    try {
        const jobKey = `offer-expiry-${jobId}-${driverId}`;

        await offerExpiryQueue.add(
            'offer-expiry',
            {
                jobId,
                driverId,
                timestamp: new Date().toISOString(),
            },
            {
                delay: delaySeconds * 1000,
                jobId: jobKey,
                removeOnComplete: true,
                removeOnFail: false,
            }
        );

        logger.debug(`Scheduled offer expiry: Job ${jobId}, Driver ${driverId}, Delay ${delaySeconds}s`);
    } catch (error: any) {
        logger.error(`Failed to schedule offer expiry: ${error.message}`);
        throw error;
    }
}

/**
 * Cancel a scheduled bucket expiry
 */
export async function cancelBucketExpiry(jobId: string, bucketIndex: number): Promise<void> {
    try {
        const jobKey = `bucket-expiry-${jobId}-${bucketIndex}`;
        const job = await matchedBucketExpiryQueue.getJob(jobKey);

        if (job) {
            await job.remove();
            logger.info(`Cancelled bucket expiry: Job ${jobId}, Bucket ${bucketIndex}`);
        }
    } catch (error: any) {
        logger.error(`Failed to cancel bucket expiry: ${error.message}`);
    }
}

/**
 * Cancel a scheduled offer expiry
 */
export async function cancelOfferExpiry(jobId: string, driverId: string): Promise<void> {
    try {
        const jobKey = `offer-expiry-${jobId}-${driverId}`;
        const job = await offerExpiryQueue.getJob(jobKey);

        if (job) {
            await job.remove();
            logger.debug(`Cancelled offer expiry: Job ${jobId}, Driver ${driverId}`);
        }
    } catch (error: any) {
        logger.error(`Failed to cancel offer expiry: ${error.message}`);
    }
}

/**
 * Cancel all offers for a job
 */
export async function cancelAllOffersForJob(jobId: string): Promise<void> {
    try {
        const jobs = await offerExpiryQueue.getJobs(['delayed', 'waiting']);
        const jobsToRemove = jobs.filter(job => job.data.jobId === jobId);

        await Promise.all(jobsToRemove.map(job => job.remove()));

        logger.info(`Cancelled ${jobsToRemove.length} offer expiries for Job ${jobId}`);
    } catch (error: any) {
        logger.error(`Failed to cancel offers for job: ${error.message}`);
    }
}

/**
 * Cancel all buckets for a job
 */
export async function cancelAllBucketsForJob(jobId: string): Promise<void> {
    try {
        const jobs = await matchedBucketExpiryQueue.getJobs(['delayed', 'waiting']);
        const jobsToRemove = jobs.filter(job => job.data.jobId === jobId);

        await Promise.all(jobsToRemove.map(job => job.remove()));

        logger.info(`Cancelled ${jobsToRemove.length} bucket expiries for Job ${jobId}`);
    } catch (error: any) {
        logger.error(`Failed to cancel buckets for job: ${error.message}`);
    }
}

/**
 * Health check for BullMQ
 */
export async function checkBullMQHealth(): Promise<boolean> {
    try {
        const client1 = await matchedBucketExpiryQueue.client;
        const client2 = await offerExpiryQueue.client;
        const client3 = await nextBucketTriggerQueue.client;

        await Promise.all([
            client1.ping(),
            client2.ping(),
            client3.ping(),
        ]);
        return true;
    } catch (error: any) {
        logger.error(`BullMQ health check failed: ${error.message}`);
        return false;
    }
}

/**
 * Get queue metrics
 */
export async function getBullMQMetrics() {
    try {
        const [
            matchedBucketCounts,
            offerExpiryCounts,
            nextBucketCounts,
        ] = await Promise.all([
            matchedBucketExpiryQueue.getJobCounts(),
            offerExpiryQueue.getJobCounts(),
            nextBucketTriggerQueue.getJobCounts(),
        ]);

        return {
            matchedBucketExpiry: matchedBucketCounts,
            offerExpiry: offerExpiryCounts,
            nextBucketTrigger: nextBucketCounts,
            timestamp: new Date().toISOString(),
        };
    } catch (error: any) {
        logger.error(`Failed to get BullMQ metrics: ${error.message}`);
        return null;
    }
}

/**
 * Pause all queues
 */
export async function pauseAllQueues(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryQueue.pause(),
            offerExpiryQueue.pause(),
            nextBucketTriggerQueue.pause(),
        ]);
        logger.info('All queues paused');
    } catch (error: any) {
        logger.error(`Failed to pause queues: ${error.message}`);
    }
}

/**
 * Resume all queues
 */
export async function resumeAllQueues(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryQueue.resume(),
            offerExpiryQueue.resume(),
            nextBucketTriggerQueue.resume(),
        ]);
        logger.info('All queues resumed');
    } catch (error: any) {
        logger.error(`Failed to resume queues: ${error.message}`);
    }
}

/**
 * Graceful shutdown
 */
export async function closeBullMQConnections(): Promise<void> {
    try {
        logger.info('Closing BullMQ connections...');

        await Promise.all([
            matchedBucketExpiryQueue.close(),
            offerExpiryQueue.close(),
            nextBucketTriggerQueue.close(),
            matchedBucketExpiryEvents.close(),
            offerExpiryEvents.close(),
            nextBucketTriggerEvents.close(),
        ]);

        logger.info('BullMQ connections closed successfully');
    } catch (error: any) {
        logger.error(`Error closing BullMQ connections: ${error.message}`);
        throw error;
    }
}

// ===========================
// EVENT LISTENERS
// ===========================

// Matched Bucket Expiry Events
matchedBucketExpiryEvents.on('completed', ({ jobId }) => {
    logger.debug(`Matched bucket expiry job completed: ${jobId}`);
});

matchedBucketExpiryEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`Matched bucket expiry job failed: ${jobId} - ${failedReason}`);
});

matchedBucketExpiryEvents.on('active', ({ jobId }) => {
    logger.debug(`Matched bucket expiry job active: ${jobId}`);
});

// Offer Expiry Events
offerExpiryEvents.on('completed', ({ jobId }) => {
    logger.debug(`Offer expiry job completed: ${jobId}`);
});

offerExpiryEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`Offer expiry job failed: ${jobId} - ${failedReason}`);
});

offerExpiryEvents.on('active', ({ jobId }) => {
    logger.debug(`Offer expiry job active: ${jobId}`);
});

// Next Bucket Trigger Events
nextBucketTriggerEvents.on('completed', ({ jobId }) => {
    logger.debug(`Next bucket trigger job completed: ${jobId}`);
});

nextBucketTriggerEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`Next bucket trigger job failed: ${jobId} - ${failedReason}`);
});

nextBucketTriggerEvents.on('active', ({ jobId }) => {
    logger.debug(`Next bucket trigger job active: ${jobId}`);
});

logger.info('✓ BullMQ infrastructure initialized successfully');