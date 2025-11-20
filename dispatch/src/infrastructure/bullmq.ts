import { Queue, QueueEvents, Worker, ConnectionOptions, Job } from 'bullmq';
import { logger } from '../logger';
import { redis } from './redis';


export const QUEUE_NAMES = {
    MATCHED_BUCKET_EXPIRY: 'matched-bucket-expiry',
    OFFER_EXPIRY: 'offer-expiry',
    NEXT_BUCKET_TRIGGER: 'next-bucket-trigger',
    DRIVER_QUEUE_PROCESSOR: 'driver-queue-processor',
    MATCHED_DRIVER_TRIGGER: 'matched-driver-trigger',
} as const;


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

export interface DriverQueueJob {
    jobId: string;
    driverId: string;
    queuePosition: number;
    jobData: any;
    timestamp: string;
}

export interface MatchedDriverTriggerJob {
    jobId: string;
    jobData: any;
    reason: 'queue_exhausted' | 'no_drivers_found' | 'fallback';
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
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
};

const queueOptions = {
    connection,
    prefix: 'bullmq',
};

export const matchedBucketExpiryQueue = new Queue<MatchedBucketExpiryJob>(
    QUEUE_NAMES.MATCHED_BUCKET_EXPIRY,
    {
        ...queueOptions,
        defaultJobOptions: {
            removeOnComplete: {
                count: 100,
                age: 3600,
            },
            removeOnFail: {
                count: 500,
                age: 7200,
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
                age: 1800,
            },
            removeOnFail: {
                count: 500,
                age: 3600,
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

export const driverQueueProcessorQueue = new Queue<DriverQueueJob>(
    QUEUE_NAMES.DRIVER_QUEUE_PROCESSOR,
    {
        ...queueOptions,
        defaultJobOptions: {
            removeOnComplete: {
                count: 500,
                age: 1800,
            },
            removeOnFail: {
                count: 300,
                age: 3600,
            },
            attempts: 2,
            backoff: {
                type: 'exponential',
                delay: 1000,
            },
        },
    }
);

export const matchedDriverTriggerQueue = new Queue<MatchedDriverTriggerJob>(
    QUEUE_NAMES.MATCHED_DRIVER_TRIGGER,
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
                delay: 2000,
            },
        },
    }
);

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

export const driverQueueProcessorEvents = new QueueEvents(
    QUEUE_NAMES.DRIVER_QUEUE_PROCESSOR,
    queueOptions
);

export const matchedDriverTriggerEvents = new QueueEvents(
    QUEUE_NAMES.MATCHED_DRIVER_TRIGGER,
    queueOptions
);


// Matched Driver Trigger Worker
export const matchedDriverTriggerWorker = new Worker<MatchedDriverTriggerJob>(
    QUEUE_NAMES.MATCHED_DRIVER_TRIGGER,
    async (job: Job<MatchedDriverTriggerJob>) => {
        const { jobId, jobData, reason } = job.data;

        try {
            logger.info(`[BullMQ] Processing matched driver trigger - Job: ${jobId}, Reason: ${reason}`);

            if (matchedDriverTriggerCallback) {
                await matchedDriverTriggerCallback(jobId, jobData, reason);
                logger.info(`[BullMQ] Matched driver trigger processed successfully - Job: ${jobId}`);
                return { success: true, jobId, reason };
            } else {
                logger.warn(`[BullMQ] No matched driver trigger callback registered`);
                return { success: false, error: 'No callback registered' };
            }
        } catch (error: any) {
            logger.error(`[BullMQ] Matched driver trigger failed - Job: ${jobId}, Error: ${error.message}`);
            throw error;
        }
    },
    {
        connection,
        concurrency: 5,
        removeOnComplete: { count: 50 },
        removeOnFail: { count: 100 },
    }
);

// Offer Expiry Worker - Sequential processing
export const offerExpiryWorker = new Worker<OfferExpiryJob>(
    QUEUE_NAMES.OFFER_EXPIRY,
    async (job: Job<OfferExpiryJob>) => {
        const { jobId, driverId } = job.data;

        try {
            logger.info(`[BullMQ] Processing offer expiry - Job: ${jobId}, Driver: ${driverId}`);

            if (offerExpiryCallback) {
                await offerExpiryCallback(jobId, driverId, 0);
                logger.info(`[BullMQ] Offer expiry callback executed - Job: ${jobId}, Driver: ${driverId}`);
                return { success: true, jobId, driverId };
            } else {
                logger.warn(`[BullMQ] No offer expiry callback registered`);
                return { success: false, error: 'No callback registered' };
            }
        } catch (error: any) {
            logger.error(`[BullMQ] Offer expiry failed - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
            throw error;
        }
    },
    {
        connection,
        concurrency: 1, // CRITICAL: Process one offer expiry at a time for sequential flow
        removeOnComplete: { count: 100 },
        removeOnFail: { count: 100 },
    }
);

// Callback storage
let offerExpiryCallback: ((jobId: string, driverId: string, remainingDrivers: number) => Promise<void>) | null = null;
let matchedBucketExpiryCallback: ((jobId: string, bucketIndex: number) => Promise<void>) | null = null;
let matchedDriverTriggerCallback: ((jobId: string, jobData: any, reason: string) => Promise<void>) | null = null;

/**
 * Register callback for offer expiry events
 */
export function registerOfferExpiryCallback(
    callback: (jobId: string, driverId: string, remainingDrivers: number) => Promise<void>
): void {
    offerExpiryCallback = callback;
    logger.info('Offer expiry callback registered');
}

/**
 * Register callback for matched bucket expiry events
 */
export function registerMatchedBucketExpiryCallback(
    callback: (jobId: string, bucketIndex: number) => Promise<void>
): void {
    matchedBucketExpiryCallback = callback;
    logger.info('Matched bucket expiry callback registered');
}

/**
 * Register callback for matched driver trigger events
 */
export function registerMatchedDriverTriggerCallback(
    callback: (jobId: string, jobData: any, reason: string) => Promise<void>
): void {
    matchedDriverTriggerCallback = callback;
    logger.info('Matched driver trigger callback registered');
}

/**
 * Trigger matched driver flow via BullMQ
 */
export async function triggerMatchedDriverFlow(
    jobId: string,
    jobData: any,
    reason: 'queue_exhausted' | 'no_drivers_found' | 'fallback'
): Promise<void> {
    try {
        const jobKey = `matched-driver-trigger-${jobId}`;

        await matchedDriverTriggerQueue.add(
            'trigger-matched-drivers',
            {
                jobId,
                jobData,
                reason,
                timestamp: new Date().toISOString(),
            },
            {
                jobId: jobKey,
                removeOnComplete: true,
                removeOnFail: false,
                priority: reason === 'queue_exhausted' ? 1 : 2,
            }
        );

        logger.info(`[BullMQ] Triggered matched driver flow: Job ${jobId}, Reason: ${reason}`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to trigger matched driver flow: ${error.message}`);
        throw error;
    }
}

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

        logger.info(`[BullMQ] Scheduled bucket expiry: Job ${jobId}, Bucket ${bucketIndex}, Delay ${delaySeconds}s`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to schedule bucket expiry: ${error.message}`);
        throw error;
    }
}


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

        logger.debug(`[BullMQ] Scheduled offer expiry: Job ${jobId}, Driver ${driverId}, Delay ${delaySeconds}s`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to schedule offer expiry: ${error.message}`);
        throw error;
    }
}

export async function processDriverQueue(
    jobId: string,
    driverIds: string[],
    jobData: any
): Promise<void> {
    try {
        const jobs = driverIds.map((driverId, index) => ({
            name: 'process-driver-offer',
            data: {
                jobId,
                driverId,
                queuePosition: index,
                jobData,
                timestamp: new Date().toISOString(),
            },
            opts: {
                jobId: `driver-queue-${jobId}-${driverId}`,
                priority: index,
                removeOnComplete: true,
                removeOnFail: false,
            },
        }));

        await driverQueueProcessorQueue.addBulk(jobs);

        logger.info(`[BullMQ] Processed driver queue: Job ${jobId}, ${driverIds.length} drivers`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to process driver queue: ${error.message}`);
        throw error;
    }
}

export async function cancelBucketExpiry(jobId: string, bucketIndex: number): Promise<void> {
    try {
        const jobKey = `bucket-expiry-${jobId}-${bucketIndex}`;
        const job = await matchedBucketExpiryQueue.getJob(jobKey);

        if (job) {
            await job.remove();
            logger.info(`[BullMQ] Cancelled bucket expiry: Job ${jobId}, Bucket ${bucketIndex}`);
        }
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to cancel bucket expiry: ${error.message}`);
    }
}


export async function cancelOfferExpiry(jobId: string, driverId: string): Promise<void> {
    try {
        const jobKey = `offer-expiry-${jobId}-${driverId}`;
        const job = await offerExpiryQueue.getJob(jobKey);

        if (job) {
            await job.remove();
            logger.debug(`[BullMQ] Cancelled offer expiry: Job ${jobId}, Driver ${driverId}`);
        }
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to cancel offer expiry: ${error.message}`);
    }
}

export async function cancelAllOffersForJob(jobId: string): Promise<void> {
    try {
        const jobs = await offerExpiryQueue.getJobs(['delayed', 'waiting']);
        const jobsToRemove = jobs.filter(j => j.data.jobId === jobId);

        await Promise.all(jobsToRemove.map(j => j.remove()));

        logger.info(`[BullMQ] Cancelled ${jobsToRemove.length} offer expiries for Job ${jobId}`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to cancel offers for job: ${error.message}`);
    }
}


export async function cancelAllBucketsForJob(jobId: string): Promise<void> {
    try {
        const jobs = await matchedBucketExpiryQueue.getJobs(['delayed', 'waiting']);
        const jobsToRemove = jobs.filter(j => j.data.jobId === jobId);

        await Promise.all(jobsToRemove.map(j => j.remove()));

        logger.info(`[BullMQ] Cancelled ${jobsToRemove.length} bucket expiries for Job ${jobId}`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to cancel buckets for job: ${error.message}`);
    }
}

export async function cancelAllDriversForJob(jobId: string): Promise<void> {
    try {
        const jobs = await driverQueueProcessorQueue.getJobs(['waiting', 'delayed', 'active']);
        const jobsToRemove = jobs.filter(j => j.data.jobId === jobId);

        await Promise.all(jobsToRemove.map(j => j.remove()));

        logger.info(`[BullMQ] Cancelled ${jobsToRemove.length} drivers from queue for Job ${jobId}`);
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to cancel driver queue for job: ${error.message}`);
    }
}



export async function pauseAllQueues(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryQueue.pause(),
            offerExpiryQueue.pause(),
            nextBucketTriggerQueue.pause(),
            driverQueueProcessorQueue.pause(),
        ]);
        logger.info('[BullMQ] All queues paused');
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to pause queues: ${error.message}`);
    }
}

export async function resumeAllQueues(): Promise<void> {
    try {
        await Promise.all([
            matchedBucketExpiryQueue.resume(),
            offerExpiryQueue.resume(),
            nextBucketTriggerQueue.resume(),
            driverQueueProcessorQueue.resume(),
        ]);
        logger.info('[BullMQ] All queues resumed');
    } catch (error: any) {
        logger.error(`[BullMQ] Failed to resume queues: ${error.message}`);
    }
}

export async function closeBullMQConnections(): Promise<void> {
    try {
        logger.info('[BullMQ] Closing connections...');

        await Promise.all([
            offerExpiryWorker.close(),
            matchedDriverTriggerWorker.close(),
            matchedBucketExpiryQueue.close(),
            offerExpiryQueue.close(),
            nextBucketTriggerQueue.close(),
            driverQueueProcessorQueue.close(),
            matchedDriverTriggerQueue.close(),
            matchedBucketExpiryEvents.close(),
            offerExpiryEvents.close(),
            nextBucketTriggerEvents.close(),
            driverQueueProcessorEvents.close(),
            matchedDriverTriggerEvents.close(),
        ]);

        logger.info('[BullMQ] Connections closed successfully');
    } catch (error: any) {
        logger.error(`[BullMQ] Error closing connections: ${error.message}`);
        throw error;
    }
}

// ===========================
// EVENT LISTENERS
// ===========================

matchedBucketExpiryEvents.on('completed', ({ jobId }) => {
    logger.debug(`[BullMQ] Matched bucket expiry completed: ${jobId}`);
});

matchedBucketExpiryEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`[BullMQ] Matched bucket expiry failed: ${jobId} - ${failedReason}`);
});

offerExpiryEvents.on('completed', ({ jobId }) => {
    logger.debug(`[BullMQ] Offer expiry completed: ${jobId}`);
});

offerExpiryEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`[BullMQ] Offer expiry failed: ${jobId} - ${failedReason}`);
});

nextBucketTriggerEvents.on('completed', ({ jobId }) => {
    logger.debug(`[BullMQ] Next bucket trigger completed: ${jobId}`);
});

nextBucketTriggerEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`[BullMQ] Next bucket trigger failed: ${jobId} - ${failedReason}`);
});

driverQueueProcessorEvents.on('completed', ({ jobId }) => {
    logger.debug(`[BullMQ] Driver queue completed: ${jobId}`);
});

driverQueueProcessorEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`[BullMQ] Driver queue failed: ${jobId} - ${failedReason}`);
});

driverQueueProcessorEvents.on('active', ({ jobId }) => {
    logger.debug(`[BullMQ] Driver queue active: ${jobId}`);
});

matchedDriverTriggerEvents.on('completed', ({ jobId }) => {
    logger.debug(`[BullMQ] Matched driver trigger completed: ${jobId}`);
});

matchedDriverTriggerEvents.on('failed', ({ jobId, failedReason }) => {
    logger.error(`[BullMQ] Matched driver trigger failed: ${jobId} - ${failedReason}`);
});

matchedDriverTriggerEvents.on('active', ({ jobId }) => {
    logger.debug(`[BullMQ] Matched driver trigger active: ${jobId}`);
});

logger.info('[BullMQ] Infrastructure initialized successfully');