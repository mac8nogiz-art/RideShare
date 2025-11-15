// index.ts - Enhanced with bucket expiry handling

import { Elysia } from 'elysia';
import { connectKafka, consumer } from './infrastructure/kafka';
import { connectRedisSubscriber, redisSubscriber } from './redisConnection/subscriber';
import { kafkaRPC } from './commonkafkaintegration/kafkarpc';
import { redis } from './infrastructure/redis';
import { logger } from './logger';
import { jobOrchestratorService } from './services/JobOrchestrator.Service';
import { initializeWorkers, closeAllWorkers, areWorkersRunning } from './infrastructure/bullmqworkers';
import { OfferManagementService } from './services/OfferManagement.Service';

const orchestrator = jobOrchestratorService;

async function start() {
    logger.info("Starting Dispatch Service...");

    await orchestrator.start();

    await connectRedisSubscriber();
    setupRedisEventHandlers();

    const offerService = new OfferManagementService();
    initializeWorkers(offerService);

    await connectKafka();
    await consumer.subscribe({ topic: "newJob.request" });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logger.info("========== Kafka Message RECEIVED ==========");
            logger.info(`Topic: ${topic}, Partition: ${partition}`);
            try {
                const data = JSON.parse(message.value?.toString() || '{}');
                await orchestrator.handleRPCRequest(data);
            } catch (err: any) {
                logger.error(` Kafka message handling failed: ${err.message}`);
            }
        },
    });

    logger.info(" Kafka consumer started successfully");
}

function setupRedisEventHandlers() {
    logger.info(" Setting up Redis event handlers...");

    redisSubscriber.addExpiryHandler(async (expiredKey: string) => {
        try {
            logger.debug(`Redis key expired: ${expiredKey}`);

            if (expiredKey.includes(':matched_bucket')) {
                await handleMatchedBucketExpiry(expiredKey);
                return;
            }

            if (expiredKey.startsWith('offer:')) {
                await handleOfferExpiry(expiredKey);
                return;
            }

            logger.debug(` Unhandled expiry event for key: ${expiredKey}`);

        } catch (err: any) {
            logger.error(` Error handling expired key ${expiredKey}: ${err.message}`);
        }
    });

    logger.info(' Redis event handlers configured');
}

async function handleMatchedBucketExpiry(expiredKey: string): Promise<void> {
    try {

        const match = expiredKey.match(/job:([^:]+):matched_bucket/);

        if (!match) {
            logger.warn(`Invalid matched bucket key format: ${expiredKey}`);
            return;
        }

        const jobId = match[1];

        logger.info(`Matched bucket expired for Job: ${jobId}`);


        await orchestrator.handleMatchedBucketExpiry(jobId);

    } catch (error: any) {
        logger.error(` Error handling matched bucket expiry: ${error.message}`);
    }
}

async function handleOfferExpiry(expiredKey: string): Promise<void> {
    try {
        const parts = expiredKey.split(':');

        if (parts.length !== 3) {
            logger.warn(`Invalid offer key format: ${expiredKey}`);
            return;
        }

        const [, jobId, driverId] = parts;
        logger.info(`Offer expired - Job: ${jobId}, Driver: ${driverId}`);

    } catch (error: any) {
        logger.error(`Error handling offer expiry: ${error.message}`);
    }
}

const app = new Elysia()
    .use(
        kafkaRPC({
            serviceName: 'dispatch-service',
            brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
            requestTopic: (data) =>
                `rpc.requests.${data.jobId || 'global'}.dispatch-service`,
            responseTopic: (data) =>
                `rpc.responses.${data.jobId || 'global'}.dispatch-service`,
            timeout: 5000,
            batchSize: 100,
            topicDiscoveryInterval: 30000,
            onMessage: async (message: any, topic: string) => {
                logger.info(`received event from topic: ${topic}`);
                return {
                    status: 'processed',
                    timestamp: Date.now(),
                };
            },
        })
    )
    .get('/', () => ({
        service: 'Dispatch Service',
        status: 'running',
        version: '2.0.0'
    }))
    .get('/health', () => ({
        status: 'ok',
        redis: redisSubscriber.isConnected(),
        orchestrator: orchestrator.isReady(),
        workers: areWorkersRunning(),
        timestamp: new Date().toISOString()
    }))
    .get('/metrics', async () => {
        try {
            const flowKeys = await redis.keys('job:*:matched_flow_active');
            const offerKeys = await redis.keys('offer:*');

            return {
                activeFlows: flowKeys.length,
                activeOffers: offerKeys.length,
                timestamp: new Date().toISOString()
            };
        } catch (error: any) {
            return {
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    })
    .listen(4005);

logger.info(`HTTP server running at http://localhost:4005`);

process.on('unhandledRejection', (reason: any) => {
    logger.error(` Unhandled Rejection: ${reason?.message || reason}`);
});

process.on('uncaughtException', (err: any) => {
    logger.error(` Uncaught Exception: ${err.message}`, err.stack);
});

process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, shutting down gracefully...');
    await closeAllWorkers(); // Add this
    orchestrator.stop();
    await redisSubscriber.disconnect();
    await consumer.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info('SIGINT received, shutting down gracefully...');
    await closeAllWorkers(); // Add this
    orchestrator.stop();
    await redisSubscriber.disconnect();
    await consumer.disconnect();
    process.exit(0);
});

start().catch((err) => {
    logger.error(` Fatal error during startup: ${err.message}`);
    process.exit(1);
});