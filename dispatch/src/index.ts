// index.ts
import { Elysia } from 'elysia';
import { connectKafka, consumer } from './infrastructure/kafka';
import { connectRedisSubscriber, redisSubscriber } from './redisConnection/subscriber';
import { kafkaRPC } from './commonkafkaintegration/kafkarpc';
import { redis } from './infrastructure/redis';
import { logger } from './logger';
import { jobOrchestratorService } from './services/JobOrchestrator.Service';

const orchestrator = jobOrchestratorService;

async function start() {
    logger.info("Starting Dispatch Service...");
    await orchestrator.start();
    await connectRedisSubscriber();
    setupRedisEventHandlers();

    await connectKafka();
    await consumer.subscribe({ topic: "newJob.request" });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logger.info("========== onMessage TRIGGERED ==========");
            logger.info(`Topic: ${topic}, Partition: ${partition}`);
            try {
                await orchestrator.handleRPCRequest(JSON.parse(message.value?.toString() || '{}'));
            } catch (err: any) {
                logger.error(`Kafka message handling failed: ${err.message}`);
            }
        },
    });

    logger.info("✅ Kafka consumer started successfully");
}


function setupRedisEventHandlers() {
    // Subscribe to all offer response patterns globally
    // redisSubscriber.subscribePattern('offer.response.*', async (message, channel) => {
    //     try {
    //         logger.debug(`Offer response received on channel: ${channel}`, message);
    //
    //         const parts = channel.split('.');
    //         if (parts.length !== 4) {
    //             logger.warn(`Invalid channel format: ${channel}`);
    //             return;
    //         }
    //
    //         const [, , jobId, driverId] = parts;
    //
    //         if (message?.action === 'accept') {
    //             logger.info(` Driver ${driverId} accepted Job ${jobId} via Redis`);
    //
    //             // Publish to Redis for the waiting handler
    //             await redis.publish(channel, JSON.stringify(message));
    //
    //         } else if (message?.action === 'reject') {
    //             logger.info(` Driver ${driverId} rejected Job ${jobId} via Redis`);
    //
    //             // Handle rejection through orchestrator
    //             await orchestrator.handleRPCRequest({
    //                 type: 'newBooking.response',
    //                 driverId,
    //                 jobId,
    //                 action: 'reject',
    //                 reason: message.reason || 'Not specified'
    //             });
    //         }
    //     } catch (err: any) {
    //         logger.error(`Error handling offer response: ${err.message}`);
    //     }
    // });

    redisSubscriber.addExpiryHandler(async (expiredKey: string) => {
        try {
            logger.debug(`Redis key expired: ${expiredKey}`);

            // Handle offer expiry pattern: offer:jobId:driverId
            if (expiredKey.startsWith('offer:')) {
                const parts = expiredKey.split(':');
                if (parts.length === 3) {
                    const [, jobId, driverId] = parts;
                    logger.info(` Offer expired  - Job: ${jobId}, Driver: ${driverId}`);

                }
            }
        } catch (err: any) {
            logger.error(`Error handling expired key: ${err.message}`);
        }
    });

    logger.info('Redis global event handlers configured');
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
                logger.info(`Received event from topic: ${topic}`);
                return {
                    status: 'processed',
                    timestamp: Date.now(),
                };
            },
        })
    )
    .get('/', () => 'Dispatch Service Running')
    .get('/health', () => ({
        status: 'ok',
        redis: redisSubscriber.isConnected(),
        timestamp: new Date().toISOString()
    }))
    .listen(4005);

logger.info(`HTTP server running at http://localhost:4005`);


process.on('unhandledRejection', (reason: any) => {
    logger.error(`Unhandled Rejection: ${reason?.message || reason}`);
});

process.on('uncaughtException', (err: any) => {
    logger.error(`Uncaught Exception: ${err.message}`, err.stack);
});

process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, shutting down gracefully...');
    await redisSubscriber.disconnect();
    await consumer.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info('SIGINT received, shutting down gracefully...');
    await redisSubscriber.disconnect();
    await consumer.disconnect();
    process.exit(0);
});

start().catch((err) => {
    logger.error(`Fatal error during startup: ${err.message}`);
    process.exit(1);
});