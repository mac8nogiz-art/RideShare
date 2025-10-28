import { Elysia } from 'elysia';
import { connectKafka, consumer } from './infrastructure/kafka';
import { kafkaRPC } from './commonkafkaintegration/kafkarpc';
import { redis } from './infrastructure/redis';
import { logger } from './logger';
import { jobOrchestratorService } from './services/JobOrchestrator.Service';
import { processKafkaMessage } from './handlers/mesage-processor';

const orchestrator = jobOrchestratorService;

async function start() {
    logger.info("🚀 Starting Dispatch Service...");

    // 🧭 Start Job Orchestrator
    await orchestrator.start();

    // 🔌 Connect Kafka
    await connectKafka();

    // 🎧 Subscribe to main topic
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

// 🧠 HTTP Interface for Observability / RPC
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
                logger.info(`📨 Received event from topic: ${topic}`);
                logger.debug(`Message Payload: ${JSON.stringify(message, null, 2)}`);

                return {
                    status: 'processed',
                    timestamp: Date.now(),
                };
            },
        })
    )
    .get('/', () => '🚀 Dispatch Service Running')
    .get('/health', () => ({ status: 'ok' }))
    .get('/stats', async () => {
        const stats = orchestrator.getStats();
        return {
            success: true,
            ...stats,
        };
    })
    .listen(4005);

logger.info(`🌐 HTTP server running at http://localhost:4005`);

// ----------------- Global Error Handlers -----------------
process.on('unhandledRejection', (reason: any) => {
    logger.error(`Unhandled Rejection: ${reason?.message || reason}`);
});

process.on('uncaughtException', (err: any) => {
    logger.error(`Uncaught Exception: ${err.message}`, err.stack);
});

start().catch((err) => {
    logger.error(`❌ Fatal error during startup: ${err.message}`);
    process.exit(1);
});
