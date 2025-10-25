import { Elysia } from 'elysia';
import { kafkaRPC } from './commonkafkaintegration/kafkarpc';
import { redis } from "./infrastructure/redis";
import { logger } from "./logger";
import { jobOrchestratorService } from "./services/JobOrchestrator.Service"; // Updated import
import { processKafkaMessage } from "./handlers/mesage-processor";


const app = new Elysia()

    .use(
        kafkaRPC({
            serviceName: 'dispatch-service',
            brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
            requestTopic: (data) => `${data.targetService || 'dispatch-service'}.request`,
            responseTopic: (data) => `${data.targetService || 'dispatch-service'}.response`,
            timeout: 5000,
            batchSize: 100,

            onMessage: async (message: any, topic: string) => {
                    console.log(`Received event from topic: ${topic}`);
                    console.log(`Message:`, JSON.stringify(message, null, 2));
                    return {
                        status: 'processed',
                        timestamp: Date.now()
                    };
                }
            })
        )

        .get('/', () => 'Dispatch Service Running')
        .get('/health', () => ({ status: 'ok' }))
        .listen(process.env.PORT || 4005)



    .get("/stats", async ({kafkaRPC}) => {
        const stats = jobOrchestratorService.getStats(); // Updated service reference
        const kafkaStats = kafkaRPC.getCacheStats();
        return {...stats, kafka: kafkaStats};
    });

async function start() {
    try {
        logger.info(' Starting Dispatch Service...');
        logger.info('═'.repeat(50));


        logger.info('Testing Redis connection...');
        await redis.ping();
        logger.info(' Redis connected');


        logger.info('Starting job orchestrator...');
        await jobOrchestratorService.start();
        logger.info('Orchestrator started');


        const preferredPort = Number(process.env.PORT) || 4005;
        let port = preferredPort;
        let serverStarted = false;

        for (let attempt = 0; attempt < 5 && !serverStarted; attempt++) {
            try {
                logger.info(`starting HTTP server on port ${port}...`);
                await app.listen({port, hostname: "0.0.0.0"});
                serverStarted = true;
            } catch (error: any) {
                if (error.message.includes('in use')) {
                    logger.warn(`  Port ${port} in use, trying ${port + 1}`);
                    port++;
                } else {
                    throw error;
                }
            }
        }

        if (!serverStarted) {
            throw new Error(`No available port starting from ${preferredPort}`);
        }

        logger.info(` HTTP Server: http://localhost:${port}`);
        logger.info(` Stats: http://localhost:${port}/stats`);
        logger.info(`Health: http://localhost:${port}/health`);
        logger.info('');

    } catch (error: any) {
        logger.error({error: error.message}, 'Startup failed');
        jobOrchestratorService.stop();
        process.exit(1);
    }
}


process.on('SIGTERM', async () => {
    logger.info('SIGTERM - shutting down...');
    jobOrchestratorService.stop();
    await redis.quit();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info('  SIGINT - shutting down...');
    jobOrchestratorService.stop();
    await redis.quit();
    process.exit(0);
});

start();

export default app;




