import { Elysia } from 'elysia';
import { kafkaRPC } from './commonkafkaintegration/kafkarpc';
import { redis } from "./infrastructure/redis";
import { logger } from "./logger";
import { jobOrchestratorService } from "./services/JobOrchestrator.Service";
import { processKafkaMessage } from "./handlers/mesage-processor";

// ============================================
// KAFKA CONFIGURATION
// ============================================
const app = new Elysia()
    // ADD THIS: Initialize Kafka RPC
    .use(
        kafkaRPC({
            serviceName: 'Dispatch-service',
            brokers: [process.env.KAFKA_BROKER || '172.105.61.99:9093'],
            requestTopic: 'dispatch.request',
            responseTopic: 'dispatch.response',
            timeout: 10000,
            batchSize: 100,
            batchTimeout: 100,
            topicDiscoveryInterval: 30000,
            // Subscribe to topics you want to listen to
            subscribeToTopics: [
                'payment.events',
                'driver.location',
                'ride.request',
                'user.request',
                'user.response'
            ],
            // THIS IS CRITICAL: Handle incoming messages
            onMessage: async (message: any, topic: string) => {
                console.log(`\n🎯 Received event from topic: ${topic}`);
                console.log(`📦 Message:`, JSON.stringify(message, null, 2));

                // Handle different topics
                switch (topic) {
                    case 'payment.events':
                        console.log(`💰 Payment Event: ${message.paymentId} - $${message.amount}`);
                        // Your payment processing logic
                        break;

                    case 'driver.location':
                        console.log(`🚗 Driver Location: ${message.driverId} at (${message.lat}, ${message.lng})`);
                        // Your driver tracking logic
                        break;

                    case 'ride.request':
                        console.log(`🚕 Ride Request: ${message.requestId} from ${message.userId}`);
                        // Your ride matching logic
                        break;

                    case 'user.request':
                        console.log(`👤 User Request: ${message.requestId} - ${message.action}`);
                        // Your user request handling
                        break;

                    case 'user.response':
                        console.log(`✅ User Response: ${message.requestId}`);
                        // Your response handling
                        break;

                    default:
                        console.log(`⚠️ Unhandled topic: ${topic}`);
                }

                // Return response if needed
                return {
                    status: 'processed',
                    timestamp: Date.now()
                };
            }
        })
    )
    // Your other routes
    .get('/', () => 'Dispatch Service Running')
    .get('/health', () => ({ status: 'ok' }))
    .listen(process.env.PORT || 4005);

console.log(`🚀 Dispatch service running on port ${app.server?.port}`);

export default app;