// import { logger } from "../logger";
// import { realTimeMatcherService } from "../services/matcher.services";
// import { handleDriverLocation } from "./location.handler";
// import { handlePaymentCompleted } from "./payment.handler";
//
// export async function processKafkaMessage(payload: any, topic: string): Promise<any> {
//     try {
//         logger.info({ type: payload.type, topic }, '📥 Kafka message received');
//
//         if (topic.startsWith('driver.location.')) {
//             await handleDriverLocation(payload);
//             return null;
//         }
//
//         if (payload.type === 'payment.completed') {
//             return await handlePaymentCompleted(payload);
//         }
//
//         if (payload.type === 'driver.response') {
//             return await realTimeMatcherService.handleDriverResponse(payload);
//         }
//
//         if (payload.type === 'get.stats') {
//             const stats = realTimeMatcherService.getStats();
//             return { success: true, stats };
//         }
//
//         logger.warn({ type: payload.type }, '️Unknown message type');
//         return { success: false, error: 'Unknown message type' };
//
//     } catch (error: any) {
//         logger.error({ error: error.message }, ' Message processing failed');
//         return { success: false, error: error.message };
//     }
// }



// // src/handlers/mesage-processor.ts
// import { logger } from '../logger';
// import { jobOrchestratorService } from '../services/JobOrchestrator.Service'; // Updated import
//
// export async function processKafkaMessage(data: any): Promise<any> {
//     const requestId = `kafka_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
//
//     logger.info(`Kafka Message Received - Type: ${data.type}, RequestId: ${requestId}`);
//
//     try {
//         // Route the message to the orchestrator service
//         const result = await jobOrchestratorService.handleRPCRequest(data);
//
//         logger.info(`Kafka Message Processed - Type: ${data.type}, RequestId: ${requestId}, Success: ${result.success}`);
//         return result;
//
//     } catch (error: any) {
//         logger.error(`Kafka Message Failed - Type: ${data.type}, RequestId: ${requestId}, Error: ${error.message}`);
//         return {
//             success: false,
//             error: error.message,
//             requestId: requestId
//         };
//     }
// }

// handlers/mesage-processor.ts
import { logger } from '../logger';
import { handlePaymentCompleted } from './payment.handler';
import { jobOrchestratorService } from '../services/JobOrchestrator.Service';

export async function processKafkaMessage(data: any, topic?: string): Promise<any> {
    const requestId = `kafka_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    logger.info(`Kafka Message Received - Type: ${data.type}, RequestId: ${requestId}`);

    try {
        if (data.type === 'payment.completed') {
            return await handlePaymentCompleted(data); 
        }

        // Other RPC events
        const result = await jobOrchestratorService.handleRPCRequest(data);
        logger.info(`Kafka Message Processed - Type: ${data.type}, RequestId: ${requestId}, Success: ${result.success}`);
        return result;

    } catch (error: any) {
        logger.error(`Kafka Message Failed - Type: ${data.type}, RequestId: ${requestId}, Error: ${error.message}`);
        return { success: false, error: error.message, requestId };
    }
}
