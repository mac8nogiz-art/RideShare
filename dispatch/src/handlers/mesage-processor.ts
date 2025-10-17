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
