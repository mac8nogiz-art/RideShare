import { logger } from "../logger";
import { jobOrchestratorService } from "../services/JobOrchestrator.Service";

export async function handlePaymentCompleted(data: any) {
    try {
        await jobOrchestratorService.addJob({
            id: data.jobId,
            customerId: data.customerId,
            pickupLat: data.pickupLat,
            pickupLng: data.pickupLng,
            fare: data.fare,
            vehicleType: data.vehicleType,
            timestamp: Date.now()
        });

        logger.info({ jobId: data.jobId }, 'Payment processed, driver search initiated');

        return {
            success: true,
            message: 'Driver search initiated',
            jobId: data.jobId,
            timestamp: Date.now()
        };
    } catch (error: any) {
        logger.error({ error: error.message }, 'Payment processing failed');
        return { success: false, error: error.message };
    }
}
