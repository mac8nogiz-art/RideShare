import { logger } from "../logger";
import { jobProcessingService } from "../services/JobProcessingService";

export async function handlePaymentCompleted(data: any) {
    try {
        await jobProcessingService.addJob({
            id: data.jobId,
            customerId: data.customerId,
            pickupLat: data.pickupLat,
            pickupLng: data.pickupLng,
            fare: data.fare,
            vehicleType: data.vehicleType,
            timestamp: Date.now()
        });

        logger.info( 'Payment processed, driver search initiated');

        return {
            success: true,
            message: 'Driver search initiated',
            jobId: data.jobId,
            timestamp: Date.now()
        };
    } catch (error: any) {

        logger.error(`payment processing failed: ${error.message || error}`);
        return { success: false, error: error.message };
    }
}
