// src/scripts/driver-response-simulator.ts
import { Kafka } from 'kafkajs';
import Redis from 'ioredis';

// Initialize Kafka and Redis
const kafka = new Kafka({
    clientId: 'driver-response-simulator',
    brokers: ['localhost:9092'],
    retry: {
        initialRetryTime: 100,
        retries: 5
    }
});

const redis = new Redis('redis://localhost:6379');
const producer = kafka.producer();

export class DriverResponseSimulator {
    private isRunning = false;
    private interval: NodeJS.Timeout | null = null;

    async start(): Promise<void> {
        if (this.isRunning) return;

        console.log('🚗 Starting Driver Response Simulator...');

        try {
            await producer.connect();
            this.isRunning = true;

            // Check for offers every 2 seconds
            this.interval = setInterval(async () => {
                await this.processOffers();
            }, 2000);

            // Initial processing
            await this.processOffers();

            console.log('✅ Driver Response Simulator started successfully');
        } catch (error) {
            console.error('❌ Failed to start simulator:', error);
            throw error;
        }
    }

    async stop(): Promise<void> {
        if (!this.isRunning) return;

        console.log('🛑 Stopping Driver Response Simulator...');
        this.isRunning = false;

        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }

        await producer.disconnect().catch(() => {});
        console.log('✅ Driver Response Simulator stopped');
    }

    private async processOffers(): Promise<void> {
        try {
            // Get all active offers from Redis
            const offerKeys = await redis.keys('offer:*');

            if (offerKeys.length === 0) {
                console.log('📭 No active offers found');
                return;
            }

            console.log(`📋 Found ${offerKeys.length} active offers`);

            for (const offerKey of offerKeys) {
                await this.processSingleOffer(offerKey);

                // Small delay between processing offers
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        } catch (error) {
            console.error('❌ Error processing offers:', error);
        }
    }

    private async processSingleOffer(offerKey: string): Promise<void> {
        try {
            // Parse jobId and driverId from offer key (format: "offer:jobId:driverId")
            const parts = offerKey.split(':');
            if (parts.length !== 3) {
                console.warn(`⚠️ Invalid offer key format: ${offerKey}`);
                return;
            }

            const jobId = parts[1];
            const driverId = parts[2];

            // Get offer details from Redis
            const offerData = await redis.get(offerKey);
            if (!offerData) {
                console.log(`📭 Offer expired or not found: ${offerKey}`);
                return;
            }

            const offer = JSON.parse(offerData);

            // Skip if offer is expired
            if (offer.expiresAt && Date.now() > offer.expiresAt) {
                console.log(`⏰ Offer expired: ${offerKey}`);
                await redis.del(offerKey);
                return;
            }

            // Simulate driver decision (80% acceptance rate)
            const shouldAccept = Math.random() < 0.8;

            if (shouldAccept) {
                await this.sendDriverAcceptance(driverId, jobId, offer);
                await redis.del(offerKey); // Remove accepted offer
            } else {
                await this.sendDriverRejection(driverId, jobId, offer);
                // Don't delete rejected offers immediately - they might be sent to other drivers
            }

        } catch (error) {
            console.error(`❌ Error processing offer ${offerKey}:`, error);
        }
    }

    private async sendDriverAcceptance(driverId: string, jobId: string, offer: any): Promise<void> {
        try {
            const acceptanceMessage = {
                type: 'driver.response',
                driverId: driverId,
                jobId: jobId,
                action: 'accept',
                customerId: offer.customerId,
                pickupLat: offer.pickupLat,
                pickupLng: offer.pickupLng,
                fare: offer.fare,
                timestamp: Date.now()
            };

            await producer.send({
                topic: 'dispatch-service.request',
                messages: [
                    {
                        key: driverId,
                        value: JSON.stringify(acceptanceMessage)
                    }
                ]
            });

            console.log(`✅ DRIVER ACCEPTED: ${driverId} accepted job ${jobId}`);

            // Also update driver status to busy in Redis
            await redis.hset(`driver:${driverId}:profile`, 'isBusy', 'true');

        } catch (error) {
            console.error(`❌ Failed to send acceptance from ${driverId}:`, error);
        }
    }

    private async sendDriverRejection(driverId: string, jobId: string, offer: any): Promise<void> {
        try {
            const rejectionMessage = {
                type: 'driver.response',
                driverId: driverId,
                jobId: jobId,
                action: 'reject',
                reason: 'Driver unavailable',
                customerId: offer.customerId,
                timestamp: Date.now()
            };

            await producer.send({
                topic: 'dispatch-service.request',
                messages: [
                    {
                        key: driverId,
                        value: JSON.stringify(rejectionMessage)
                    }
                ]
            });

            console.log(`❌ DRIVER REJECTED: ${driverId} rejected job ${jobId} (Driver unavailable)`);

        } catch (error) {
            console.error(`❌ Failed to send rejection from ${driverId}:`, error);
        }
    }

    // Method to manually trigger a specific driver response
    async triggerManualResponse(driverId: string, jobId: string, action: 'accept' | 'reject', reason?: string): Promise<void> {
        try {
            const message = {
                type: 'driver.response',
                driverId: driverId,
                jobId: jobId,
                action: action,
                reason: reason,
                timestamp: Date.now()
            };

            await producer.send({
                topic: 'dispatch-service.request',
                messages: [
                    {
                        key: driverId,
                        value: JSON.stringify(message)
                    }
                ]
            });

            console.log(`📤 Manual response: ${driverId} ${action}ed job ${jobId}`);

        } catch (error) {
            console.error('❌ Failed to send manual response:', error);
        }
    }

    // Method to check current offers
    async checkOffers(): Promise<void> {
        const offerKeys = await redis.keys('offer:*');
        console.log(`\n📊 CURRENT OFFERS (${offerKeys.length}):`);

        for (const key of offerKeys) {
            const offerData = await redis.get(key);
            if (offerData) {
                const offer = JSON.parse(offerData);
                const timeLeft = offer.expiresAt ? Math.max(0, offer.expiresAt - Date.now()) : 0;
                console.log(`   ${key} - Expires in: ${Math.round(timeLeft/1000)}s`);
            }
        }
    }
}

// Singleton instance
export const driverResponseSimulator = new DriverResponseSimulator();

// Start the simulator if this file is run directly
if (require.main === module) {
    console.log('🚗 Starting Driver Response Simulator in standalone mode...');

    driverResponseSimulator.start().catch(console.error);

    // Graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n🛑 Received SIGINT, shutting down...');
        await driverResponseSimulator.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n🛑 Received SIGTERM, shutting down...');
        await driverResponseSimulator.stop();
        process.exit(0);
    });
}