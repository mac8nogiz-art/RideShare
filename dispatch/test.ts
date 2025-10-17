import { Kafka } from 'kafkajs';
import Redis from 'ioredis';

const kafka = new Kafka({
    clientId: 'master-test-client',
    brokers: ['localhost:9092'],
});

const redis = new Redis('redis://localhost:6379');
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `master-test-group-${Date.now()}` });

// Test Configuration
const CONFIG = {
    TOTAL_JOBS: 10,
    BATCH_DELAY_MS: 300,
    PROCESSING_TIMEOUT_MS: 15000,
    CLEANUP_BEFORE_TEST: true,
    DEBUG_MODE: true
};

// Test Metrics
const metrics = {
    paymentsSent: 0,
    kafkaAssignmentEvents: 0,
    redisAssignments: 0,
    startTime: 0,
    endTime: 0
};

class TestMaster {
    private testJobs = new Set<string>();

    async comprehensiveCleanup() {
        console.log('🧹 COMPREHENSIVE CLEANUP INITIATED...');
        console.log('='.repeat(50));

        const patterns = [
            // Jobs and offers
            'job:test_job_*',
            'offer:test_job_*',
            'job:test_job_*:pending_drivers',
            'job:test_job_*:rejected_drivers',
            'job:test_job_*:matched_drivers',

            // Driver offers and busy status
            'driver:*:offers',

            // Test customer data
            'customer:test_*:favorites',

            // Kafka consumer groups (if any)
            'kafka:*'
        ];

        let totalCleaned = 0;

        for (const pattern of patterns) {
            try {
                const keys = await redis.keys(pattern);
                if (keys.length > 0) {
                    const deleted = await redis.del(...keys);
                    totalCleaned += deleted;
                    console.log(`   ✅ Cleaned ${deleted} keys: ${pattern}`);
                }
            } catch (error) {
                console.log(`   ⚠️  Cleanup failed for ${pattern}: ${error}`);
            }
        }

        // Reset driver busy status for test drivers
        await this.resetTestDrivers();

        console.log(`🎯 TOTAL CLEANED: ${totalCleaned} keys`);
        console.log('✅ COMPREHENSIVE CLEANUP COMPLETE\n');
    }

    private async resetTestDrivers() {
        console.log('🔄 Resetting test driver status...');

        const testDriverPatterns = ['driver_*:profile'];
        let resetCount = 0;

        for (const pattern of testDriverPatterns) {
            try {
                const keys = await redis.keys(pattern);
                for (const key of keys) {
                    await redis.hset(key, 'isBusy', 'false');
                    resetCount++;
                }
            } catch (error) {
                console.log(`   ⚠️  Driver reset failed for ${pattern}`);
            }
        }

        console.log(`   ✅ Reset ${resetCount} driver profiles`);
    }

    async sendPaymentEvent(jobId: string, customerId: string) {
        const event = {
            type: 'payment.completed',
            jobId,
            customerId,
            pickupLat: 28.6139 + (Math.random() - 0.5) * 0.01, // Small variation
            pickupLng: 77.2090 + (Math.random() - 0.5) * 0.01,
            fare: 200 + Math.floor(Math.random() * 100),
            vehicleType: 'sedan',
            timestamp: Date.now()
        };

        await producer.send({
            topic: 'dispatch-service.request',
            messages: [{
                key: jobId,
                value: JSON.stringify(event)
            }]
        });

        this.testJobs.add(jobId);
        metrics.paymentsSent++;
        console.log(`📤 Sent payment: ${jobId}`);
    }

    async monitorAssignments() {
        await consumer.subscribe({
            topics: ['driver-assignments'],
            fromBeginning: false
        });

        await consumer.run({
            eachMessage: async ({ message, topic }) => {
                try {
                    const payload = JSON.parse(message.value?.toString() || '{}');

                    if (topic === 'driver-assignments' && payload.type === 'driver.assigned') {
                        metrics.kafkaAssignmentEvents++;
                        console.log(`✅ Kafka Assignment: ${payload.jobId} → ${payload.driverId}`);
                    }
                } catch (error) {
                    // Ignore parse errors
                }
            }
        });
    }

    async checkRedisAssignments(): Promise<number> {
        console.log('\n🔍 CHECKING REDIS ASSIGNMENTS...');

        const assignedJobs: { jobId: string; driverId: string; status: string; assignedAt: string }[] = [];
        const jobKeys = await redis.keys('job:test_job_*');

        for (const key of jobKeys) {
            try {
                const type = await redis.type(key);
                if (type !== 'hash') continue;

                const jobData = await redis.hgetall(key);
                if (jobData.assignedDriver && jobData.status === 'accepted') {
                    assignedJobs.push({
                        jobId: key.replace('job:', ''),
                        driverId: jobData.assignedDriver,
                        status: jobData.status,
                        assignedAt: jobData.assignedAt || 'unknown'
                    });
                }
            } catch (error) {
                console.log(`   ⚠️  Error reading job ${key}: ${error}`);
            }
        }

        metrics.redisAssignments = assignedJobs.length;

        console.log(`✅ Redis Assignments: ${assignedJobs.length}/${metrics.paymentsSent}`);

        if (assignedJobs.length > 0) {
            console.log('\n📋 ASSIGNMENT DETAILS:');
            assignedJobs.forEach(job => {
                console.log(`   🎯 ${job.jobId} → ${job.driverId} (${new Date(job.assignedAt).toLocaleTimeString()})`);
            });
        }

        return assignedJobs.length;
    }

    async debugSystemState() {
        if (!CONFIG.DEBUG_MODE) return;

        console.log('\n🐛 SYSTEM DEBUG INFORMATION:');
        console.log('-'.repeat(40));

        // Check active offers
        const offerKeys = await redis.keys('offer:*');
        console.log(`   Active offers: ${offerKeys.length}`);

        // Check driver states
        const driverLocationKeys = await redis.keys('driver:*:location');
        console.log(`   Active drivers: ${driverLocationKeys.length}`);

        // Check busy drivers
        let busyDrivers = 0;
        let availableDrivers = 0;

        for (const key of driverLocationKeys) {
            const driverId = key.split(':')[1];
            const profileKey = `driver:${driverId}:profile`;

            try {
                const profile = await redis.hgetall(profileKey);
                if (profile.isBusy === 'true') {
                    busyDrivers++;
                } else {
                    availableDrivers++;
                }
            } catch (error) {
                // Profile might not exist
            }
        }

        console.log(`   Busy drivers: ${busyDrivers}`);
        console.log(`   Available drivers: ${availableDrivers}`);

        // Check pending jobs
        const pendingJobKeys = await redis.keys('job:*:pending_drivers');
        console.log(`   Jobs with pending drivers: ${pendingJobKeys.length}`);

        // Check consumer lag (simplified)
        console.log(`   Test jobs created: ${this.testJobs.size}`);
    }

    async calculateSuccessMetrics() {
        const successRate = (metrics.redisAssignments / metrics.paymentsSent) * 100;
        const processingTime = metrics.endTime - metrics.startTime;
        const kafkaDeliveryRate = (metrics.kafkaAssignmentEvents / metrics.redisAssignments) * 100;

        console.log('\n📊 PERFORMANCE METRICS:');
        console.log('='.repeat(50));
        console.log(`⏱️  Total Processing Time: ${processingTime}ms`);
        console.log(`📤 Payments Sent: ${metrics.paymentsSent}`);
        console.log(`✅ Kafka Assignment Events: ${metrics.kafkaAssignmentEvents}`);
        console.log(`🔍 Redis Actual Assignments: ${metrics.redisAssignments}`);
        console.log(`📈 Success Rate: ${successRate.toFixed(1)}%`);
        console.log(`📨 Kafka Delivery Rate: ${kafkaDeliveryRate.toFixed(1)}%`);
        console.log('='.repeat(50));

        // Performance assessment
        if (successRate >= 90) {
            console.log('🎉 EXCELLENT! System is performing optimally (90%+ success rate)');
        } else if (successRate >= 80) {
            console.log('✅ GOOD! System is working correctly (80%+ success rate)');
        } else if (successRate >= 50) {
            console.log('⚠️  MODERATE! System has some issues (50%+ success rate)');
        } else {
            console.log('❌ POOR! System needs investigation (<50% success rate)');
        }

        if (kafkaDeliveryRate < 80) {
            console.log('🔧 SUGGESTION: Check Kafka connection and producer configuration');
        }
    }

    async gracefulShutdown() {
        console.log('\n🛑 INITIATING GRACEFUL SHUTDOWN...');

        try {
            await producer.disconnect();
            await consumer.disconnect();
            await redis.quit();
            console.log('✅ All connections closed successfully');
        } catch (error) {
            console.log('⚠️  Some connections failed to close gracefully');
        }

        process.exit(0);
    }

    async runTest() {
        console.log('🚀 MASTER TEST SUITE STARTING');
        console.log('='.repeat(50));
        console.log(`Configuration:`);
        console.log(`   Jobs: ${CONFIG.TOTAL_JOBS}`);
        console.log(`   Cleanup: ${CONFIG.CLEANUP_BEFORE_TEST ? 'ENABLED' : 'DISABLED'}`);
        console.log(`   Debug: ${CONFIG.DEBUG_MODE ? 'ENABLED' : 'DISABLED'}`);
        console.log('='.repeat(50));

        metrics.startTime = Date.now();

        try {
            // Step 1: Connect to services
            await producer.connect();
            await consumer.connect();
            console.log('✅ Connected to Kafka & Redis');

            // Step 2: Comprehensive cleanup
            if (CONFIG.CLEANUP_BEFORE_TEST) {
                await this.comprehensiveCleanup();
            }

            // Step 3: Start monitoring
            await this.monitorAssignments();
            console.log('✅ Started assignment monitoring');

            // Step 4: Send test payments
            console.log(`\n💰 SENDING ${CONFIG.TOTAL_JOBS} PAYMENT EVENTS...`);
            for (let i = 0; i < CONFIG.TOTAL_JOBS; i++) {
                const jobId = `test_job_${Date.now()}_${i}`;
                const customerId = `test_customer_${i}`;
                await this.sendPaymentEvent(jobId, customerId);

                if (i < CONFIG.TOTAL_JOBS - 1) {
                    await new Promise(r => setTimeout(r, CONFIG.BATCH_DELAY_MS));
                }
            }

            // Step 5: Wait for processing
            console.log(`\n⏳ WAITING FOR PROCESSING (${CONFIG.PROCESSING_TIMEOUT_MS/1000}s)...`);
            await new Promise(r => setTimeout(r, CONFIG.PROCESSING_TIMEOUT_MS));

            // Step 6: Collect results
            metrics.endTime = Date.now();
            await this.checkRedisAssignments();

            if (CONFIG.DEBUG_MODE) {
                await this.debugSystemState();
            }

            // Step 7: Calculate and display metrics
            await this.calculateSuccessMetrics();

        } catch (error) {
            console.error('❌ TEST FAILED:', error);
            throw error;
        } finally {
            await this.gracefulShutdown();
        }
    }
}

// Initialize and run test
const testMaster = new TestMaster();

// Handle process signals
process.on('SIGINT', () => testMaster.gracefulShutdown());
process.on('SIGTERM', () => testMaster.gracefulShutdown());

// Run test
testMaster.runTest().catch(async (error) => {
    console.error('💥 CRITICAL TEST FAILURE:', error);
    await testMaster.gracefulShutdown();
});