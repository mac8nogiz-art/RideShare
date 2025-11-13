// test-batch-system.ts - Test script for the batch TTL system

import { redis } from './infrastructure/redis';
import { logger } from './logger';
import { redisSubscriber, connectRedisSubscriber } from './redisConnection/subscriber';

/**
 * Test script to verify the event-based batch system
 */
async function testBatchTTLSystem() {
    logger.info('🧪 Starting Batch TTL System Test...\n');

    try {
        // Connect to Redis subscriber
        await connectRedisSubscriber();

        // Setup event handler for testing
        setupTestEventHandler();

        // Test 1: Single batch trigger
        await test1_SingleBatchTrigger();

        // Test 2: Multiple sequential batches
        await test2_MultipleSequentialBatches();

        // Test 3: Cleanup on job assignment
        await test3_CleanupOnAssignment();

        // Test 4: Missed event detection
        await test4_MissedEventDetection();

        logger.info('\n✅ All tests completed!\n');

    } catch (error: any) {
        logger.error(`❌ Test failed: ${error.message}`);
    }
}

function setupTestEventHandler() {
    logger.info('📡 Setting up test event handler...\n');

    redisSubscriber.addExpiryHandler(async (expiredKey: string) => {
        if (expiredKey.includes(':batch_trigger:')) {
            const match = expiredKey.match(/job:([^:]+):batch_trigger:(\d+)/);
            if (match) {
                const [, jobId, batchNumber] = match;
                logger.info(`⏰ EVENT DETECTED: Batch ${batchNumber} trigger expired for Job ${jobId}\n`);
            }
        }
    });
}

async function test1_SingleBatchTrigger() {
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger.info('TEST 1: Single Batch Trigger with TTL');
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    const testJobId = `TEST-${Date.now()}`;
    const ttl = 5; // 5 seconds for testing

    logger.info(`Creating batch trigger for Job: ${testJobId}`);
    logger.info(`TTL: ${ttl} seconds\n`);

    // Create a batch trigger key with short TTL
    await redis.setex(`job:${testJobId}:batch_trigger:1`, ttl, '2');

    logger.info('✅ Batch trigger key created');
    logger.info(`Waiting ${ttl} seconds for expiry event...\n`);

    // Wait for the key to expire
    await sleep((ttl + 1) * 1000);

    // Verify the key is gone
    const exists = await redis.exists(`job:${testJobId}:batch_trigger:1`);
    logger.info(`Key exists after expiry: ${exists ? '❌ YES (ERROR)' : '✅ NO (CORRECT)'}\n`);
}

async function test2_MultipleSequentialBatches() {
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger.info('TEST 2: Multiple Sequential Batches');
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    const testJobId = `TEST-${Date.now()}`;
    const ttl = 3; // 3 seconds for testing
    const numBatches = 3;

    logger.info(`Creating ${numBatches} batch triggers for Job: ${testJobId}`);
    logger.info(`TTL: ${ttl} seconds each\n`);

    // Simulate sequential batch flow
    for (let i = 1; i <= numBatches; i++) {
        logger.info(`Setting up Batch ${i} trigger...`);
        await redis.setex(`job:${testJobId}:batch_trigger:${i}`, ttl, (i + 1).toString());

        logger.info(`⏰ Waiting ${ttl} seconds for Batch ${i} to expire...\n`);
        await sleep(ttl * 1000 + 500);

        // Verify expiry
        const exists = await redis.exists(`job:${testJobId}:batch_trigger:${i}`);
        logger.info(`Batch ${i} expired: ${!exists ? '✅ YES' : '❌ NO'}\n`);
    }
}

async function test3_CleanupOnAssignment() {
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger.info('TEST 3: Cleanup on Job Assignment');
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    const testJobId = `TEST-${Date.now()}`;

    // Create flow data
    logger.info(`Creating flow data for Job: ${testJobId}`);
    await redis.setex(`job:${testJobId}:matched_flow_active`, 300, '1');
    await redis.setex(`job:${testJobId}:flow_data`, 300, JSON.stringify({
        job: { id: testJobId },
        batches: [{}, {}, {}],
        nextBatchIndex: 1
    }));

    // Create batch triggers
    await redis.setex(`job:${testJobId}:batch_trigger:1`, 300, '2');
    await redis.setex(`job:${testJobId}:batch_trigger:2`, 300, '3');

    logger.info('✅ Created flow data and batch triggers');

    // Count keys before cleanup
    let keys = await redis.keys(`job:${testJobId}:*`);
    logger.info(`Keys before cleanup: ${keys.length}\n`);

    // Simulate driver acceptance and cleanup
    logger.info('Simulating driver acceptance...');
    await redis.set(`job:${testJobId}:status`, 'assigned');

    // Cleanup
    logger.info('Running cleanup...\n');
    await cleanupTestFlow(testJobId);

    // Count keys after cleanup
    keys = await redis.keys(`job:${testJobId}:*`);
    logger.info(`Keys after cleanup: ${keys.length} ${keys.length === 1 ? '✅' : '❌'}`);
    logger.info('(Only status key should remain)\n');
}

async function test4_MissedEventDetection() {
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger.info('TEST 4: Missed Event Detection');
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    const testJobId = `TEST-${Date.now()}`;

    // Create flow data with timestamp in past
    const pastTime = Date.now() - 60000; // 60 seconds ago
    const flowData = {
        job: { id: testJobId },
        batches: [
            { batchNumber: 1, sendTime: pastTime, drivers: ['D1', 'D2'] },
            { batchNumber: 2, sendTime: 0, drivers: ['D3', 'D4'] }
        ],
        nextBatchIndex: 1,
        totalBatches: 2,
        flowStartTime: pastTime
    };

    await redis.setex(`job:${testJobId}:matched_flow_active`, 300, '1');
    await redis.setex(`job:${testJobId}:flow_data`, 300, JSON.stringify(flowData));

    logger.info(`Created stuck flow for Job: ${testJobId}`);
    logger.info('Batch 1 sent 60 seconds ago, but Batch 2 never triggered\n');

    // Simulate monitor detection
    logger.info('Running monitor check...');
    const detected = await checkStuckBatch(testJobId, flowData);

    logger.info(`Stuck batch detected: ${detected ? '✅ YES' : '❌ NO'}\n`);

    // Cleanup
    await cleanupTestFlow(testJobId);
}

// Helper functions
async function cleanupTestFlow(jobId: string) {
    await redis.del(`job:${jobId}:matched_flow_active`);
    await redis.del(`job:${jobId}:flow_data`);

    const triggerKeys = await redis.keys(`job:${jobId}:batch_trigger:*`);
    if (triggerKeys.length > 0) {
        await redis.del(...triggerKeys);
    }
}

async function checkStuckBatch(jobId: string, flowData: any): Promise<boolean> {
    const BATCH_INTERVAL = 45000;

    if (flowData.nextBatchIndex > 0 && flowData.nextBatchIndex < flowData.totalBatches) {
        const previousBatch = flowData.batches[flowData.nextBatchIndex - 1];
        const timeSinceBatch = Date.now() - previousBatch.sendTime;

        if (timeSinceBatch > BATCH_INTERVAL + 5000) {
            const triggerKey = `job:${jobId}:batch_trigger:${previousBatch.batchNumber}`;
            const triggerExists = await redis.exists(triggerKey);

            return !triggerExists; // Stuck if trigger doesn't exist
        }
    }

    return false;
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Run tests
testBatchTTLSystem()
    .then(() => {
        logger.info('🎉 Test suite completed successfully!');
        process.exit(0);
    })
    .catch((error) => {
        logger.error(`❌ Test suite failed: ${error.message}`);
        process.exit(1);
    });

// Export for use in other test files
export {
    testBatchTTLSystem,
    cleanupTestFlow,
    checkStuckBatch
};