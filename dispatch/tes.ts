import { BusyDriverService } from './src/services/busyDriver.services';
import { FreeDriverService } from './src/services/freeDriverService';
import { MatchedDriverService } from './src/services/MatchedDriver.services';
import { DriverLocationService } from './src/services/DriverLocation.Service';
import { ZoneService } from './src/services/ZoneService';
import { logger } from './src/logger';
import { Job } from './src/types';

async function testMatchedDriverService() {
    logger.info('========================================');
    logger.info('Starting MatchedDriverService Test');
    logger.info('========================================');

    try {
        // Initialize services
        const driverLocationService = new DriverLocationService();
        const zoneService = new ZoneService();

        // Initialize zone service
        logger.info('Initializing ZoneService...');
        await zoneService.init();

        // Create service instances
        const busyDriverService = new BusyDriverService(driverLocationService, zoneService);
        const freeDriverService = new FreeDriverService(driverLocationService, zoneService);
        const matchedDriverService = new MatchedDriverService(busyDriverService, freeDriverService);

        // Test Case 1: Sample job with pickup coordinates
        logger.info('\n--- Test Case 1: Finding matched drivers for a new job ---');

        const testJob: Job = {
            id: 'test-job-123',
            customerId: '507f1f77bcf86cd799439011',
            pickupLat: 30.7046,  // Mohali, Punjab
            pickupLng: 76.7179,
            dropLat: 30.7333,    // Chandigarh
            dropLng: 76.7794,
            fare: 500,
            vehicleType: 'Sedan',
            timestamp: Date.now()
        };

        logger.info(`Test Job Details:`);
        logger.info(`  Job ID: ${testJob.id}`);
        logger.info(`  Pickup: [${testJob.pickupLat}, ${testJob.pickupLng}]`);
        logger.info(`  Drop: [${testJob.dropLat}, ${testJob.dropLng}]`);
        logger.info(`  Customer ID: ${testJob.customerId}`);

        const startTime = Date.now();
        const batches = await matchedDriverService.getMatchedDriversForJob(testJob, testJob.customerId);
        const executionTime = Date.now() - startTime;

        logger.info('\n--- Test Results ---');
        logger.info(`Execution Time: ${executionTime}ms`);
        logger.info(`Total Batches Created: ${batches.length}`);

        if (batches.length === 0) {
            logger.warn('⚠️  No batches created - No drivers found matching criteria');
            logger.info('Possible reasons:');
            logger.info('  1. No free drivers within 15km radius');
            logger.info('  2. No busy drivers with matching drop-to-pickup routes');
            logger.info('  3. All drivers are blocked by customer');
            logger.info('  4. Zone configuration issues');
        } else {
            logger.info('✅ Success! Created driver batches');

            // Display batch details
            batches.forEach(batch => {
                logger.info(`\nBatch ${batch.batchNumber}:`);
                logger.info(`  Drivers: ${batch.drivers.length}`);
                logger.info(`  Distance Range: ${batch.distanceRange}`);
                logger.info(`  Send Time: ${batch.sendTime}ms`);
                logger.info(`  Driver IDs: ${batch.drivers.join(', ')}`);
            });

            // Display batch summary
            const summary = matchedDriverService.getBatchSummary(batches);
            logger.info(`\n📊 Batch Summary: ${summary}`);
        }

        // Test Case 2: Test batch sending simulation
        logger.info('\n--- Test Case 2: Testing Batch Sending Simulation ---');

        if (batches.length > 0) {
            logger.info('Simulating batch requests (without actual notifications)...');

            // Create a mock send method for testing
            const originalSendMethod = matchedDriverService.sendRequestsToDrivers;
            matchedDriverService.sendRequestsToDrivers = async (driverIds: string[], job: Job) => {
                logger.info(`[SIMULATION] Would send requests to ${driverIds.length} drivers for job ${job.id}`);
                driverIds.forEach(driverId => {
                    logger.info(`[SIMULATION] Sending request to driver: ${driverId}`);
                });
                await new Promise(resolve => setTimeout(resolve, 100)); // Simulate API call
            };

            await matchedDriverService.sendBatchRequests(batches, testJob);

            // Restore original method
            matchedDriverService.sendRequestsToDrivers = originalSendMethod;

            logger.info('✅ Batch sending simulation completed');
        } else {
            logger.info('Skipping batch sending simulation - no batches available');
        }

        // Test Case 3: Different coordinates
        logger.info('\n--- Test Case 3: Testing with different coordinates ---');

        const testJob2: Job = {
            id: 'test-job-456',
            customerId: '507f1f77bcf86cd799439012',
            pickupLat: 28.6139,  // Delhi
            pickupLng: 77.2090,
            dropLat: 28.7041,    // Delhi
            dropLng: 77.1025,
            fare: 300,
            vehicleType: 'Hatchback',
            timestamp: Date.now()
        };

        logger.info(`Test Job 2 Details:`);
        logger.info(`  Pickup: [${testJob2.pickupLat}, ${testJob2.pickupLng}]`);

        const batches2 = await matchedDriverService.getMatchedDriversForJob(testJob2, testJob2.customerId);

        if (batches2.length > 0) {
            logger.info(`✅ Found ${batches2.length} batches for Test Case 2`);
            const summary2 = matchedDriverService.getBatchSummary(batches2);
            logger.info(`📊 Batch Summary: ${summary2}`);
        } else {
            logger.info('⚠️  No batches found for Test Case 2');
        }

        // Test Case 4: Edge case - no drivers scenario
        logger.info('\n--- Test Case 4: Testing edge cases ---');

        const testJob3: Job = {
            id: 'test-job-789',
            customerId: '507f1f77bcf86cd799439013',
            pickupLat: 90.0000,  // Invalid/remote coordinates
            pickupLng: 180.0000,
            dropLat: 90.1000,
            dropLng: 180.1000,
            fare: 1000,
            vehicleType: 'SUV',
            timestamp: Date.now()
        };

        const batches3 = await matchedDriverService.getMatchedDriversForJob(testJob3, testJob3.customerId);

        if (batches3.length === 0) {
            logger.info('✅ Correctly handled edge case - no drivers in remote location');
        } else {
            logger.info(`⚠️  Unexpected: Found ${batches3.length} batches in remote location`);
        }

        logger.info('\n========================================');
        logger.info('All Tests Completed Successfully');
        logger.info('========================================');

    } catch (error: any) {
        logger.error('\n========================================');
        logger.error('❌ Test Failed');
        logger.error('========================================');
        logger.error(`Error: ${error.message}`);
        logger.error(`Stack: ${error.stack}`);
    }
}

// Helper function to inspect current driver data in Redis
async function inspectDriverData() {
    logger.info('\n--- Inspecting Redis Driver Data ---');

    try {
        const { redis } = require('./src/infrastructure/redis');

        // Check free drivers
        const freeDriversResult = await redis.geosearch(
            'drivers:locations',
            'FROMLONLAT',
            76.7179, // Test longitude
            30.7046, // Test latitude
            'BYRADIUS',
            15, // 15km radius
            'km',
            'WITHDIST',
            'ASC'
        );

        logger.info(`Free drivers in 15km radius: ${freeDriversResult.length}`);

        if (freeDriversResult.length > 0) {
            const sampleDriverIds = freeDriversResult.slice(0, 3).map((item: any) => item[0]);

            for (const driverId of sampleDriverIds) {
                const driverKey = `driver:${driverId}`;
                const driverData = await redis.call('JSON.GET', driverKey, '$');

                if (driverData) {
                    const parsed = JSON.parse(driverData as string)?.[0];
                    logger.info(`Driver ${driverId}:`);
                    logger.info(`  Busy: ${parsed.iAmBusy || false}`);
                    logger.info(`  Location: ${JSON.stringify(parsed.location?.coordinates)}`);
                    logger.info(`  Approved Zones: ${JSON.stringify(parsed.approved_zones)}`);
                }
            }
        }

    } catch (error: any) {
        logger.error(`Failed to inspect driver data: ${error.message}`);
    }
}

// Helper function to inspect booking data
async function inspectRedisBookings() {
    logger.info('\n--- Inspecting Redis Bookings ---');

    try {
        const { redis } = require('./src/infrastructure/redis');

        const bookingKeys = await redis.keys('booking:*');
        logger.info(`Total booking keys in Redis: ${bookingKeys.length}`);

        if (bookingKeys.length === 0) {
            logger.warn('No bookings found in Redis. Create some test bookings first!');
            return;
        }

        // Inspect first 3 bookings
        const keysToInspect = bookingKeys.slice(0, 3);

        for (const key of keysToInspect) {
            logger.info(`\nInspecting: ${key}`);

            const bookingData = await redis.call('JSON.GET', key);
            if (bookingData) {
                const parsed = typeof bookingData === 'string' ? JSON.parse(bookingData) : bookingData;

                logger.info(`  Status: ${parsed.status || parsed.bookingStatus || 'N/A'}`);
                logger.info(`  Assigned Driver: ${parsed.assignedDriver || parsed.driver?._id || 'None'}`);

                const tripAddress = parsed.tripAddress || [];
                if (tripAddress.length > 0) {
                    const pickup = tripAddress[0]?.location;
                    const drop = tripAddress[tripAddress.length - 1]?.location;

                    logger.info(`  Pickup: [${pickup?.latitude}, ${pickup?.longitude}]`);
                    logger.info(`  Drop: [${drop?.latitude}, ${drop?.longitude}]`);
                } else {
                    logger.info(`  Trip Address: Not available`);
                }
            }
        }

    } catch (error: any) {
        logger.error(`Failed to inspect bookings: ${error.message}`);
    }
}

// Performance test
async function runPerformanceTest() {
    logger.info('\n--- Performance Test ---');

    const driverLocationService = new DriverLocationService();
    const zoneService = new ZoneService();
    await zoneService.init();

    const busyDriverService = new BusyDriverService(driverLocationService, zoneService);
    const freeDriverService = new FreeDriverService(driverLocationService, zoneService);
    const matchedDriverService = new MatchedDriverService(busyDriverService, freeDriverService);

    const testJob: Job = {
        id: 'perf-test-job',
        customerId: '507f1f77bcf86cd799439011',
        pickupLat: 30.7046,
        pickupLng: 76.7179,
        dropLat: 30.7333,
        dropLng: 76.7794,
        fare: 500,
        vehicleType: 'Sedan',
        timestamp: Date.now()
    };

    // Run multiple iterations
    const iterations = 3;
    let totalTime = 0;

    for (let i = 0; i < iterations; i++) {
        const startTime = Date.now();
        const batches = await matchedDriverService.getMatchedDriversForJob(testJob, testJob.customerId);
        const executionTime = Date.now() - startTime;

        totalTime += executionTime;
        logger.info(`Iteration ${i + 1}: ${executionTime}ms - ${batches.length} batches`);
    }

    const averageTime = totalTime / iterations;
    logger.info(`Average execution time: ${averageTime.toFixed(2)}ms`);
}

// Main execution
async function main() {
    logger.info('Starting MatchedDriverService Comprehensive Test Suite');

    // First inspect what's in Redis
    await inspectRedisBookings();
    await inspectDriverData();

    // Run the main test
    await testMatchedDriverService();

    // Run performance test
    await runPerformanceTest();

    logger.info('\n🎉 All tests completed!');
    process.exit(0);
}

// Run the test
main().catch(error => {
    logger.error('Unhandled error in test:', error);
    process.exit(1);
});