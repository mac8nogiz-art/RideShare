import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point, Polygon } from '@turf/helpers';

const redis = new Redis('redis://localhost:6379');
const kafka = new Kafka({
    clientId: 'driver-test-client',
    brokers: ['localhost:9092'],
    retry: {
        retries: 3,
        initialRetryTime: 100
    }
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-results-group' });

const TEST_ZONE = {
    _id: "68062fe1d17296e5867ddae1",
    name: "St. John's International Airport",
    geoPoint: { type: "Point", coordinates: [-52.742546, 47.612504] },
    location: { type: "Polygon", coordinates: [[
            [-52.771726,47.62434],[-52.752243,47.625063],[-52.752543,47.626075],
            [-52.75117,47.626249],[-52.750226,47.625497],[-52.737051,47.625728],
            [-52.737051,47.62489],[-52.739196,47.624745],[-52.738209,47.615575],
            [-52.73306,47.611641],[-52.734261,47.61086],[-52.737651,47.611959],
            [-52.738038,47.611467],[-52.741514,47.61112],[-52.742587,47.612682],
            [-52.74426,47.61316],[-52.745655,47.61287],[-52.744872,47.614512],
            [-52.744904,47.615206],[-52.74249,47.615922],[-52.742394,47.616277],
            [-52.744904,47.615517],[-52.744936,47.617325],[-52.745548,47.617282],
            [-52.746074,47.619054],[-52.752275,47.61828],[-52.756888,47.616718],
            [-52.764592,47.621867],[-52.76575,47.622503],[-52.772016,47.622735],
            [-52.772788,47.623545],[-52.771726,47.62434]
        ]] }
};

const TOTAL_DRIVERS = 20;
const TOTAL_JOBS = 5;
const TEST_TIMEOUT = 30000;

// Track sent jobs for accurate matching
const sentJobs = new Map<string, {
    jobId: string;
    timestamp: number;
    location: [number, number];
    customerId: string;
}>();

// Test results tracking
interface TestResults {
    jobsSent: number;
    driversSeeded: number;
    matchedJobs: Map<string, {
        jobId: string;
        matchedDrivers: string[];
        timestamp: number;
        zone: string;
        responseTime: number;
    }>;
    rejectedJobs: Set<string>;
    kafkaHealthy: boolean;
    redisHealthy: boolean;
    zoneSystemWorking: boolean;
    errors: string[];
    debugMessages: string[];
}

const results: TestResults = {
    jobsSent: 0,
    driversSeeded: 0,
    matchedJobs: new Map(),
    rejectedJobs: new Set(),
    kafkaHealthy: false,
    redisHealthy: false,
    zoneSystemWorking: false,
    errors: [],
    debugMessages: []
};

// Utility function to add debug messages
function addDebugMessage(message: string) {
    const timestamp = new Date().toISOString();
    results.debugMessages.push(`[${timestamp}] ${message}`);
    console.log(`🔍 ${message}`);
}

// Generates a random point inside a polygon
function randomPointInPolygon(polygon: number[][]): [number, number] {
    const poly: Polygon = { type: 'Polygon', coordinates: [polygon] };
    let p: [number, number];
    let tries = 0;
    do {
        const lons = polygon.map(c => c[0]);
        const lats = polygon.map(c => c[1]);
        const lng = Math.min(...lons) + Math.random() * (Math.max(...lons) - Math.min(...lons));
        const lat = Math.min(...lats) + Math.random() * (Math.max(...lats) - Math.min(...lats));
        p = [lng, lat];
        tries++;
        if (tries > 1000) throw new Error("Cannot generate point in polygon");
    } while (!booleanPointInPolygon(point(p), poly));
    return p;
}

// Clean up old test data
async function cleanupOldData() {
    console.log('🧹 Cleaning up old test data...');
    try {
        const driverKeys = await redis.keys('driver:*:*');
        const testDriverKeys = await redis.keys('driver:test_driver_*:*');
        const allKeys = [...driverKeys, ...testDriverKeys];

        if (allKeys.length > 0) {
            await redis.del(...allKeys);
            addDebugMessage(`Deleted ${allKeys.length} old driver keys`);
        }

        const geoMembers = await redis.zrange('drivers:locations', 0, -1);
        if (geoMembers.length > 0) {
            await redis.zrem('drivers:locations', ...geoMembers);
            addDebugMessage(`Cleared ${geoMembers.length} entries from geospatial index`);
        }

        // Clear job cache and test data
        const jobKeys = await redis.keys('job:*:*');
        const testJobKeys = await redis.keys('test_job_*');
        if (jobKeys.length > 0 || testJobKeys.length > 0) {
            await redis.del(...jobKeys, ...testJobKeys);
            addDebugMessage(`Cleared ${jobKeys.length + testJobKeys.length} job cache entries`);
        }

        // Clear any test consumer offsets
        try {
            await redis.del('test-consumer-offsets');
        } catch (e) {
            // Ignore errors for this
        }

        console.log('✅ Cleanup complete\n');
    } catch (error) {
        console.error('⚠️  Cleanup error:', error);
        results.errors.push(`Cleanup error: ${error}`);
    }
}

// Check system health
async function checkSystemHealth() {
    console.log('🏥 Checking system health...');

    try {
        await redis.ping();
        results.redisHealthy = true;
        console.log('   ✓ Redis: Healthy');

        // Test Redis commands we'll use
        await redis.geosearch('drivers:locations', 'FROMLONLAT', 0, 0, 'BYRADIUS', 1, 'km');
        console.log('   ✓ Redis GEOSEARCH: Working');
    } catch (error) {
        console.error('   ✗ Redis: Unhealthy', error);
        results.errors.push('Redis connection failed');
    }

    try {
        await producer.connect();
        results.kafkaHealthy = true;
        console.log('   ✓ Kafka: Healthy');

        // Test Kafka topics
        const admin = kafka.admin();
        await admin.connect();
        const topics = await admin.listTopics();
        console.log(`   ✓ Kafka Topics: ${topics.length} topics available`);
        await admin.disconnect();
    } catch (error) {
        console.error('   ✗ Kafka: Unhealthy', error);
        results.errors.push('Kafka connection failed');
    }

    console.log();
}

// Seed drivers inside the zone
async function seedDrivers() {
    console.log(`📍 Seeding ${TOTAL_DRIVERS} drivers in zone "${TEST_ZONE.name}"...`);

    const pipeline = redis.pipeline();
    const now = Date.now();

    for (let i = 0; i < TOTAL_DRIVERS; i++) {
        const [lng, lat] = randomPointInPolygon(TEST_ZONE.location.coordinates[0]);
        const id = `test_driver_${i+1}`;

        // Store location with proper timestamp
        pipeline.hset(`driver:${id}:location`, {
            lat: lat.toString(),
            lng: lng.toString(),
            lastUpdate: now.toString(),
            ts: now.toString()
        });

        // Store profile
        pipeline.hset(`driver:${id}:profile`, {
            isBusy: 'false',
            score: (60 + Math.floor(Math.random() * 40)).toString(), // 60-100
            isNew: (i < 3).toString() // First 3 are new drivers
        });

        // Add to approved zones
        pipeline.sadd(`driver:${id}:approved_zones`, TEST_ZONE._id);

        // Add to geospatial index
        pipeline.geoadd('drivers:locations', lng, lat, id);
    }

    await pipeline.exec();
    results.driversSeeded = TOTAL_DRIVERS;

    console.log(`✅ Seeded ${TOTAL_DRIVERS} drivers successfully\n`);
}

// Verify geospatial index
async function verifyGeospatialIndex() {
    console.log('🔍 Verifying geospatial index...');

    const geoCount = await redis.zcard('drivers:locations');
    console.log(`   Drivers in geo index: ${geoCount}`);

    if (geoCount === TOTAL_DRIVERS) {
        console.log('   ✓ All drivers indexed correctly');
    } else {
        console.log(`   ⚠️  Expected ${TOTAL_DRIVERS}, found ${geoCount}`);
        results.errors.push(`Geo index mismatch: expected ${TOTAL_DRIVERS}, got ${geoCount}`);
    }

    // Test a sample geosearch from zone center
    const [centerLng, centerLat] = TEST_ZONE.geoPoint.coordinates;
    const testSearch = await redis.geosearch(
        'drivers:locations',
        'FROMLONLAT', centerLng, centerLat,
        'BYRADIUS', 15, 'km',
        'WITHDIST'
    );

    console.log(`   Sample geosearch found: ${testSearch ? testSearch.length / 2 : 0} drivers`);

    // Verify we can find drivers
    if (testSearch && testSearch.length > 0) {
        console.log('   ✓ Geosearch is returning results');
    } else {
        console.log('   ⚠️  Geosearch returned no results');
        results.errors.push('Geosearch returned no drivers');
    }
    console.log();
}

// Setup result consumer with comprehensive monitoring
async function setupResultConsumer() {
    console.log('📡 Setting up result consumer...');

    await consumer.connect();
    await consumer.subscribe({
        topics: ['driver.notifications', 'dispatch-service.response', 'dispatch-service.request'],
        fromBeginning: true // Important: read all messages for debugging
    });

    let messageCount = 0;

    consumer.run({
        eachMessage: async ({ topic, message }) => {
            messageCount++;
            try {
                const data = JSON.parse(message.value?.toString() || '{}');
                const messageInfo = `[${topic}] ${message.key?.toString() || 'no-key'}`;

                addDebugMessage(`Kafka Message #${messageCount}: ${messageInfo}`);

                // Track ALL job matches from any topic
                if (data.jobId && data.driverId) {
                    addDebugMessage(`MATCH DETECTED: Job ${data.jobId} -> Driver ${data.driverId} on topic ${topic}`);

                    if (!results.matchedJobs.has(data.jobId)) {
                        // Calculate response time if we have the sent job info
                        const sentJob = sentJobs.get(data.jobId);
                        const responseTime = sentJob ? Date.now() - sentJob.timestamp : 0;

                        results.matchedJobs.set(data.jobId, {
                            jobId: data.jobId,
                            matchedDrivers: [data.driverId],
                            timestamp: Date.now(),
                            zone: data.zone || topic,
                            responseTime
                        });
                    } else {
                        const match = results.matchedJobs.get(data.jobId)!;
                        if (!match.matchedDrivers.includes(data.driverId)) {
                            match.matchedDrivers.push(data.driverId);
                        }
                    }
                }

                // Special handling for different message types
                if (topic === 'driver.notifications') {
                    addDebugMessage(`NOTIFICATION: ${data.type || 'unknown-type'}`);
                } else if (topic === 'dispatch-service.response') {
                    addDebugMessage(`DISPATCH RESPONSE: ${data.status || 'unknown-status'}`);
                } else if (topic === 'dispatch-service.request') {
                    addDebugMessage(`DISPATCH REQUEST: ${data.type || 'unknown-type'}`);
                }

            } catch (error) {
                addDebugMessage(`ERROR parsing message: ${error}`);
                results.errors.push(`Message parsing error: ${error}`);
            }
        }
    });

    console.log('✅ Result consumer ready (listening to all topics)\n');
}

// Send test jobs with proper tracking
async function sendTestJobs() {
    console.log(`📤 Sending ${TOTAL_JOBS} test jobs...`);
    console.log('   (Waiting 3 seconds for dispatch service to load cache...)\n');

    await new Promise(r => setTimeout(r, 3000));

    const baseTimestamp = Date.now();

    for (let i = 0; i < TOTAL_JOBS; i++) {
        const jobId = `test_job_${baseTimestamp}_${i}`;
        const customerId = `test_customer_${i}`;
        const [pickupLng, pickupLat] = randomPointInPolygon(TEST_ZONE.location.coordinates[0]);

        const event = {
            type: 'payment.completed',
            jobId,
            customerId,
            pickupLat,
            pickupLng,
            fare: 200 + Math.floor(Math.random() * 100),
            vehicleType: 'sedan',
            timestamp: baseTimestamp,
            zoneId: TEST_ZONE._id
        };

        // Track the job BEFORE sending
        sentJobs.set(jobId, {
            jobId,
            timestamp: baseTimestamp,
            location: [pickupLat, pickupLng],
            customerId
        });

        addDebugMessage(`SENDING JOB: ${jobId} for customer ${customerId}`);

        await producer.send({
            topic: 'dispatch-service.request',
            messages: [{ key: jobId, value: JSON.stringify(event) }]
        });

        results.jobsSent++;
        console.log(`   ✓ Sent ${jobId} at [${pickupLat.toFixed(6)}, ${pickupLng.toFixed(6)}]`);

        // Smaller delay between jobs
        await new Promise(r => setTimeout(r, 300));
    }

    console.log(`\n✅ Sent ${TOTAL_JOBS} jobs successfully\n`);
    addDebugMessage(`Sent ${TOTAL_JOBS} jobs, tracking: ${Array.from(sentJobs.keys()).join(', ')}`);
}

// Wait for results with improved tracking
async function waitForResults() {
    console.log(`⏳ Waiting up to ${TEST_TIMEOUT/1000} seconds for results...\n`);

    const startTime = Date.now();
    const checkInterval = 1000;
    let lastMatchCount = 0;

    while (Date.now() - startTime < TEST_TIMEOUT) {
        await new Promise(r => setTimeout(r, checkInterval));

        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const currentMatches = results.matchedJobs.size;

        // Show progress with more details
        console.log(`   Progress: ${currentMatches}/${TOTAL_JOBS} jobs matched (${elapsed}s elapsed)`);

        // Show if we got new matches
        if (currentMatches > lastMatchCount) {
            console.log(`   🎯 New matches detected!`);
            lastMatchCount = currentMatches;
        }

        // Check if we got all expected results
        const matchedSentJobs = Array.from(results.matchedJobs.keys()).filter(jobId =>
            sentJobs.has(jobId)
        ).length;

        if (matchedSentJobs === TOTAL_JOBS) {
            console.log('✅ All sent jobs processed!\n');
            break;
        }

        // Show detailed matching progress every 5 seconds
        if (elapsed % 5 === 0) {
            const sentJobIds = Array.from(sentJobs.keys());
            const matchedJobIds = Array.from(results.matchedJobs.keys());
            const unmatchedJobs = sentJobIds.filter(id => !matchedJobIds.includes(id));

            console.log(`   🔍 Detailed: ${matchedJobIds.length} matched, ${unmatchedJobs.length} unmatched`);
            if (unmatchedJobs.length > 0) {
                console.log(`   ⏳ Waiting for: ${unmatchedJobs.slice(0, 3).join(', ')}${unmatchedJobs.length > 3 ? '...' : ''}`);
            }
        }
    }

    // Final status
    const finalMatched = Array.from(results.matchedJobs.keys()).filter(jobId =>
        sentJobs.has(jobId)
    ).length;

    console.log(`\n📊 Final matching: ${finalMatched}/${TOTAL_JOBS} sent jobs matched`);
}

// Check for rejected jobs using actual sent jobs
async function checkRejectedJobs() {
    console.log('🔍 Checking for rejected/unmatched jobs...\n');

    let unmatchedCount = 0;

    for (const [jobId, jobInfo] of sentJobs.entries()) {
        if (!results.matchedJobs.has(jobId)) {
            results.rejectedJobs.add(jobId);
            unmatchedCount++;
            console.log(`   ❌ Unmatched: ${jobId} (customer: ${jobInfo.customerId})`);
        } else {
            const match = results.matchedJobs.get(jobId)!;
            console.log(`   ✅ Matched: ${jobId} -> ${match.matchedDrivers.length} drivers in ${match.responseTime}ms`);
        }
    }

    console.log(`\n📈 Summary: ${unmatchedCount} unmatched jobs out of ${sentJobs.size} sent`);
}

// Verify zone system
async function verifyZoneSystem() {
    console.log('🗺️  Verifying zone system...');

    try {
        const activeZones = await redis.get('zones:active');
        if (activeZones) {
            const zones = JSON.parse(activeZones);
            results.zoneSystemWorking = zones.some((z: any) => z._id === TEST_ZONE._id);
            console.log(`   ✓ Zone system working: ${results.zoneSystemWorking ? 'YES' : 'NO'}`);
            console.log(`   ✓ Active zones: ${zones.length}`);

            if (results.zoneSystemWorking) {
                const ourZone = zones.find((z: any) => z._id === TEST_ZONE._id);
                console.log(`   ✓ Our zone: ${ourZone?.name || 'unknown'}`);
            }
        } else {
            console.log('   ✗ No active zones found');
            results.errors.push('No active zones in Redis');
        }
    } catch (error) {
        console.error('   ✗ Zone system error:', error);
        results.errors.push(`Zone system error: ${error}`);
    }

    console.log();
}

// Print final results with comprehensive details
function printResults() {
    console.log('\n' + '='.repeat(80));
    console.log('📊 FINAL TEST RESULTS');
    console.log('='.repeat(80) + '\n');

    // System Health
    console.log('🏥 SYSTEM HEALTH:');
    console.log(`   Redis: ${results.redisHealthy ? '✅ Healthy' : '❌ Unhealthy'}`);
    console.log(`   Kafka: ${results.kafkaHealthy ? '✅ Healthy' : '❌ Unhealthy'}`);
    console.log(`   Zone System: ${results.zoneSystemWorking ? '✅ Working' : '❌ Not Working'}`);
    console.log();

    // Test Data
    console.log('📍 TEST DATA:');
    console.log(`   Zone: ${TEST_ZONE.name}`);
    console.log(`   Zone ID: ${TEST_ZONE._id}`);
    console.log(`   Drivers Seeded: ${results.driversSeeded}`);
    console.log(`   Jobs Sent: ${results.jobsSent}`);
    console.log(`   Jobs Tracked: ${sentJobs.size}`);
    console.log();

    // Job Matching Results
    console.log('🎯 JOB MATCHING RESULTS:');
    const matchedSentJobs = Array.from(results.matchedJobs.keys()).filter(jobId =>
        sentJobs.has(jobId)
    ).length;

    console.log(`   ✅ Accepted (Matched): ${matchedSentJobs}`);
    console.log(`   ❌ Rejected (No Match): ${results.rejectedJobs.size}`);
    console.log(`   📈 Success Rate: ${((matchedSentJobs / results.jobsSent) * 100).toFixed(1)}%`);
    console.log();

    // Detailed Matching
    if (results.matchedJobs.size > 0) {
        console.log('📋 MATCHED JOBS DETAILS:');
        let totalDriversMatched = 0;
        let totalResponseTime = 0;
        let countedJobs = 0;

        results.matchedJobs.forEach((match, jobId) => {
            // Only count jobs we actually sent
            if (sentJobs.has(jobId)) {
                console.log(`   Job: ${jobId}`);
                console.log(`      - Zone: ${match.zone}`);
                console.log(`      - Matched Drivers: ${match.matchedDrivers.length}`);
                console.log(`      - Response Time: ${match.responseTime}ms`);
                console.log(`      - Drivers: [${match.matchedDrivers.slice(0, 5).join(', ')}${match.matchedDrivers.length > 5 ? '...' : ''}]`);
                totalDriversMatched += match.matchedDrivers.length;
                totalResponseTime += match.responseTime;
                countedJobs++;
            }
        });

        if (countedJobs > 0) {
            const avgDriversPerJob = (totalDriversMatched / countedJobs).toFixed(1);
            const avgResponseTime = (totalResponseTime / countedJobs).toFixed(0);
            console.log(`\n   📊 Averages: ${avgDriversPerJob} drivers/job, ${avgResponseTime}ms response time`);
        }
        console.log();
    }

    // Rejected Jobs
    if (results.rejectedJobs.size > 0) {
        console.log('❌ REJECTED JOBS:');
        results.rejectedJobs.forEach(jobId => {
            const jobInfo = sentJobs.get(jobId);
            console.log(`   - ${jobId} ${jobInfo ? `(customer: ${jobInfo.customerId})` : ''}`);
        });
        console.log();
    }

    // Debug Info
    if (results.debugMessages.length > 0) {
        console.log('🔍 LAST 10 DEBUG MESSAGES:');
        results.debugMessages.slice(-10).forEach(msg => {
            console.log(`   ${msg}`);
        });
        console.log();
    }

    // Errors
    if (results.errors.length > 0) {
        console.log('⚠️  ERRORS ENCOUNTERED:');
        results.errors.forEach(error => {
            console.log(`   - ${error}`);
        });
        console.log();
    }

    // Overall Status
    console.log('='.repeat(80));
    const allHealthy = results.redisHealthy && results.kafkaHealthy && results.zoneSystemWorking;
    const goodMatchRate = (matchedSentJobs / results.jobsSent) >= 0.8;

    if (allHealthy && goodMatchRate && results.errors.length === 0) {
        console.log('✅ TEST PASSED - All systems working correctly!');
    } else if (allHealthy && goodMatchRate) {
        console.log('⚠️  TEST PASSED WITH WARNINGS - Check errors above');
    } else if (allHealthy && matchedSentJobs > 0) {
        console.log('⚠️  TEST PARTIALLY SUCCESSFUL - Some jobs matched');
    } else {
        console.log('❌ TEST FAILED - Issues detected, review results above');
    }
    console.log('='.repeat(80) + '\n');
}

// Quick validation test
async function runQuickValidation() {
    console.log(' RUNNING QUICK VALIDATION...\n');

    // Test basic Redis operations
    try {
        await redis.set('test:validation', 'ok');
        const result = await redis.get('test:validation');
        if (result === 'ok') {
            console.log('    Redis basic operations working');
        }
        await redis.del('test:validation');
    } catch (error) {
        console.log('    Redis operations failed');
    }

    // Test Kafka producer
    try {
        await producer.send({
            topic: 'dispatch-service.request',
            messages: [{ key: 'validation_test', value: JSON.stringify({ test: true })}]
        });
        console.log('    Kafka producer working');
    } catch (error) {
        console.log('   Kafka producer failed');
    }

    console.log(' Quick validation complete\n');
}

// Main test flow
async function main() {
    try {
        console.log(' DISPATCH SERVICE COMPREHENSIVE TEST\n');
        console.log('='.repeat(80) + '\n');

        // 0. Quick validation
        await runQuickValidation();

        // 1. Check system health
        await checkSystemHealth();

        if (!results.redisHealthy || !results.kafkaHealthy) {
            throw new Error('System health check failed. Cannot proceed.');
        }

        // 2. Cleanup old data
        await cleanupOldData();

        // 3. Save active zones
        await redis.set('zones:active', JSON.stringify([TEST_ZONE]));
        console.log(' Saved active zones to Redis\n');

        // 4. Seed drivers
        await seedDrivers();

        // 5. Verify geospatial index
        await verifyGeospatialIndex();

        // 6. Verify zone system
        await verifyZoneSystem();

        // 7. Setup result consumer
        await setupResultConsumer();

        // 8. Send test jobs
        await sendTestJobs();

        // 9. Wait for results
        await waitForResults();

        // 10. Check for rejected jobs
        await checkRejectedJobs();

        // 11. Print final results
        printResults();

    } catch (error) {
        console.error(' TEST FAILED:', error);
        results.errors.push(`Fatal error: ${error}`);
        printResults();
        process.exit(1);
    } finally {
        // Cleanup
        try {
            await consumer.disconnect();
            await producer.disconnect();
            await redis.quit();
            console.log(' Disconnected from all services\n');
        } catch (error) {
            console.error('Cleanup error:', error);
        }
    }
}

// Run the test
main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});