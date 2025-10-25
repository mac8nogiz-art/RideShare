import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import { Kafka, Producer, Consumer, logLevel } from 'kafkajs';

dotenv.config();

// Use your actual configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '172.105.61.99:9093';
const PAYMENT_TOPIC = process.env.PAYMENT_TOPIC || 'payment.events';
const DRIVER_LOCATION_TOPIC = process.env.DRIVER_LOCATION_TOPIC || 'driver.location';
const RIDE_REQUEST_TOPIC = process.env.RIDE_REQUEST_TOPIC || 'ride.request';
const USER_REQUEST_TOPIC = process.env.USER_REQUEST_TOPIC || 'user.request';
const USER_RESPONSE_TOPIC = process.env.USER_RESPONSE_TOPIC || 'user.response';

console.log('\n' + '═'.repeat(60));
console.log('🔌 KAFKA CONFIGURATION');
console.log('═'.repeat(60));
console.log(`Broker: ${KAFKA_BROKER}`);
console.log(`Topics:`);
console.log(`  - ${PAYMENT_TOPIC}`);
console.log(`  - ${DRIVER_LOCATION_TOPIC}`);
console.log(`  - ${RIDE_REQUEST_TOPIC}`);
console.log(`  - ${USER_REQUEST_TOPIC}`);
console.log(`  - ${USER_RESPONSE_TOPIC}`);
console.log('═'.repeat(60) + '\n');

// Initialize Kafka
const kafka = new Kafka({
    clientId: 'kafka-test-client',
    brokers: [KAFKA_BROKER],
    logLevel: logLevel.INFO,
    retry: {
        retries: 8,
        initialRetryTime: 300,
        maxRetryTime: 30000,
    },
    connectionTimeout: 30000,
    requestTimeout: 30000,
});

const producer: Producer = kafka.producer({
    allowAutoTopicCreation: true,
    idempotent: true,
    maxInFlightRequests: 5,
    retry: {
        retries: 5,
        initialRetryTime: 300,
    }
});

// Random generator helpers
const getRandomAmount = () => Number((Math.random() * 1000).toFixed(2));
const getRandomDriverId = () => `driver-${Math.floor(Math.random() * 100 + 1)}`;
const getRandomUserId = () => `user-${Math.floor(Math.random() * 1000 + 1)}`;
const getRandomStatus = () => ['SUCCESS', 'FAILED', 'PENDING'][Math.floor(Math.random() * 3)];
const getRandomLocation = () => ({
    latitude: 28.7041 + (Math.random() - 0.5) * 0.1,
    longitude: 77.1025 + (Math.random() - 0.5) * 0.1,
});

// Event generators
const createPaymentEvent = () => ({
    paymentId: uuidv4(),
    driverId: getRandomDriverId(),
    userId: getRandomUserId(),
    amount: getRandomAmount(),
    status: getRandomStatus(),
    timestamp: new Date().toISOString(),
});

const createDriverLocationEvent = () => ({
    driverId: getRandomDriverId(),
    location: getRandomLocation(),
    timestamp: new Date().toISOString(),
    speed: Math.floor(Math.random() * 80),
    heading: Math.floor(Math.random() * 360),
});

const createRideRequestEvent = () => ({
    requestId: uuidv4(),
    userId: getRandomUserId(),
    pickupLocation: getRandomLocation(),
    dropoffLocation: getRandomLocation(),
    timestamp: new Date().toISOString(),
    vehicleType: ['sedan', 'suv', 'bike'][Math.floor(Math.random() * 3)],
});

const createUserRequestEvent = () => ({
    requestId: uuidv4(),
    userId: getRandomUserId(),
    action: ['CREATE_RIDE', 'CANCEL_RIDE', 'UPDATE_PROFILE'][Math.floor(Math.random() * 3)],
    timestamp: new Date().toISOString(),
});

// Send message function
async function sendMessage(topic: string, key: string, value: any) {
    try {
        const result = await producer.send({
            topic,
            messages: [
                {
                    key,
                    value: JSON.stringify(value),
                    timestamp: Date.now().toString(),
                },
            ],
        });

        console.log(`✅ Sent to ${topic}`);
        console.log(`   Key: ${key}`);
        console.log(`   Partition: ${result[0].partition}`);
        console.log(`   Offset: ${result[0].baseOffset}`);
        return true;
    } catch (error: any) {
        console.error(`❌ Failed to send to ${topic}:`, error.message);
        return false;
    }
}

// Test all topics
async function testAllTopics(count: number = 5, intervalMs: number = 2000) {
    console.log('\n🚀 Starting Kafka message sending...\n');

    try {
        console.log('🔌 Connecting to producer...');
        await producer.connect();
        console.log('✅ Producer connected\n');

        for (let i = 1; i <= count; i++) {
            console.log('\n' + '─'.repeat(60));
            console.log(`📦 Batch ${i}/${count} - ${new Date().toLocaleTimeString()}`);
            console.log('─'.repeat(60));

            // Send payment event
            const payment = createPaymentEvent();
            console.log('\n💰 Sending Payment Event:');
            console.log(`   Payment ID: ${payment.paymentId}`);
            console.log(`   Amount: $${payment.amount}`);
            await sendMessage(PAYMENT_TOPIC, payment.paymentId, payment);

            await new Promise((res) => setTimeout(res, 500));

            // Send driver location event
            const location = createDriverLocationEvent();
            console.log('\n🚗 Sending Driver Location:');
            console.log(`   Driver ID: ${location.driverId}`);
            console.log(`   Location: (${location.location.latitude.toFixed(4)}, ${location.location.longitude.toFixed(4)})`);
            await sendMessage(DRIVER_LOCATION_TOPIC, location.driverId, location);

            await new Promise((res) => setTimeout(res, 500));

            // Send ride request event
            const rideRequest = createRideRequestEvent();
            console.log('\n🚕 Sending Ride Request:');
            console.log(`   Request ID: ${rideRequest.requestId}`);
            console.log(`   User ID: ${rideRequest.userId}`);
            await sendMessage(RIDE_REQUEST_TOPIC, rideRequest.requestId, rideRequest);

            await new Promise((res) => setTimeout(res, 500));

            // Send user request event
            const userRequest = createUserRequestEvent();
            console.log('\n👤 Sending User Request:');
            console.log(`   Request ID: ${userRequest.requestId}`);
            console.log(`   Action: ${userRequest.action}`);
            await sendMessage(USER_REQUEST_TOPIC, userRequest.requestId, userRequest);

            if (i < count) {
                console.log(`\n⏳ Waiting ${intervalMs}ms before next batch...`);
                await new Promise((res) => setTimeout(res, intervalMs));
            }
        }

        console.log('\n' + '═'.repeat(60));
        console.log(`✅ Successfully sent ${count * 4} messages to Kafka`);
        console.log('═'.repeat(60));

    } catch (error: any) {
        console.error('\n❌ Error during message sending:', error.message);
        console.error('Stack:', error.stack);
    } finally {
        await producer.disconnect();
        console.log('\n✅ Producer disconnected\n');
    }
}

// Test consumer (listen to messages)
async function testConsumer(topic: string, duration: number = 30000) {
    console.log(`\n👂 Listening to ${topic} for ${duration / 1000} seconds...\n`);

    const consumer: Consumer = kafka.consumer({
        groupId: `test-consumer-${Date.now()}`,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
    });

    try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        let messageCount = 0;

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                messageCount++;
                const value = message.value?.toString();
                const key = message.key?.toString();
                const parsedValue = value ? JSON.parse(value) : null;

                console.log('\n' + '─'.repeat(60));
                console.log(`📨 Message #${messageCount} - ${new Date().toLocaleTimeString()}`);
                console.log('─'.repeat(60));
                console.log(`   Topic: ${topic}`);
                console.log(`   Partition: ${partition}`);
                console.log(`   Offset: ${message.offset}`);
                console.log(`   Key: ${key}`);
                console.log(`   Value: ${JSON.stringify(parsedValue, null, 2)}`);
            },
        });

        // Run for specified duration
        await new Promise((res) => setTimeout(res, duration));

        console.log(`\n✅ Received ${messageCount} messages from ${topic}`);
    } catch (error: any) {
        console.error('\n❌ Consumer error:', error.message);
    } finally {
        await consumer.disconnect();
    }
}

// List all topics
async function listTopics() {
    console.log('\n📋 Fetching topics from Kafka...\n');

    const admin = kafka.admin();

    try {
        await admin.connect();
        console.log('✅ Admin connected\n');

        const topics = await admin.listTopics();

        console.log('═'.repeat(60));
        console.log('📋 AVAILABLE TOPICS');
        console.log('═'.repeat(60));
        topics.forEach((topic, index) => {
            const isConfigured = [
                PAYMENT_TOPIC,
                DRIVER_LOCATION_TOPIC,
                RIDE_REQUEST_TOPIC,
                USER_REQUEST_TOPIC,
                USER_RESPONSE_TOPIC
            ].includes(topic);

            console.log(`${index + 1}. ${topic} ${isConfigured ? '✅' : ''}`);
        });
        console.log('═'.repeat(60));
        console.log(`Total: ${topics.length} topics\n`);

        console.log('📊 Fetching topic details...\n');
        const metadata = await admin.fetchTopicMetadata({ topics });

        metadata.topics.forEach((topic) => {
            console.log(`\n📌 ${topic.name}:`);
            console.log(`   Partitions: ${topic.partitions.length}`);
            topic.partitions.forEach((p) => {
                console.log(`   └─ Partition ${p.partitionId}: Leader ${p.leader}, Replicas: ${p.replicas.length}`);
            });
        });
    } catch (error: any) {
        console.error('❌ Error listing topics:', error.message);
    } finally {
        await admin.disconnect();
    }
}

// Main execution
async function main() {
    const args = process.argv.slice(2);
    const command = args[0] || 'send';

    try {
        switch (command) {
            case 'send':
                const count = Number(args[1]) || 5;
                const interval = Number(args[2]) || 2000;
                await testAllTopics(count, interval);
                break;

            case 'consume':
                const topic = args[1] || PAYMENT_TOPIC;
                const duration = Number(args[2]) || 30000;
                await testConsumer(topic, duration);
                break;

            case 'list':
                await listTopics();
                break;

            case 'help':
            default:
                console.log(`
Usage:
  bun run test-kafka.ts send [count] [interval]      # Send test messages
  bun run test-kafka.ts consume [topic] [duration]   # Consume messages
  bun run test-kafka.ts list                         # List all topics

Examples:
  bun run test-kafka.ts send 10 1000                 # Send 10 batches, 1s interval
  bun run test-kafka.ts consume payment.events 30000 # Listen for 30 seconds
  bun run test-kafka.ts list                         # List all topics

Current Configuration:
  Broker: ${KAFKA_BROKER}
  Topics:
    - ${PAYMENT_TOPIC}
    - ${DRIVER_LOCATION_TOPIC}
    - ${RIDE_REQUEST_TOPIC}
    - ${USER_REQUEST_TOPIC}
    - ${USER_RESPONSE_TOPIC}
                `);
        }
    } catch (error: any) {
        console.error('\n❌ Fatal Error:', error.message);
        console.error('Stack:', error.stack);
        process.exit(1);
    }

    console.log('\n✨ Done!\n');
    process.exit(0);
}

main();