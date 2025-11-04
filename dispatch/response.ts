const { Kafka } = require('kafkajs');

// Kafka Configuration
const KAFKA_BROKER = '172.105.61.99:9093';
const TOPIC_NAME = 'newBooking.response';

// Test Data
const TEST_DRIVER_ID = '"68b920ac35e816497cf07994"';
const TEST_JOB_ID = '6900561da9ea6f1301d29aba';

// Initialize Kafka
const kafka = new Kafka({
    clientId: 'driver-response-test',
    brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

// Driver Accept Event
const driverAcceptEvent = {
    type: "newBooking.response",
    driverId: TEST_DRIVER_ID,
    jobId: TEST_JOB_ID,
    action: "accept",
    timestamp: new Date().toISOString()
};

// Send Event
async function sendDriverAccept() {
    try {
        console.log('🔌 Connecting to Kafka...');
        await producer.connect();
        console.log('✅ Connected');

        console.log('\n📤 Sending driver accept event...');
        console.log('Driver:', TEST_DRIVER_ID);
        console.log('Job:', TEST_JOB_ID);

        const result = await producer.send({
            topic: TOPIC_NAME,
            messages: [{
                key: TEST_DRIVER_ID,
                value: JSON.stringify(driverAcceptEvent),
                headers: {
                    'event-type': 'newBooking.response',
                    'job-id': TEST_JOB_ID,
                    'driver-id': TEST_DRIVER_ID,
                    'action': 'accept',
                    'timestamp': driverAcceptEvent.timestamp
                }
            }]
        });

        console.log('\n✅ Event sent successfully!');
        console.log('Result:', JSON.stringify(result, null, 2));

    } catch (error) {
        console.error('❌ Error:', error.message);
        throw error;
    } finally {
        await producer.disconnect();
        console.log('\n🔌 Disconnected\n');
    }
}

// Execute
sendDriverAccept()
    .then(() => {
        console.log('✨ Done!');
        process.exit(0);
    })
    .catch((error) => {
        console.error('💥 Failed:', error.message);
        process.exit(1);
    });