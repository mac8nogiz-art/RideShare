import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'payment-test-client',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-test-client-group' });

const MAX_HITS = 200;

let responseCount = 0;
let successCount = 0;
let failureCount = 0;

async function sendPaymentEvent(jobId: string, customerId: string, pickupLat: number, pickupLng: number, fare: number, vehicleType: string) {
    const correlationId = `corr_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
    const requestTopic = 'dispatch-service.request';
    const responseTopic = 'dispatch-service.response';

    const event = {
        type: 'payment.completed',
        jobId,
        customerId,
        pickupLat,
        pickupLng,
        fare,
        vehicleType,
        timestamp: Date.now(),
        targetService: 'dispatch-service',
        source: 'payment-service',
        correlationId
    };

    await producer.send({
        topic: requestTopic,
        messages: [
            {
                key: jobId,
                value: JSON.stringify(event),
                headers: {
                    correlationId,
                    replyTo: responseTopic,
                    timestamp: Date.now().toString()
                }
            }
        ]
    });

    console.log(`📤 Payment event sent: Job ${jobId} | Customer ${customerId} | Fare ₹${fare}`);
}

async function sendMultiplePayments(count: number = MAX_HITS) {
    for (let i = 0; i < count; i++) {
        const jobId = `job_${Date.now()}_${i}`;
        const customerId = `customer_${100 + i}`;
        const pickupLat = 28.6129 + (Math.random() - 0.5) * 0.05;
        const pickupLng = 77.2295 + (Math.random() - 0.5) * 0.05;
        const fare = 150 + Math.floor(Math.random() * 200);
        const vehicleType = ['sedan', 'hatchback', 'suv'][i % 3];

        await sendPaymentEvent(jobId, customerId, pickupLat, pickupLng, fare, vehicleType);
        await new Promise(r => setTimeout(r, 50)); // small delay between events
    }
}

async function start() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'dispatch-service.response', fromBeginning: false });

    console.log('✅ Kafka producer & consumer connected');

    // Listen for responses
    consumer.run({
        eachMessage: async ({ message }) => {
            const value = message.value?.toString();
            const response = value ? JSON.parse(value) : {};
            responseCount++;

            // Determine result
            if (response.success && response.message?.includes('Driver assigned')) {
                successCount++;
                console.log(`✅ Payment Successful & Driver Assigned #${responseCount}: Job ${response.jobId}`);
            } else {
                failureCount++;
                console.log(`❌ Payment Failed / No Driver Assigned #${responseCount}: Job ${response.jobId || 'unknown'}`);
            }

            // Stop after MAX_HITS
            if (responseCount >= MAX_HITS) {
                console.log(`\n📊 Summary after ${MAX_HITS} events:`);
                console.log(`✅ Payment & Driver Assigned: ${successCount}`);
                console.log(`❌ Payment Failed / No Driver Assigned: ${failureCount}`);

                await producer.disconnect();
                await consumer.disconnect();
                process.exit(0);
            }
        }
    });

    // Send multiple payment events
    await sendMultiplePayments(MAX_HITS);
}

start().catch(err => {
    console.error('❌ Error running test:', err);
    process.exit(1);
});
