import { Kafka } from "kafkajs";
import dotenv from "dotenv";
dotenv.config();

const kafka = new Kafka({
    clientId: process.env.SERVICE_NAME,
    brokers: [process.env.KAFKA_BROKER!],
});

// Existing producer/consumer
export const producer = kafka.producer();
export const consumer = kafka.consumer({
    groupId: `${process.env.SERVICE_NAME}-group`,
});

// ✅ New: Kafka admin instance
export const admin = kafka.admin();

export const connectKafka = async () => {
    await producer.connect();
    await consumer.connect();
    await admin.connect(); // <-- connect admin too
    console.log(`✅ Kafka connected for ${process.env.SERVICE_NAME}`);
};
