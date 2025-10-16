import { Kafka } from 'kafkajs';
import { config } from 'dotenv';
config();

const broker =
    process.env.KAFKA_BROKER ||
    (process.env.DOCKER_ENV ? 'auth-kafka:9092' : 'localhost:9092');

export const kafka = new Kafka({
    clientId: process.env.SERVICE_NAME || 'auth-service',
    brokers: [broker],
});

export const producer = kafka.producer();
export const consumer = kafka.consumer({
    groupId: `${process.env.SERVICE_NAME}-group`,
});

export const connectKafka = async () => {
    await producer.connect();
    await consumer.connect();
    console.log(`✅ Kafka connected for ${process.env.SERVICE_NAME}`);
};
