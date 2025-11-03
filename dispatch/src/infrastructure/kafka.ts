import { Kafka, Producer, Consumer, Admin } from "kafkajs";
import dotenv from "dotenv";
import { logger } from "../logger";

dotenv.config();

const kafka = new Kafka({
    clientId: process.env.SERVICE_NAME,
    brokers: [process.env.KAFKA_BROKER!],
    retry: {
        initialRetryTime: 100,
        retries: 10,
        maxRetryTime: 30000,
    },
    connectionTimeout: 15000,
    requestTimeout: 30000,
});

export const producer: Producer = kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 10000,
    idempotent: true,
    retry: {
        initialRetryTime: 100,
        retries: 8,
        maxRetryTime: 10000
    }
});

export const consumer: Consumer = kafka.consumer({
    groupId: `${process.env.SERVICE_NAME}-group`,
    sessionTimeout: 60000,
    heartbeatInterval: 3000,
    retry: {
        initialRetryTime: 100,
        retries: 5
    }
});

export const admin: Admin = kafka.admin();

let isConnected = false;
let connectionPromise: Promise<boolean> | null = null;
let lastConnectionAttempt = 0;
const CONNECTION_COOLDOWN = 5000; // 5 seconds

export const connectKafka = async (): Promise<boolean> => {

    if (connectionPromise) {
        return connectionPromise;
    }


    const now = Date.now();
    if (now - lastConnectionAttempt < CONNECTION_COOLDOWN) {
        logger.warn('Connection attempts too frequent, waiting...');
        await new Promise(resolve => setTimeout(resolve, CONNECTION_COOLDOWN));
    }

    connectionPromise = (async (): Promise<boolean> => {
        try {
            lastConnectionAttempt = Date.now();
            logger.info('Connecting to Kafka...');

            await Promise.all([
                producer.connect(),
                consumer.connect(),
                admin.connect()
            ]);

            isConnected = true;
            logger.info('Kafka connected successfully');
            return true;
        } catch (error: any) {
            isConnected = false;
            logger.error(`Kafka connection failed: ${error.message}`);
            return false;
        } finally {
            connectionPromise = null;
        }
    })();

    return connectionPromise;
};


export const ensureKafkaConnection = async (): Promise<boolean> => {
    if (isConnected) {
        try {

            await producer.send({
                topic: 'health-check',
                messages: [{ value: JSON.stringify({ check: Date.now() }) }]
            });
            return true;
        } catch (error) {
            logger.warn('Producer health check failed, reconnecting...');
            isConnected = false;
        }
    }

    return await connectKafka();
};





export const isKafkaConnected = (): boolean => isConnected;

// Legacy functions for backward compatibility
export const checkProducerHealth = ensureKafkaConnection;
export const reconnectKafka = connectKafka;