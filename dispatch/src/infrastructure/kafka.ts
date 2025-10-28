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
    sessionTimeout: 30000,
    heartbeatInterval: 10000,
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

export const sendKafkaMessage = async (topic: string, key: string, value: any): Promise<boolean> => {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {

            const connected = await ensureKafkaConnection();
            if (!connected) {
                throw new Error('Kafka not connected');
            }

            await producer.send({
                topic,
                messages: [{
                    key,
                    value: JSON.stringify(value),
                    headers: {
                        'attempt': attempt.toString(),
                        'timestamp': Date.now().toString()
                    }
                }]
            });

            logger.debug(`Kafka message sent - Topic: ${topic}, Key: ${key}, Attempt: ${attempt}`);
            return true;

        } catch (error: any) {
            logger.warn(`Kafka send failed (Attempt ${attempt}/${maxRetries}) - Topic: ${topic}, Error: ${error.message}`);

            if (attempt === maxRetries) {
                logger.error(`Failed to send Kafka message after ${maxRetries} attempts: ${error.message}`);
                return false;
            }

            // Wait before retry with exponential backoff
            const backoffTime = Math.min(200 * Math.pow(2, attempt - 1), 2000);
            await new Promise(resolve => setTimeout(resolve, backoffTime));
        }
    }

    return false;
};

export const disconnectKafka = async (): Promise<void> => {
    try {
        await Promise.allSettled([
            producer.disconnect(),
            consumer.disconnect(),
            admin.disconnect()
        ]);
        isConnected = false;
        logger.info('Kafka disconnected');
    } catch (error: any) {
        logger.error(`Kafka disconnection error: ${error.message}`);
    }
};

export const isKafkaConnected = (): boolean => isConnected;

// Legacy functions for backward compatibility
export const checkProducerHealth = ensureKafkaConnection;
export const reconnectKafka = connectKafka;