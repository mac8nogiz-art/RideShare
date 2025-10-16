import { Kafka } from "kafkajs";
import { config } from "../config";
import { logger } from "../logger";

const kafka = new Kafka({
    clientId: config.serviceName,
    brokers: config.kafkaBrokers,
});

export const producer = kafka.producer();

export const connectKafka = async () => {
    await producer.connect();
    logger.info(`✅ Kafka connected at ${config.kafkaBrokers.join(",")}`);
};
