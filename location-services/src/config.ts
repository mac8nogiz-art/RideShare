import { z } from "zod";
import "dotenv/config";

const EnvSchema = z.object({
    SERVICE_NAME: z.string().default("location-service"),
    PORT: z.string().default("5000"),
    KAFKA_BROKERS: z.string(),
    REDIS_URL: z.string(),
    NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
    DRIVER_LOCATION_TOPIC: z.string().default("driver.location"),
});

const env = EnvSchema.parse(process.env);

export const config = {
    serviceName: env.SERVICE_NAME,
    port: Number(env.PORT),
    kafkaBrokers: env.KAFKA_BROKERS.split(","),
    redisUrl: env.REDIS_URL,
    nodeEnv: env.NODE_ENV,
    driverLocationTopic: env.DRIVER_LOCATION_TOPIC,
};
