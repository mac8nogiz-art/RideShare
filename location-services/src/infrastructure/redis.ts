import Redis from "ioredis";
import { config } from "../config";
import { logger } from "../logger";

export const redis = new Redis(config.redisUrl);

redis.on("connect", () => logger.info("Redis connected successfully"));

redis.on("error", (err: Error) => {
    logger.error({ err }, " Redis connection error");
});
