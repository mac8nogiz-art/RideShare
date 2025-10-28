import Redis from "ioredis";
import { logger } from "../logger";
import { loadLuaScripts } from "../infrastructure/lualoader";

const redisHost = process.env.REDIS_HOST || "redis";
const redisPort = Number(process.env.REDIS_PORT || 6379);

export const redis = new Redis({
    host: redisHost,
    port: redisPort,
    username: process.env.REDIS_USERNAME || undefined,
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_DB || 0),
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    lazyConnect: false,
});

redis.on("connect", () => {
    logger.info(" Connected to Redis successfully");
    loadLuaScripts(redis);
});

redis.on("error", (err) => {
  console.error("Redis connection error:", err);
});