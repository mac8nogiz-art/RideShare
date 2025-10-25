import Redis from "ioredis";
import { logger } from "../logger";
import { loadLuaScripts } from "../infrastructure/lualoader";

// Support both REDIS_HOST and REDIS_SOCKET_HOST for compatibility
const redisHost = process.env.REDIS_HOST || process.env.REDIS_SOCKET_HOST || "redis";
const redisPort = Number(process.env.REDIS_PORT || process.env.REDIS_SOCKET_PORT || 6379);
const redisUsername = process.env.REDIS_USERNAME || "default";
const redisPassword = process.env.REDIS_PASSWORD || "";

logger.info(`Connecting to Redis at ${redisHost}:${redisPort}`);

export const redis = new Redis({
    host: redisHost,
    port: redisPort,
    username: redisUsername,
    password: redisPassword,
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    lazyConnect: false,  // Changed: connect immediately
    connectTimeout: 10000,
    retryStrategy: (times: number) => {
        if (times > 15) {
            logger.error("Max Redis connection retries reached (15 attempts)");
            return null;
        }
        const delay = Math.min(times * 100, 3000);
        logger.warn(`Redis retry attempt ${times}, waiting ${delay}ms...`);
        return delay;
    },
    // Enable TLS for Redis Cloud
    tls: redisHost.includes('redis-cloud.com') || redisHost.includes('redns.redis-cloud.com')
        ? {
            rejectUnauthorized: false,
        }
        : undefined,
    keepAlive: 30000,
    family: 4, // Force IPv4
    enableOfflineQueue: true,  // CHANGED: Allow queuing commands while connecting
    reconnectOnError: (err) => {
        const targetError = 'READONLY';
        if (err.message.includes(targetError)) {
            return true;
        }
        return false;
    }
});

redis.on("connect", () => {
    logger.info("✓ Connected to Redis successfully");
});

redis.on("ready", () => {
    logger.info("✓ Redis client ready");
    // Load Lua scripts AFTER connection is ready
    loadLuaScripts(redis);
});

redis.on("error", (err) => {
    logger.error({
        message: "Redis connection error:",
        error: err.message,
        host: redisHost,
        port: redisPort,
    });
});

redis.on("close", () => {
    logger.warn("Redis connection closed");
});

redis.on("reconnecting", (delay: number) => {
    logger.info(`Reconnecting to Redis in ${delay}ms...`);
});

// Test connection on startup - but wait for ready event
redis.on("ready", async () => {
    try {
        const result = await redis.ping();
        // @ts-ignore
        logger.info("✓ Redis PING successful:", result);
    } catch (err: any) {
        logger.error("Redis PING failed:", err.message);
    }
});