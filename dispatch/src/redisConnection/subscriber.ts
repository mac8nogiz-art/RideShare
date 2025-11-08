// infrastructure/redisSubscriber.ts
import Redis from 'ioredis';
import { logger } from '../logger';

type Handler = (message: any, channel: string) => void | Promise<void>;
type ExpiryHandler = (expiredKey: string) => void | Promise<void>;

class RedisSubscriber {
    private client: Redis | null = null;
    private handlers = new Map<string, Handler>();
    private expiryHandlers: ExpiryHandler[] = [];

    async connect() {
        if (this.client) return this;

        this.client = new Redis({
            host: process.env.REDIS_HOST || 'localhost',
            port: Number(process.env.REDIS_PORT) || 6379,
            password: process.env.REDIS_PASSWORD || undefined,
        });

        this.client.on("message", (channel: string, message: string) => {
            const handler = this.handlers.get(channel);
            if (handler) {
                try {
                    handler(JSON.parse(message), channel);
                } catch {
                    handler(message, channel);
                }
            }
        });

        this.client.on("pmessage", (pattern: string, channel: string, message: string) => {
            const handler = this.handlers.get(pattern);

            if (handler) {
                try {
                    handler(JSON.parse(message), channel);
                } catch {
                    handler(message, channel);
                }
            }

            if (pattern.includes("expired")) {
                this.expiryHandlers.forEach(fn => fn(message));
            }
        });

        this.client.on('error', (err) => {
            logger.error('Redis subscriber error:', err);
        });

        this.client.on('ready', () => {
            logger.info('Redis subscriber ready');
        });

        return this;
    }

    isConnected(): boolean {
        return this.client !== null;
    }

    async subscribe(channel: string, handler: Handler) {
        if (!this.client) throw new Error("Not connected");

        this.handlers.set(channel, handler);
        await this.client.subscribe(channel);
    }

    async subscribePattern(pattern: string, handler: Handler) {
        if (!this.client) throw new Error("Not connected");

        this.handlers.set(pattern, handler);
        await this.client.psubscribe(pattern);
    }

    addExpiryHandler(handler: ExpiryHandler) {
        this.expiryHandlers.push(handler);
    }

    async unsubscribe(channel: string) {
        if (!this.client) return;

        await this.client.unsubscribe(channel);
        this.handlers.delete(channel);
    }

    async disconnect() {
        await this.client?.quit();
        this.client = null;
    }
}

// Export singleton instance
export const redisSubscriber = new RedisSubscriber();

// Export function to connect Redis subscriber
export async function connectRedisSubscriber() {
    await redisSubscriber.connect();

    // Subscribe to keyspace notifications for expiry events
    // Make sure Redis is configured with: CONFIG SET notify-keyspace-events Ex
    await redisSubscriber.subscribePattern('__keyevent@0__:expired', (message, channel) => {
        logger.debug(`Key expired: ${message}`);
    });

    logger.info('✅ Redis subscriber connected successfully');
}

// Export function to check if subscriber is connected
export function isRedisSubscriberConnected(): boolean {
    return redisSubscriber.isConnected();
}