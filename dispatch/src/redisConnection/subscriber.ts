import Redis from 'ioredis';

type Handler = (message: any, channel: string) => void | Promise<void>;
type ExpiryHandler = (expiredKey: string) => void | Promise<void>;

export default class RedisSubscriber {
    private client: Redis | null = null;
    private handlers = new Map<string, Handler>();
    private expiryHandlers: ExpiryHandler[] = [];

    async connect() {
        if (this.client) return this; // already connected

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
