import Redis from 'ioredis';

export default class RedisPublisher {
    private client: Redis | null = null;

    async connect() {
        this.client = new Redis({
            host: process.env.REDIS_HOST || 'localhost',
            port: Number(process.env.REDIS_PORT) || 6379,
            password: process.env.REDIS_PASSWORD || undefined,
        });

        return this;
    }

    async publish(channel: string, message: any) {
        if (!this.client) throw new Error('Not connected');

        const payload = typeof message === 'object' ? JSON.stringify(message) : String(message);
        return await this.client.publish(channel, payload);
    }

    async disconnect() {
        await this.client?.quit();
    }
}
