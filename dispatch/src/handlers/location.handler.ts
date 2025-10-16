import { redis } from "../infrastructure/redis";
import { logger } from "../logger";

export async function handleDriverLocation(data: any): Promise<void> {
    try {
        const key = `driver:${data.driverId}:location`;

        await redis.hset(
            key,
            "lat", String(data.lat),
            "lng", String(data.lng),
            "ts", String(data.timestamp || Date.now())
        );

        await redis.expire(key, 120);

        logger.debug({
            driverId: data.driverId,
            lat: data.lat,
            lng: data.lng
        }, "Driver location updated");

    } catch (err: any) {
        logger.error({ error: err }, " Failed to update driver location");
        throw err;
    }
}