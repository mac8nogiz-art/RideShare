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


        logger.debug(
            `Driver location updated - ID: ${data.driverId}, Lat: ${data.lat}, Lng: ${data.lng}`
        );

    } catch (err: any) {

        logger.error(`Failed to update driver location: ${err.message || err}`);
        throw err;
    }
}