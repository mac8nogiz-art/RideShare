import { Elysia } from "elysia";
import { producer } from "../infrastructure/kafka";
import { redis } from "../infrastructure/redis";
import { logger } from "../logger";

function getTopic(payload: any): string {
    if (payload.city) return `driver.location.${payload.city.toLowerCase()}`;
    return `driver.location.global`;
}

export const driverWS = new Elysia({ prefix: "/driver" })
    .ws("/location", {
        open(ws) {
            logger.info("Driver connected");
        },

        async message(ws, msg) {
            try {
                const payload = msg as Record<string, any>;
                if (!payload.driverId || !payload.lat || !payload.lng) return;

                payload.timestamp = Date.now();
                const topic = getTopic(payload);

                await redis.hset(
                    "driver_location",
                    payload.driverId,
                    JSON.stringify(payload)
                );

                await producer.send({
                    topic,
                    messages: [{ key: payload.driverId, value: JSON.stringify(payload) }],
                });

                logger.info(` Sent driver ${payload.driverId} → ${topic}`);
            } catch (err) {
                logger.error(err);
            }
        },

        close() {
            logger.info("🔌 Driver disconnected");
        },
    });
