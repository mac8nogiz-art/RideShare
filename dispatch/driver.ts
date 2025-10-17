// test-drivers.ts - Update to include profiles
import { redis } from "./src/infrastructure/redis";
import { logger } from "./src/logger";

const drivers = [
    { id: "driver_101", lat: 28.6139, lng: 77.2090, score: 85, isNew: true },
    { id: "driver_102", lat: 28.6145, lng: 77.2080, score: 75, isNew: false },
    { id: "driver_103", lat: 28.6150, lng: 77.2070, score: 90, isNew: false },
    { id: "driver_01", lat: 28.6139, lng: 77.2090, score: 85, isNew: true },
    { id: "driver_02", lat: 28.6145, lng: 77.2080, score: 75, isNew: false },
    { id: "driver_03", lat: 28.6150, lng: 77.2070, score: 90, isNew: false },
    { id: "driver_11", lat: 28.6139, lng: 77.2090, score: 85, isNew: true },
    { id: "driver_12", lat: 28.6145, lng: 77.2080, score: 75, isNew: false },
    { id: "driver_13", lat: 28.6150, lng: 77.2070, score: 90, isNew: false },
    { id: "driver_11", lat: 28.6139, lng: 77.2090, score: 85, isNew: true },
    { id: "driver_12", lat: 28.6145, lng: 77.2080, score: 75, isNew: false },
    { id: "driver_3", lat: 28.6150, lng: 77.2070, score: 90, isNew: false },
    { id: "driver_1", lat: 28.6139, lng: 77.2090, score: 85, isNew: true },
    { id: "driver_2", lat: 28.6145, lng: 77.2080, score: 75, isNew: false },
    { id: "driver_10223", lat: 28.6150, lng: 77.2070, score: 90, isNew: false },
];

async function updateDriverLocations() {
    try {
        for (const driver of drivers) {
            // Update location
            const locationKey = `driver:${driver.id}:location`;
            await redis.hset(
                locationKey,
                "lat", String(driver.lat),
                "lng", String(driver.lng),
                "ts", String(Date.now())
            );
            await redis.expire(locationKey, 120);

            // Update profile (important for matching)
            const profileKey = `driver:${driver.id}:profile`;
            await redis.hset(
                profileKey,
                "score", String(driver.score),
                "isFavorite", "false",
                "isBusy", "false",
                "isNew", String(driver.isNew),
                "approvedDate", new Date().toISOString()
            );

            logger.info(`Driver ${driver.id} updated - location and profile`);
        }
    } catch (err: any) {
        logger.error({ err }, "Failed to update driver locations");
    }
}

// Run every 5 seconds
setInterval(updateDriverLocations, 5000);
updateDriverLocations();