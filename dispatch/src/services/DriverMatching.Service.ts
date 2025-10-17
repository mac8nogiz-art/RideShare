// src/services/DriverMatching.Service.ts
import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { DriverWithDistance, Job } from '../types';
import { DriverLocationService } from './DriverLocation.Service';

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;

    constructor(driverLocationService: DriverLocationService) {
        this.driverLocationService = driverLocationService;
    }


    async findBestDrivers(job: Job, customerId: string): Promise<string[]> {
        const start = Date.now();
        const nearby: DriverWithDistance[] = [];

        try {

            const allDrivers = Array.from(this.driverLocationService.getAllDrivers().values());

            const now = Date.now();
            for (const driver of allDrivers) {
                // Skip stale drivers
                if (now - driver.lastUpdate > 300000) continue;

                const distance = this.calculateDistance(job.pickupLat, job.pickupLng, driver.lat, driver.lng);
                if (distance > 10) continue;

                nearby.push({
                    driverId: driver.driverId,
                    lat: driver.lat,
                    lng: driver.lng,
                    distance,
                    score: driver.score,
                    isBusy: driver.isBusy,
                    isNew: driver.isNew,
                    lastUpdate: driver.lastUpdate,
                    priority: 0,
                    isFavorite: driver.isFavorite,
                });
            }

            if (nearby.length === 0) {
                logger.warn(` No nearby drivers found for job ${job.id}`);
                return [];
            }


            const favorites = await this.getCustomerFavorites(customerId);


            const sorted = nearby
                .map((d) => ({
                    driverId: d.driverId,
                    priority: this.calculateDriverPriority(d, favorites.has(d.driverId)),
                    distance: d.distance,
                }))
                .filter((d) => d.priority > 0)
                .sort((a, b) =>
                    b.priority !== a.priority ? b.priority - a.priority : a.distance - b.distance
                )
                .slice(0, 5)
                .map((d) => d.driverId);

            logger.info(
                ` Matched ${sorted.length} drivers for job ${job.id} in ${Date.now() - start}ms`
            );

            await redis.set(`job:${job.id}:matched_drivers`, JSON.stringify(sorted));

            return sorted;
        } catch (err) {
            logger.error(`DriverMatchingService failed for job ${job.id}: ${err}`);
            return [];
        }
    }

    private async getCustomerFavorites(customerId: string): Promise<Set<string>> {
        try {
            const favs = await redis.smembers(`customer:${customerId}:favorites`);
            return new Set(favs);
        } catch (err) {
            logger.warn(`Failed to fetch favorites for customer ${customerId}: ${err}`);
            return new Set();
        }
    }

    private calculateDriverPriority(driver: DriverWithDistance, isFavorite: boolean): number {
        let priority = 0;

        if (isFavorite && driver.distance <= 3) priority = 1000 + driver.score;
        else if (!driver.isBusy && driver.distance <= 3) priority = 900 + driver.score;
        else if (!driver.isBusy && driver.distance <= 5) priority = 700 + driver.score;
        else if (driver.distance <= 10) priority = 400 - driver.distance;

        if (driver.isNew) priority += 10;

        return priority;
    }

    private calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
        const R = 6371;
        const dLat = ((lat2 - lat1) * Math.PI) / 180;
        const dLng = ((lng2 - lng1) * Math.PI) / 180;
        const a =
            Math.sin(dLat / 2) ** 2 +
            Math.cos((lat1 * Math.PI) / 180) *
            Math.cos((lat2 * Math.PI) / 180) *
            Math.sin(dLng / 2) ** 2;

        return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }
}
