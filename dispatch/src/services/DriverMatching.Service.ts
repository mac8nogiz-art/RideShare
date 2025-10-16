// src/services/DriverMatchingService.ts
import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Driver, DriverWithDistance, Job } from '../types';
import { DriverLocationService } from './DriverLocation.Service'; // FIX: Correct import

export class DriverMatchingService {
    constructor(private driverLocationService: DriverLocationService) {}

    async findBestDrivers(job: Job, customerId: string): Promise<string[]> {
        const startTime = Date.now();

        try {
            // FIX: Use fallback method first - geospatial might not be ready
            const nearbyDrivers = this.getNearbyDriversFallback(job.pickupLat, job.pickupLng, 15);

            if (nearbyDrivers.length === 0) {
                logger.warn(`No drivers found for job ${job.id}`);
                return [];
            }

            // Get favorites in one call
            const favoriteDriverIds = await this.getCustomerFavorites(customerId);

            // Calculate priorities
            const driversWithPriority = nearbyDrivers.map(driver => ({
                driverId: driver.driverId,
                priority: this.calculateDriverPriority(driver, favoriteDriverIds.has(driver.driverId)),
                distance: driver.distance
            }));

            // Filter and sort
            const matches = driversWithPriority
                .filter(driver => driver.priority > 0)
                .sort((a, b) => {
                    if (b.priority !== a.priority) return b.priority - a.priority;
                    return a.distance - b.distance;
                })
                .slice(0, 10)
                .map(driver => driver.driverId);

            const matchingTime = Date.now() - startTime;
            logger.debug(`Driver Matching - JobId: ${job.id}, Matches: ${matches.length}, Time: ${matchingTime}ms`);

            return matches;
        } catch (error) {
            logger.error(`Driver Matching Error - JobId: ${job.id}, Error: ${error}`);
            return [];
        }
    }

    private getNearbyDriversFallback(jobLat: number, jobLng: number, radiusKm: number): DriverWithDistance[] {
        const nearbyDrivers: DriverWithDistance[] = [];
        const now = Date.now();
        const allDrivers = this.driverLocationService.getAllDrivers();

        // FIX: Check if driver cache is populated
        if (allDrivers.size === 0) {
            logger.warn('Driver cache is empty - no drivers available for matching');
            return [];
        }

        for (const [driverId, driver] of allDrivers.entries()) {
            // Skip stale drivers (5 minutes instead of 30 seconds for testing)
            if (now - driver.lastUpdate > 300000) {
                logger.debug(`Skipping stale driver: ${driverId}`);
                continue;
            }

            const distance = this.calculateDistance(jobLat, jobLng, driver.lat, driver.lng);

            if (distance <= radiusKm) {
                nearbyDrivers.push({
                    ...driver,
                    distance,
                    priority: 0
                });
            }
        }

        logger.debug(`Found ${nearbyDrivers.length} drivers within ${radiusKm}km`);
        return nearbyDrivers;
    }

    private async getCustomerFavorites(customerId: string): Promise<Set<string>> {
        try {
            const favorites = await redis.smembers(`customer:${customerId}:favorites`);
            return new Set(favorites);
        } catch (error) {
            logger.error(`Get Favorites Error - Customer: ${customerId}, Error: ${error}`);
            return new Set();
        }
    }

    private calculateDriverPriority(driver: DriverWithDistance, isFavorite: boolean): number {
        let priority = 0;

        // Your exact priority logic:
        if (isFavorite && driver.distance <= 3) {
            priority = 1000 + driver.score;
        } else if (driver.score >= 80 && driver.distance <= 3) {
            priority = 900 + driver.score;
        } else if (driver.isNew && driver.distance <= 3) {
            priority = 800;
        } else if (driver.score >= 60 && driver.distance <= 3) {
            priority = 700 + driver.score;
        } else if (driver.distance <= 3) {
            priority = 600 - driver.distance;
        } else if (driver.isBusy && driver.distance <= 5) {
            priority = 500 - driver.distance;
        } else if (driver.distance <= 15) {
            const distanceSteps = Math.floor((driver.distance - 3) / 0.5);
            priority = Math.max(100, 400 - (distanceSteps * 10));
        }

        return priority;
    }

    private calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
        const R = 6371;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLng = (lng2 - lng1) * Math.PI / 180;
        const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }
}