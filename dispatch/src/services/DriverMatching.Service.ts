import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from "./ZoneService";
import {SpatialService} from "../infrastructure/spatial";


export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;

    private readonly DEFAULT_RADIUS_STEPS = [3, 5, 10, 15];
    private readonly DEFAULT_STALE_THRESHOLD = 300_000;

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
    }

    async findBestDrivers(job: Job, customerId: string, maxDrivers: number = 10): Promise<string[]> {
        const start = Date.now();

        try {
            // 1. Determine zone for this job
            const zone = await this.zoneService.getZoneForJob(job);
            if (!zone) {
                logger.warn(`No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                return [];
            }

            logger.info(`Job ${job.id} is in zone: ${zone.name}`);

            // 2. Get customer favorites
            const customerFavorites = await this.getCustomerFavorites(customerId);

            // 3. Filter drivers by zone approval and proximity
            const eligibleDrivers = await this.getEligibleDrivers(job, zone._id);

            if (eligibleDrivers.length === 0) {
                logger.warn(`No eligible drivers found in zone ${zone.name} for job ${job.id}`);
                return [];
            }

            // 4. Prioritize and select top drivers
            const prioritizedDrivers = this.prioritizeDrivers(eligibleDrivers, customerFavorites);
            const selectedDrivers = prioritizedDrivers.slice(0, maxDrivers).map(d => d.driverId);

            // 5. Cache results
            await redis.set(`job:${job.id}:matched_drivers`, JSON.stringify(selectedDrivers));
            await redis.set(`job:${job.id}:zone`, zone._id);

            logger.info(`Matched ${selectedDrivers.length} drivers in zone ${zone.name} for job ${job.id} in ${Date.now() - start}ms`);
            return selectedDrivers;

        } catch (err) {
            logger.error(`DriverMatchingService failed for job ${job.id}: ${err}`);
            return [];
        }
    }

    private async getEligibleDrivers(job: Job, zoneId: string): Promise<DriverWithDistance[]> {
        const allDrivers = Array.from(this.driverLocationService.getAllDrivers().values());
        const eligibleDrivers: DriverWithDistance[] = [];

        for (const driver of allDrivers) {
            // Skip stale or busy drivers
            if (Date.now() - driver.lastUpdate > this.DEFAULT_STALE_THRESHOLD) continue;
            if (driver.isBusy) continue;

            // Check if driver is approved for this zone
            const isApproved = await this.zoneService.isDriverApprovedForZone(driver.driverId, zoneId);
            if (!isApproved) continue;

            // Calculate distance
            const distance = this.spatialService.calculateDistance(
                job.pickupLat, job.pickupLng, driver.lat, driver.lng
            );

            // Check if within maximum radius
            if (distance <= 15) { // 15km max radius
                eligibleDrivers.push({
                    ...driver,
                    distance,
                    priority: 0
                });
            }
        }

        return eligibleDrivers;
    }

    private prioritizeDrivers(drivers: DriverWithDistance[], customerFavorites: Set<string>): DriverWithDistance[] {
        return drivers
            .map(driver => ({
                ...driver,
                priority: this.calculateDriverPriority(driver, customerFavorites.has(driver.driverId))
            }))
            .sort((a, b) => b.priority - a.priority || a.distance - b.distance);
    }

    private calculateDriverPriority(driver: DriverWithDistance, isFavorite: boolean): number {
        let priority = 0;

        // Base priority based on distance
        if (isFavorite && driver.distance <= 3) priority = 1000 + driver.score;
        else if (driver.distance <= 3) priority = 900 + driver.score;
        else if (driver.distance <= 5) priority = 700 + driver.score;
        else if (driver.distance <= 10) priority = 500 - driver.distance;
        else priority = 400 - driver.distance;

        // Bonuses
        if (driver.isNew) priority += 50;
        if (isFavorite) priority += 100;

        return priority;
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
}