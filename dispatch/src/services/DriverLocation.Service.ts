// src/services/DriverLocationService.ts
import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Driver } from '../types';

export class DriverLocationService {
    private driverCache = new Map<string, Driver>();
    private profileCache = new Map<string, any>();
    private locationRefreshInterval: NodeJS.Timeout | null = null;
    private profileRefreshInterval: NodeJS.Timeout | null = null;

    async startDriverCacheRefresh(): Promise<void> {
        logger.info('Starting Driver Cache Refresh - Location: 2s, Profile: 30s');

        // Initial refresh
        await this.refreshDriverCache();
        await this.refreshProfileCache();

        // Refresh locations every 2s
        this.locationRefreshInterval = setInterval(async () => {
            await this.refreshDriverCache();
        }, 2000);

        // Refresh profiles every 30s
        this.profileRefreshInterval = setInterval(async () => {
            await this.refreshProfileCache();
        }, 30000);
    }

    private async refreshDriverCache(): Promise<void> {
        const startTime = Date.now();

        try {
            const keys = await redis.keys('driver:*:location');
            if (keys.length === 0) return;

            const pipeline = redis.pipeline();
            keys.forEach(key => pipeline.hgetall(key));
            const results = await pipeline.exec();

            const newCache = new Map<string, Driver>();

            results?.forEach((result, index) => {
                if (result[0]) return;

                const location = result[1] as any;
                const driverId = keys[index].split(':')[1];

                if (location?.lat && location?.lng) {
                    const profile = this.profileCache.get(driverId) || {};

                    const driver: Driver = {
                        driverId,
                        lat: parseFloat(location.lat),
                        lng: parseFloat(location.lng),
                        score: parseFloat(profile?.score || "0"),
                        isFavorite: profile?.isFavorite === "true",
                        isBusy: profile?.isBusy === "true",
                        isNew: this.isNewDriver(profile?.approvedDate || ""),
                        lastUpdate: parseInt(location.ts || "0")
                    };

                    newCache.set(driverId, driver);

                    // FIX: Use proper geospatial update
                    this.updateDriverGeolocation(driverId, driver.lat, driver.lng)
                        .catch(err => logger.error(`GeoAdd Error - Driver: ${driverId}, Error: ${err}`));
                }
            });

            this.driverCache = newCache;
            logger.debug(`Driver Cache Refreshed - Drivers: ${newCache.size}, Time: ${Date.now() - startTime}ms`);
        } catch (error) {
            logger.error(`Cache Refresh Error: ${error}`);
        }
    }

    private async updateDriverGeolocation(driverId: string, lat: number, lng: number): Promise<void> {
        try {
            // FIX: Ensure geospatial index exists and update it
            await redis.geoadd('drivers:locations', lng, lat, driverId);
        } catch (error) {
            // If geospatial fails, log but don't break the flow
            logger.warn(`Geospatial update failed for driver ${driverId}, using fallback`);
        }
    }

    private async refreshProfileCache(): Promise<void> {
        const startTime = Date.now();
        try {
            const keys = await redis.keys('driver:*:profile');
            if (keys.length === 0) return;

            const pipeline = redis.pipeline();
            keys.forEach(key => pipeline.hgetall(key));
            const results = await pipeline.exec();

            const newProfileCache = new Map<string, any>();

            results?.forEach((result, index) => {
                if (result[0]) return;
                const profile = result[1];
                const driverId = keys[index].split(':')[1];
                newProfileCache.set(driverId, profile);
            });

            this.profileCache = newProfileCache;
            logger.debug(`Profile Cache Refreshed - Profiles: ${newProfileCache.size}, Time: ${Date.now() - startTime}ms`);
        } catch (error) {
            logger.error(`Profile Cache Refresh Error: ${error}`);
        }
    }

    getAllDrivers(): Map<string, Driver> {
        return this.driverCache;
    }

    getDriver(driverId: string): Driver | undefined {
        return this.driverCache.get(driverId);
    }

    getDriverCacheSize(): number {
        return this.driverCache.size;
    }

    private isNewDriver(approvedDate: string): boolean {
        if (!approvedDate) return false;
        const approved = new Date(approvedDate);
        return (Date.now() - approved.getTime()) <= (30 * 24 * 60 * 60 * 1000);
    }

    stop(): void {
        if (this.locationRefreshInterval) clearInterval(this.locationRefreshInterval);
        if (this.profileRefreshInterval) clearInterval(this.profileRefreshInterval);
    }
}