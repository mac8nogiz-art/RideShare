import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Driver } from '../types';

export class DriverLocationService {
    private driverCache = new Map<string, Driver>();
    private profileCache = new Map<string, any>();
    private locationRefreshInterval: NodeJS.Timeout | null = null;
    private profileRefreshInterval: NodeJS.Timeout | null = null;
    private isInitialized = false;

    async startDriverCacheRefresh(): Promise<void> {
        logger.info('Starting Driver Cache Refresh - Location: 2s, Profile: 30s');

        // Initial load - wait for completion
        await this.refreshProfileCache();
        await this.refreshDriverCache();

        this.isInitialized = true;
        logger.info(`✅ Driver cache initialized with ${this.driverCache.size} drivers`);

        // Start periodic refresh
        this.locationRefreshInterval = setInterval(async () => {
            await this.refreshDriverCache();
        }, 2000);

        this.profileRefreshInterval = setInterval(async () => {
            await this.refreshProfileCache();
        }, 30000);
    }

    private async refreshDriverCache(): Promise<void> {
        const startTime = Date.now();

        try {
            const keys = await redis.keys('driver:*:location');
            if (keys.length === 0) {
                logger.warn('No driver location keys found in Redis');
                return;
            }

            const pipeline = redis.pipeline();
            keys.forEach(key => pipeline.hgetall(key));
            const results = await pipeline.exec();

            const newCache = new Map<string, Driver>();
            const geoUpdates: Array<{driverId: string, lat: number, lng: number}> = [];

            results?.forEach((result, index) => {
                if (result[0]) return;

                const location = result[1] as any;
                const driverId = keys[index].split(':')[1];

                if (location?.lat && location?.lng) {
                    const profile = this.profileCache.get(driverId) || {};

                    const approvedZones = this.parseApprovedZones(profile?.approvedZones);

                    const lat = parseFloat(location.lat);
                    const lng = parseFloat(location.lng);

                    const driver: Driver = {
                        driverId,
                        lat,
                        lng,
                        score: parseFloat(profile?.score || "0"),
                        isFavorite: profile?.isFavorite === "true",
                        isBusy: profile?.isBusy === "true",
                        isNew: this.isNewDriver(profile?.approvedDate || ""),
                        lastUpdate: parseInt(location.lastUpdate || location.ts || "0"),
                        approvedZones: approvedZones
                    };

                    newCache.set(driverId, driver);

                    // Collect geo updates to batch them
                    if (!isNaN(lat) && !isNaN(lng)) {
                        geoUpdates.push({ driverId, lat, lng });
                    }
                }
            });

            // Batch update geospatial index
            if (geoUpdates.length > 0) {
                await this.batchUpdateGeolocation(geoUpdates);
            }

            this.driverCache = newCache;
            logger.debug(`Driver Cache Refreshed - Drivers: ${newCache.size}, Geo updates: ${geoUpdates.length}, Time: ${Date.now() - startTime}ms`);
        } catch (error) {
            logger.error(`Cache Refresh Error: ${error}`);
        }
    }

    private parseApprovedZones(approvedZonesData: any): string[] {
        if (!approvedZonesData) return [];

        try {
            if (typeof approvedZonesData === 'string') {
                if (approvedZonesData.startsWith('[') || approvedZonesData.startsWith('{')) {
                    const parsed = JSON.parse(approvedZonesData);
                    return Array.isArray(parsed) ? parsed : [];
                } else {
                    return approvedZonesData.split(',').map((zone: string) => zone.trim()).filter(Boolean);
                }
            } else if (Array.isArray(approvedZonesData)) {
                return approvedZonesData;
            }
        } catch (error) {
            logger.warn(`Failed to parse approvedZones: ${approvedZonesData}`);
        }

        return [];
    }

    /**
     * FIXED: Batch update geolocation to ensure all drivers are in the geo index
     */
    private async batchUpdateGeolocation(updates: Array<{driverId: string, lat: number, lng: number}>): Promise<void> {
        try {
            // GEOADD accepts: key, lng1, lat1, member1, lng2, lat2, member2, ...
            const args: (string | number)[] = ['drivers:locations'];

            updates.forEach(({ driverId, lat, lng }) => {
                args.push(lng, lat, driverId);
            });

            // Use a single GEOADD command to add all drivers
            await redis.geoadd(args[0] as string, ...args.slice(1) as any[]);

            logger.debug(`✓ Updated ${updates.length} drivers in geospatial index`);
        } catch (error) {
            logger.error(`❌ Batch geospatial update failed: ${error}`);

            // Fallback: Update one by one
            logger.warn('Falling back to individual GEOADD commands...');
            for (const { driverId, lat, lng } of updates) {
                try {
                    await redis.geoadd('drivers:locations', lng, lat, driverId);
                } catch (err) {
                    logger.error(`Failed to update geolocation for ${driverId}: ${err}`);
                }
            }
        }
    }

    private async refreshProfileCache(): Promise<void> {
        const startTime = Date.now();
        try {
            const keys = await redis.keys('driver:*:profile');
            if (keys.length === 0) {
                logger.warn('No driver profile keys found in Redis');
                return;
            }

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

    isReady(): boolean {
        return this.isInitialized;
    }

    private isNewDriver(approvedDate: string): boolean {
        if (!approvedDate) return false;
        const approved = new Date(approvedDate);
        return (Date.now() - approved.getTime()) <= (30 * 24 * 60 * 60 * 1000); // 30 days
    }

    stop(): void {
        if (this.locationRefreshInterval) {
            clearInterval(this.locationRefreshInterval);
            this.locationRefreshInterval = null;
        }
        if (this.profileRefreshInterval) {
            clearInterval(this.profileRefreshInterval);
            this.profileRefreshInterval = null;
        }
    }
}