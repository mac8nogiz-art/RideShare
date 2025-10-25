import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Driver} from '../types';

export class DriverLocationService {
    private driverCache = new Map<string, Driver>();
    private locationRefreshInterval: NodeJS.Timeout | null = null;
    private isInitialized = false;

    async startDriverCacheRefresh(): Promise<void> {
        logger.info('Starting Driver Cache Refresh - Interval: 2s');


        await this.refreshDriverCache();

        this.isInitialized = true;
        logger.info(`Driver cache initialized with ${this.driverCache.size} drivers`);

        this.locationRefreshInterval = setInterval(async () => {
            await this.refreshDriverCache();
        }, 2000);
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

    stop(): void {
        if (this.locationRefreshInterval) {
            clearInterval(this.locationRefreshInterval);
            this.locationRefreshInterval = null;
        }
    }

    private async refreshDriverCache(): Promise<void> {
        const startTime = Date.now();

        try {

            const keys = await redis.keys('driver:*');


            const driverKeys = keys.filter(key => {
                const parts = key.split(':');
                return parts.length === 2 && parts[0] === 'driver';
            });

            if (driverKeys.length === 0) {
                logger.warn('No driver JSON documents found in Redis');
                return;
            }

            logger.debug(`Found ${driverKeys.length} driver documents`);

            const pipeline = redis.pipeline();
            driverKeys.forEach(key => {
                pipeline.call('JSON.GET', key);
            });
            const results = await pipeline.exec();

            const newCache = new Map<string, Driver>();
            const geoUpdates: Array<{ driverId: string, lat: number, lng: number }> = [];

            results?.forEach((result, index) => {
                if (result[0]) {
                    logger.error(`Error fetching ${driverKeys[index]}: ${result[0]}`);
                    return;
                }

                try {
                    const driverData = typeof result[1] === 'string' ? JSON.parse(result[1]) : result[1];

                    const driverId = driverKeys[index].split(':')[1];

                    if (!driverData.iAmOnline) {
                        logger.debug(`Driver ${driverId} is offline, skipping`);
                        return;
                    }

                    if (!driverData.location || !driverData.location.coordinates) {
                        logger.debug(`Driver ${driverId} has no location data`);
                        return;
                    }

                    const [lng, lat] = driverData.location.coordinates;

                    if (isNaN(lat) || isNaN(lng) || lat === 0 || lng === 0) {
                        logger.debug(`Driver ${driverId} has invalid coordinates: [${lat}, ${lng}]`);
                        return;
                    }
                    const approvedZones = Array.isArray(driverData.approved_zones) ? driverData.approved_zones : [];

                    const driver: Driver = {
                        driverId,
                        lat,
                        lng,
                        score: driverData.score || 50,
                        isBusy: driverData.iAmBusy || false,
                        isNew: false,
                        lastUpdate: Date.now(),
                        approvedZones: approvedZones
                    };

                    newCache.set(driverId, driver);
                    geoUpdates.push({driverId, lat, lng});

                    logger.debug(`✓ Loaded driver ${driverId}: ${driverData.fullName} at [${lat}, ${lng}]`);

                } catch (error) {
                    logger.error(`Failed to parse driver data for ${driverKeys[index]}: ${error}`);
                }
            });
            if (geoUpdates.length > 0) {
                await this.batchUpdateGeolocation(geoUpdates);
            }

            this.driverCache = newCache;
            logger.info(`Driver Cache Refreshed - Drivers: ${newCache.size}, Geo updates: ${geoUpdates.length}, Time: ${Date.now() - startTime}ms`);

        } catch (error) {
            logger.error(`Cache Refresh Error: ${error}`);
        }
    }

    private async batchUpdateGeolocation(updates: Array<{
        driverId: string,
        lat: number,
        lng: number
    }>): Promise<void> {
        try {
            const args: (string | number)[] = ['drivers:locations'];
            updates.forEach(({driverId, lat, lng}) => {
                args.push(lng, lat, driverId);
            });
            await redis.geoadd(args[0] as string, ...args.slice(1) as any[]);
            logger.debug(`✓ Updated ${updates.length} drivers in geospatial index 'drivers:locations'`);

        } catch (error) {
            logger.error(`Batch geospatial update failed: ${error}`);
            logger.warn('Falling back to individual GEOADD commands...');
            for (const {driverId, lat, lng} of updates) {
                try {
                    await redis.geoadd('drivers:locations', lng, lat, driverId);
                } catch (err) {
                    logger.error(`Failed to update geolocation for ${driverId}: ${err}`);
                }
            }
        }
    }
}