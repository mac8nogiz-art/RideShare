import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Driver, DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;

    private readonly DEFAULT_STALE_THRESHOLD = 30000;
    private readonly MAX_RADIUS = 15;
    private readonly JOB_SEARCH_TTL = 15;
    private readonly MATCHED_DRIVERS_TTL = 900;

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
    }

    async findBestDrivers(job: Job, customerId: string): Promise<string[]> {
        const startTime = Date.now();

        try {
            // Set TTL marker - background search will check this
            await redis.setex(`job:${job.id}:active`, this.JOB_SEARCH_TTL, Date.now().toString());

            const zone = await this.zoneService.getZoneForJob(job);

            if (!zone) {
                logger.warn(`No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                await redis.del(`job:${job.id}:active`);
                return [];
            }

            logger.info(`Job ${job.id} at [${job.pickupLat}, ${job.pickupLng}] assigned to zone: ${zone.name} (${zone._id})`);

            // Store all unique
            const allMatchedDrivers = new Map<string, DriverWithDistance>();
            const favoriteDriverIds = await this.getCustomerFavorites(customerId);
            const favoriteSet = new Set(favoriteDriverIds);

            let firstDriverFound = false;
            const radiusSteps = [3, 6, 9, 12, 15];

            // Store zone info
            await redis.setex(`job:${job.id}:zone`, this.MATCHED_DRIVERS_TTL, zone._id);

            // Progressive search through radius steps
            for (const radius of radiusSteps) {
                // Check if job is still active (TTL not expired)
                const isActive = await redis.exists(`job:${job.id}:active`);
                if (!isActive) {
                    logger.info(`Job ${job.id} TTL expired, stopping search at ${radius}km`);
                    break;
                }

                logger.info(`Searching for drivers within ${radius}km...`);

                const drivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, radius);

                // Add new drivers (avoid duplicates)
                let newDriversCount = 0;
                for (const driver of drivers) {
                    if (!allMatchedDrivers.has(driver.driverId)) {
                        allMatchedDrivers.set(driver.driverId, driver);
                        newDriversCount++;
                    }
                }

                logger.info(`Found ${newDriversCount} new drivers at ${radius}km (Total: ${allMatchedDrivers.size})`);

                // If this is the first driver found, return immediately
                if (!firstDriverFound && allMatchedDrivers.size > 0) {
                    firstDriverFound = true;

                    const matches = this.processBestDrivers(
                        Array.from(allMatchedDrivers.values()),
                        favoriteSet
                    );

                    logger.info(`IMMEDIATE RESPONSE: Returning ${matches.length} driver(s) at ${radius}km in ${Date.now() - startTime}ms`);


                    await redis.setex(`job:${job.id}:matched_drivers`, this.MATCHED_DRIVERS_TTL, JSON.stringify(matches));

                    // Continue background search (will stop after 15s TTL)
                    this.continueBackgroundSearch(
                        job,
                        zone,
                        allMatchedDrivers,
                        favoriteSet,
                        radiusSteps,
                        radius,
                        startTime
                    ).catch(err => {
                        logger.error(`Background search error for job ${job.id}: ${err.message}`);
                    });

                    return matches;
                }
            }


            if (allMatchedDrivers.size === 0) {
                logger.info(`No eligible drivers found in zone ${zone.name} for job ${job.id}`);
                await redis.del(`job:${job.id}:active`);
                return [];
            }

            // Process final results
            const matches = this.processBestDrivers(
                Array.from(allMatchedDrivers.values()),
                favoriteSet
            );

            if (matches.length > 0) {
                await redis.setex(`job:${job.id}:matched_drivers`, this.MATCHED_DRIVERS_TTL, JSON.stringify(matches));
            }

            await redis.del(`job:${job.id}:active`);

            const matchingTime = Date.now() - startTime;
            logger.info(`Matched ${matches.length} drivers in zone ${zone.name} for job ${job.id} in ${matchingTime}ms`);

            return matches;

        } catch (error) {
            logger.error(`Driver matching failed for job ${job.id}: ${error}`);
            await redis.del(`job:${job.id}:active`);
            return [];
        }
    }

    /**
     * Continue searching for more drivers in the background
     * Stops automatically when TTL expires (15 seconds)
     */
    private async continueBackgroundSearch(
        job: Job,
        zone: any,
        allMatchedDrivers: Map<string, DriverWithDistance>,
        favoriteSet: Set<string>,
        radiusSteps: number[],
        currentRadius: number,
        startTime: number
    ): Promise<void> {
        logger.info(`Background search continuing for job ${job.id} from ${currentRadius}km...`);

        const remainingRadii = radiusSteps.filter(r => r > currentRadius);

        for (const radius of remainingRadii) {
            // Check if TTL has expired
            const isActive = await redis.exists(`job:${job.id}:active`);
            if (!isActive) {
                logger.info(` Job ${job.id} TTL expired (15s), stopping background search`);
                break;
            }

            try {
                logger.info(`Background: Searching ${radius}km...`);

                const drivers = await this.getNearbyDriversInZone(
                    job.pickupLat,
                    job.pickupLng,
                    zone._id,
                    radius
                );

                let newDriversCount = 0;
                for (const driver of drivers) {
                    if (!allMatchedDrivers.has(driver.driverId)) {
                        allMatchedDrivers.set(driver.driverId, driver);
                        newDriversCount++;
                    }
                }

                if (newDriversCount > 0) {
                    logger.info(`Background: Found ${newDriversCount} new drivers at ${radius}km (Total: ${allMatchedDrivers.size})`);

                    const updatedMatches = this.processBestDrivers(
                        Array.from(allMatchedDrivers.values()),
                        favoriteSet
                    );

                    await redis.setex(`job:${job.id}:matched_drivers`, this.MATCHED_DRIVERS_TTL, JSON.stringify(updatedMatches));

                    logger.info(`Background: Updated matched drivers list with ${updatedMatches.length} drivers`);

                    // Publish update via Redis Pub/Sub
                    await redis.publish(
                        `job:${job.id}:driver-updates`,
                        JSON.stringify({
                            jobId: job.id,
                            totalDrivers: updatedMatches.length,
                            newDriversAdded: newDriversCount,
                            searchRadius: radius,
                            timestamp: new Date().toISOString()
                        })
                    );
                }
            } catch (error) {
                logger.error(`Background search failed at ${radius}km: ${error}`);
            }
        }

        // Cleanup - remove active marker
        await redis.del(`job:${job.id}:active`);

        const totalTime = Date.now() - startTime;
        logger.info(`🏁 Background search completed for job ${job.id}. Total: ${allMatchedDrivers.size} drivers in ${totalTime}ms`);
    }

    /**
     * Process and rank drivers based on priority and distance
     * NO MAX LIMIT - returns all eligible drivers
     */
    private processBestDrivers(
        drivers: DriverWithDistance[],
        favoriteSet: Set<string>
    ): string[] {
        const driversWithPriority = drivers.map(driver => ({
            driverId: driver.driverId,
            priority: this.calculateDriverPriority(driver, favoriteSet.has(driver.driverId)),
            distance: driver.distance,
            isFavorite: favoriteSet.has(driver.driverId)
        }));

        // Return ALL drivers, sorted by priority and distance
        return driversWithPriority
            .filter(d => d.priority > 0)
            .sort((a, b) => {
                if (b.priority !== a.priority) return b.priority - a.priority;
                return a.distance - b.distance;
            })
            .map(d => d.driverId);
    }

    private async getNearbyDriversInZone(jobLat: number, jobLng: number, zoneId: string, radiusKm: number): Promise<DriverWithDistance[]> {
        const nearbyDrivers: DriverWithDistance[] = [];

        try {
            // Get drivers within radius
            const result = await redis.geosearch('drivers:locations', 'FROMLONLAT', jobLng, jobLat, 'BYRADIUS', radiusKm, 'km', 'WITHDIST', 'ASC') as any;
            logger.info(`GEOSEARCH raw result length: ${result?.length || 0}`);
            if (!result || result.length === 0) {
                logger.warn(`GEOSEARCH returned 0 results within ${radiusKm}km`);
                return nearbyDrivers;
            }
            // Parse GEOSEARCH results
            const driverIds: string[] = [];
            const distances: number[] = [];

            const isNestedArray = Array.isArray(result[0]);

            if (isNestedArray) {
                logger.info(`Parsing NESTED array format (${result.length} entries)`);
                result.forEach((item: any, index: number) => {
                    if (Array.isArray(item) && item.length >= 2) {
                        const driverId = item[0] as string;
                        const distance = parseFloat(item[1] as string);

                        if (driverId && !isNaN(distance) && distance <= radiusKm) {
                            driverIds.push(driverId);
                            distances.push(distance);
                            logger.debug(`[${index}] ✓ ${driverId} at ${distance.toFixed(2)}km`);
                        }
                    }
                });
            } else {
                logger.info(`Parsing FLAT array format (${result.length} elements)`);
                for (let i = 0; i < result.length; i += 2) {
                    const driverId = result[i] as string;
                    const distance = parseFloat(result[i + 1] as string);

                    if (driverId && !isNaN(distance) && distance <= radiusKm) {
                        driverIds.push(driverId);
                        distances.push(distance);
                        logger.debug(`[${i / 2}] ✓ ${driverId} at ${distance.toFixed(2)}km`);
                    }
                }
            }

            logger.info(`GEOSEARCH parsed ${driverIds.length} valid drivers within ${radiusKm}km`);

            if (driverIds.length === 0) {
                logger.warn(`All ${result.length} GEOSEARCH results failed validation`);
                return nearbyDrivers;
            }

            // OPTIMIZATION: Use pipeline to fetch JSON data efficiently
            const pipeline = redis.pipeline();
            driverIds.forEach(driverId => {
                pipeline.call('JSON.GET', `driver:${driverId}`);
            });
            const results = await pipeline.exec();

            if (!results) {
                logger.warn('Pipeline returned no results');
                return nearbyDrivers;
            }

            logger.info(`Processing ${driverIds.length} drivers from pipeline`);

            // Process each driver
            for (let i = 0; i < driverIds.length; i++) {
                const driverId = driverIds[i];
                const distance = distances[i];

                const result = results[i];

                if (result[0]) {
                    const errorMsg = result[0] instanceof Error ? result[0].message : JSON.stringify(result[0]);
                    logger.warn(`Redis error for driver ${driverId}: ${errorMsg}`);
                    continue;
                }

                const driverJson = result[1];

                if (!driverJson) {
                    logger.debug(`${driverId}: No data found`);
                    continue;
                }

                try {
                    // Parse the JSON result
                    const driverData = typeof driverJson === 'string' ? JSON.parse(driverJson) : driverJson;

                    // Extract location coordinates
                    const coordinates = driverData.location?.coordinates;
                    if (!coordinates || !Array.isArray(coordinates) || coordinates.length < 2) {
                        logger.debug(`${driverId}: Invalid location data`);
                        continue;
                    }

                    const [lng, lat] = coordinates;
                    const lastUpdate = Date.now();
                    const isBusy = driverData.iAmBusy === true;
                    const score = parseInt(driverData.score || '50');
                    const approvedZones = Array.isArray(driverData.approved_zone) ? driverData.approved_zone : [];

                    // Check zone approval
                    const isApproved = approvedZones.length === 0 || approvedZones.includes(zoneId);
                    if (!isApproved) {
                        logger.debug(`${driverId}: Not approved for zone ${zoneId}`);
                        continue;
                    }

                    // Create driver object
                    const driver: Driver = {
                        driverId,
                        lat,
                        lng,
                        score,
                        isBusy,
                        isNew: false,
                        lastUpdate,
                        approvedZones
                    };

                    // Add to results
                    nearbyDrivers.push({
                        ...driver,
                        distance,
                        priority: 0
                    });

                    logger.info(`${driverId}: ELIGIBLE - ${distance.toFixed(2)}km, score: ${score}`);

                } catch (parseError) {
                    logger.error(`${driverId}: JSON parse error - ${parseError}`);
                }
            }

            logger.info(`Found ${nearbyDrivers.length}/${driverIds.length} eligible drivers within ${radiusKm}km`);

        } catch (error) {
            logger.error(`Geosearch failed - Radius: ${radiusKm}km, Error: ${error}`);
            return nearbyDrivers;
        }

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


}