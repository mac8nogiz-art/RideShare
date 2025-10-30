import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Driver, DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import { sendKafkaMessage } from '../infrastructure/kafka';

export class DriverMatchingService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;

    private readonly DEFAULT_STALE_THRESHOLD = 30000;
    private readonly MAX_RADIUS = 15;

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
    }

    async findBestDrivers(job: Job, customerId: string, maxDrivers: number = 10): Promise<string[]> {
        const startTime = Date.now();

        try {
            const zone = await this.zoneService.getZoneForJob(job);

            if (!zone) {
                logger.warn(`No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                return [];
            }

            logger.info(`Job ${job.id} at [${job.pickupLat}, ${job.pickupLng}] assigned to zone: ${zone.name} (${zone._id})`);
            logger.info(`Searching for drivers within 3km...`);

            let nearbyDrivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 3);

            if (nearbyDrivers.length < maxDrivers) {
                logger.info(`Expanding search to 5km (found ${nearbyDrivers.length} so far)...`);
                const drivers5km = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 5);
                nearbyDrivers = [...nearbyDrivers, ...drivers5km.filter(d => d.distance > 3)];
            }

            if (nearbyDrivers.length < maxDrivers) {
                logger.info(`Expanding search to 15km (found ${nearbyDrivers.length} so far)...`);
                const drivers15km = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 15);
                nearbyDrivers = [...nearbyDrivers, ...drivers15km.filter(d => d.distance > 5)];
            }


            if (nearbyDrivers.length === 0) {
                logger.info(`No eligible drivers found in zone ${zone.name} for job ${job.id}`);


                await sendKafkaMessage(
                    'no-drivers-found',
                    job.id,
                    {
                        jobId: job.id,
                        customerId: customerId,
                        searchTime: Date.now() - startTime,
                        pickupLocation: { lat: job.pickupLat, lng: job.pickupLng },
                        zone: { id: zone._id, name: zone.name },
                        timestamp: new Date().toISOString()
                    }
                );

                return [];
            }

            const favoriteDriverIds = await this.getCustomerFavorites(customerId);
            const favoriteSet = new Set(favoriteDriverIds);

            const driversWithPriority = nearbyDrivers.map(driver => ({
                driverId: driver.driverId,
                priority: this.calculateDriverPriority(driver, favoriteSet.has(driver.driverId)),
                distance: driver.distance,
                isFavorite: favoriteSet.has(driver.driverId)
            }));

            const matches = driversWithPriority
                .filter(d => d.priority > 0)
                .sort((a, b) => {
                    if (b.priority !== a.priority) return b.priority - a.priority;
                    return a.distance - b.distance;
                })
                .slice(0, maxDrivers)
                .map(d => d.driverId);

            if (matches.length > 0) {
                await redis.setex(`job:${job.id}:matched_drivers`, 300, JSON.stringify(matches));
                await redis.setex(`job:${job.id}:zone`, 300, zone._id);
            }

            const matchingTime = Date.now() - startTime;
            logger.info(`Matched ${matches.length} drivers in zone ${zone.name} for job ${job.id} in ${matchingTime}ms`);


            if (matches.length > 0) {
                const matchedDriversDetails = driversWithPriority
                    .filter(d => matches.includes(d.driverId))
                    .map(d => ({
                        driverId: d.driverId,
                        distance: d.distance,
                        priority: d.priority,
                        isFavorite: d.isFavorite
                    }));



            } else {

                await sendKafkaMessage(
                    'no-drivers-found',
                    job.id,
                    {
                        jobId: job.id,
                        customerId: customerId,
                        searchTime: matchingTime,
                        pickupLocation: { lat: job.pickupLat, lng: job.pickupLng },
                        zone: { id: zone._id, name: zone.name },
                        reason: 'No drivers passed priority filter',
                        timestamp: new Date().toISOString()
                    }
                );
            }

            return matches;

        } catch (error) {
            logger.error(`Driver matching failed for job ${job.id}: ${error}`);


            await sendKafkaMessage(
                'driver-matching-error',
                job.id,
                {
                    jobId: job.id,
                    customerId: customerId,
                    error: error instanceof Error ? error.message : String(error),
                    timestamp: new Date().toISOString()
                }
            );

            return [];
        }
    }

    private async getNearbyDriversInZone(jobLat: number, jobLng: number, zoneId: string, radiusKm: number): Promise<DriverWithDistance[]> {
        const nearbyDrivers: DriverWithDistance[] = [];

        try {
            logger.info(`GEOSEARCH: Looking for drivers near [${jobLat}, ${jobLng}] within ${radiusKm}km`);

            const geoCount = await redis.zcard('drivers:locations');
            logger.info(`Geospatial index has ${geoCount} entries`);

            if (geoCount === 0) {
                logger.warn(`Geospatial index is EMPTY - falling back to memory search`);
                return this.getNearbyDriversInZoneFallback(jobLat, jobLng, zoneId, radiusKm);
            }

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

            const now = Date.now();

            // OPTIMIZATION: Use pipeline to fetch JSON data efficiently
            // const pipeline = redis.pipeline();
            const pipeline = redis.pipeline();
            driverIds.forEach(driverId => {
                pipeline.call('JSON.GET', `driver:${driverId}`); // Use JSON.GET command
            });
            const results = await pipeline.exec();

            if (!results) {
                logger.warn('Pipeline returned no results');
                return nearbyDrivers;
            }

            logger.info(`Processing ${driverIds.length} drivers from pipeline`);

            // Process each driver
            // Process each driver
            for (let i = 0; i < driverIds.length; i++) {
                const driverId = driverIds[i];
                const distance = distances[i];

                const result = results[i];

                if (result[0]) {
                    logger.warn(`Redis error for driver ${driverId}: ${result[0]}`);
                    continue;
                }

                const driverJson = result[1];

                if (!driverJson) {
                    logger.debug(`${driverId}: No data found`);
                    continue;
                }

                try {
                    // Parse the JSON result (it might already be an object or a string)
                    const driverData = typeof driverJson === 'string' ? JSON.parse(driverJson) : driverJson;

                    // NOW extract from the ACTUAL driver document structure
                    // Based on your document, the structure is different!
                    const coordinates = driverData.location?.coordinates;
                    if (!coordinates || !Array.isArray(coordinates) || coordinates.length < 2) {
                        logger.debug(`${driverId}: Invalid location data`);
                        continue;
                    }

                    const [lng, lat] = coordinates;
                    const lastUpdate = Date.now(); // Use current time since we just fetched it
                    const isBusy = driverData.iAmBusy === true;
                    const score = parseInt(driverData.score || '50');
                    const approvedZones = Array.isArray(driverData.approved_zone) ? driverData.approved_zone : [];
                    const isOnline = driverData.iAmOnline === true;

                    // Validate coordinates
                    if (isNaN(lat) || isNaN(lng)) {
                        logger.debug(`${driverId}: Invalid coordinates`);
                        continue;
                    }

                    // Check if online
                    if (!isOnline) {
                        logger.debug(`${driverId}: Offline`);
                        continue;
                    }

                    // Check if busy
                    if (isBusy) {
                        logger.debug(`${driverId}: Busy`);
                        continue;
                    }

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
                    continue;
                }
            }

            logger.info(`Found ${nearbyDrivers.length}/${driverIds.length} eligible drivers within ${radiusKm}km`);

        } catch (error) {
            logger.error(`Geosearch failed - Radius: ${radiusKm}km, Error: ${error}`);
            return nearbyDrivers;
        }

        return nearbyDrivers;
    }

    private async getNearbyDriversInZoneFallback(jobLat: number, jobLng: number, zoneId: string, radiusKm: number): Promise<DriverWithDistance[]> {
        const nearbyDrivers: DriverWithDistance[] = [];
        const now = Date.now();
        const allDrivers = this.driverLocationService.getAllDrivers();

        logger.warn(`  Using FALLBACK search (in-memory has ${allDrivers.size} drivers)`);

        for (const [driverId, driver] of allDrivers.entries()) {
            // Skip stale or busy drivers
            const age = now - driver.lastUpdate;
            if (age > this.DEFAULT_STALE_THRESHOLD) {
                continue;
            }
            if (driver.isBusy) {
                continue;
            }

            // Calculate distance
            const distance = this.spatialService.calculateDistance(jobLat, jobLng, driver.lat, driver.lng);

            // Check radius
            if (distance > radiusKm) {
                continue;
            }

            // Check zone approval
            const isApproved = await this.zoneService.isDriverApprovedForZone(driverId, zoneId);
            if (!isApproved) {
                continue;
            }

            nearbyDrivers.push({
                ...driver, distance, priority: 0
            });
        }

        logger.info(` Fallback found ${nearbyDrivers.length} eligible drivers`);
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