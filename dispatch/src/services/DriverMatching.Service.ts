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

            const favoriteDriverIds = await this.getCustomerFavorites(customerId);
            //todo sort it optimized

            logger.info(` Searching for drivers within 3km...`);
            let nearbyDrivers = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 3);


            if (nearbyDrivers.length < maxDrivers) {
                logger.info(` Expanding search to 5km (found ${nearbyDrivers.length} so far)...`);
                const drivers5km = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 5);
                nearbyDrivers = [...nearbyDrivers, ...drivers5km.filter(d => d.distance > 3)];
            }

            if (nearbyDrivers.length < maxDrivers) {
                logger.info(` Expanding search to 15km (found ${nearbyDrivers.length} so far)...`);
                const drivers15km = await this.getNearbyDriversInZone(job.pickupLat, job.pickupLng, zone._id, 15);
                nearbyDrivers = [...nearbyDrivers, ...drivers15km.filter(d => d.distance > 5)];
            }

            if (nearbyDrivers.length === 0) {
                logger.info(`No eligible drivers found in zone ${zone.name} for job ${job.id}`);
                return [];
            }

            const driversWithPriority = nearbyDrivers.map(driver => ({
                driverId: driver.driverId,
                priority: this.calculateDriverPriority(driver, favoriteDriverIds.has(driver.driverId)),
                distance: driver.distance
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

            return matches;

        } catch (error) {
            logger.error(`Driver matching failed for job ${job.id}: ${error}`);
            return [];
        }
    }

    async getDriversInZone(zoneId: string): Promise<string[]> {
        try {
            const allDrivers = Array.from(this.driverLocationService.getAllDrivers().keys());
            const driversInZone: string[] = [];
            // Batch check zone approvals
            const pipeline = redis.pipeline();
            allDrivers.forEach(driverId => {
                pipeline.smembers(`driver:${driverId}:approved_zones`);
            });
            const results = await pipeline.exec();

            if (!results) return [];

            allDrivers.forEach((driverId, index) => {
                const approvedZones = (results[index]?.[1] as string[]) || [];
                if (approvedZones.length === 0 || approvedZones.includes(zoneId)) {
                    driversInZone.push(driverId);
                }
            });

            return driversInZone;
        } catch (error) {
            logger.error(`Get Drivers In Zone Error - Zone: ${zoneId}, Error: ${error}`);
            return [];
        }
    }

    private async getNearbyDriversInZone(jobLat: number, jobLng: number, zoneId: string, radiusKm: number): Promise<DriverWithDistance[]> {
        const nearbyDrivers: DriverWithDistance[] = [];

        try {
            logger.info(` GEOSEARCH: Looking for drivers near [${jobLat}, ${jobLng}] within ${radiusKm}km`);

            const geoCount = await redis.zcard('drivers:locations');
            logger.info(` Geospatial index has ${geoCount} entries`);

            if (geoCount === 0) {
                logger.warn(` Geospatial index is EMPTY - falling back to memory search`);
                return this.getNearbyDriversInZoneFallback(jobLat, jobLng, zoneId, radiusKm);
            }


            const result = await redis.geosearch('drivers:locations', 'FROMLONLAT', jobLng, jobLat, 'BYRADIUS', radiusKm, 'km', 'WITHDIST', 'ASC') as any;

            logger.info(` GEOSEARCH raw result length: ${result?.length || 0}`);

            if (!result || result.length === 0) {
                logger.warn(`  GEOSEARCH returned 0 results within ${radiusKm}km`);
                return nearbyDrivers;
            }

            // CRITICAL FIX: Parse GEOSEARCH results - handle both formats
            const driverIds: string[] = [];
            const distances: number[] = [];


            const isNestedArray = Array.isArray(result[0]);

            if (isNestedArray) {

                logger.info(` Parsing NESTED array format (${result.length} entries)`);
                result.forEach((item: any, index: number) => {
                    if (Array.isArray(item) && item.length >= 2) {
                        const driverId = item[0] as string;
                        const distance = parseFloat(item[1] as string);

                        if (driverId && !isNaN(distance) && distance <= radiusKm) {
                            driverIds.push(driverId);
                            distances.push(distance);
                            logger.debug(`   [${index}] ✓ ${driverId} at ${distance.toFixed(2)}km`);
                        } else {
                            logger.debug(`   [${index}] ✗ Invalid: id=${driverId}, dist=${distance}`);
                        }
                    }
                });
            } else {

                logger.info(` Parsing FLAT array format (${result.length} elements)`);
                for (let i = 0; i < result.length; i += 2) {
                    const driverId = result[i] as string;
                    const distance = parseFloat(result[i + 1] as string);

                    if (driverId && !isNaN(distance) && distance <= radiusKm) {
                        driverIds.push(driverId);
                        distances.push(distance);
                        logger.debug(`   [${i / 2}] ✓ ${driverId} at ${distance.toFixed(2)}km`);
                    } else {
                        logger.debug(`   [${i / 2}] ✗ Invalid: id=${driverId}, dist=${distance}`);
                    }
                }
            }

            logger.info(` GEOSEARCH parsed ${driverIds.length} valid drivers within ${radiusKm}km`);

            if (driverIds.length === 0) {
                logger.warn(`  All ${result.length} GEOSEARCH results failed validation`);
                return nearbyDrivers;
            }

            const now = Date.now();


            const pipeline = redis.pipeline();
            driverIds.forEach(driverId => {
                pipeline.hgetall(`driver:${driverId}:location`);
                pipeline.hgetall(`driver:${driverId}:profile`);
                pipeline.smembers(`driver:${driverId}:approved_zones`);
            });
            const results = await pipeline.exec();

            if (!results) {
                logger.warn(' Pipeline returned no results');
                return nearbyDrivers;
            }

            logger.info(` Processing ${driverIds.length} drivers from pipeline (${results.length} results)`);


            for (let i = 0; i < driverIds.length; i++) {
                const driverId = driverIds[i];
                const distance = distances[i];

                const locationIdx = i * 3;
                const profileIdx = i * 3 + 1;
                const zonesIdx = i * 3 + 2;


                if (locationIdx >= results.length || profileIdx >= results.length || zonesIdx >= results.length) {
                    logger.warn(` Index out of bounds for driver ${driverId}`);
                    continue;
                }

                const locationResult = results[locationIdx];
                const profileResult = results[profileIdx];
                const zonesResult = results[zonesIdx];


                if (locationResult[0] || profileResult[0] || zonesResult[0]) {
                    logger.warn(`  Redis error for driver ${driverId}: ${locationResult[0] || profileResult[0] || zonesResult[0]}`);
                    continue;
                }

                const locationData = locationResult[1] as Record<string, string> | null;
                const profileData = profileResult[1] as Record<string, string> | null;
                const approvedZones = (zonesResult[1] as string[]) || [];


                if (!locationData || !profileData) {
                    logger.debug(` ${driverId}: Missing data (location: ${!!locationData}, profile: ${!!profileData})`);
                    continue;
                }


                const lat = parseFloat(locationData.lat);
                const lng = parseFloat(locationData.lng);
                const lastUpdate = parseInt(locationData.lastUpdate || locationData.ts || '0');
                const isBusy = profileData.isBusy === 'true';
                const score = parseInt(profileData.score || '0');


                if (isNaN(lat) || isNaN(lng)) {
                    logger.debug(`${driverId}: Invalid coordinates`);
                    continue;
                }


                const age = now - lastUpdate;
                if (lastUpdate === 0 || age > this.DEFAULT_STALE_THRESHOLD) {
                    logger.debug(`${driverId}: Stale data (age: ${age}ms, threshold: ${this.DEFAULT_STALE_THRESHOLD}ms)`);
                    continue;
                }

                if (isBusy) {
                    logger.debug(` ${driverId}: Busy`);
                    continue;
                }

                const isApproved = approvedZones.length === 0 || approvedZones.includes(zoneId);
                if (!isApproved) {
                    logger.debug(` ${driverId}: Not approved for zone ${zoneId}`);
                    continue;
                }

                const driver: Driver = {
                    driverId, lat, lng, score, isBusy, isNew: false, lastUpdate, approvedZones
                };

                // Add to results
                nearbyDrivers.push({
                    ...driver, distance, priority: 0
                });

                logger.info(` ${driverId}: ELIGIBLE - ${distance.toFixed(2)}km, score: ${score}, age: ${age}ms`);
            }

            logger.info(` Found ${nearbyDrivers.length}/${driverIds.length} eligible drivers within ${radiusKm}km`);

        } catch (error) {
            logger.error(`Geosearch failed - Radius: ${radiusKm}km, Error: ${error}`);
            // Don't fallback on every error - return empty
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