import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {ObjectId} from 'mongodb';
import {getMongoDB} from "../infrastructure/mongo";

export class FreeDriverService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;
    private readonly FREE_DRIVERS_RADIUS = 15; // 15 km

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
    }

    async getFreeDriversForJob(job: Job, customerId: string): Promise<string[] | null> {
        const startTime = Date.now();

        try {
            const [zoneIds, blockedSet] = await Promise.all([
                this.zoneService.getZoneForJob(job),
                this.getCustomerBlockedDrivers(customerId)
            ]);

            if (!zoneIds || zoneIds.length === 0) {
                logger.warn(`No zone found for job ${job.id}`);
                return null;
            }

            logger.info(`Looking for free drivers in zones: ${zoneIds.join(', ')}`);

            const freeDrivers = await this.findFreeDrivers(
                job.pickupLat,
                job.pickupLng,
                zoneIds,
                blockedSet
            );

            if (freeDrivers && freeDrivers.length > 0) {
                logger.info(`Found ${freeDrivers.length} free drivers in ${Date.now() - startTime}ms`);
                return freeDrivers;
            }

            logger.info(`No free drivers found for job ${job.id} in ${Date.now() - startTime}ms`);
            return [];
        } catch (error: any) {
            logger.error(`Free driver search failed for job ${job.id}: ${error.message}`);
            return null;
        }
    }

    private async getNearbyDriversInZone(
        jobLat: number,
        jobLng: number,
        jobZoneIds: string[],
        radiusKm: number
    ): Promise<DriverWithDistance[]> {
        try {
            const result = (await redis.geosearch(
                'drivers:locations',
                'FROMLONLAT',
                jobLng,
                jobLat,
                'BYRADIUS',
                radiusKm,
                'km',
                'WITHDIST',
                'ASC'
            )) as any;

            const driverIds = result.map((item: any) => `driver:${item[0]}`);
            const distances = result.map((item: any) => parseFloat(item[1]));

            const jsonStrings: any = await redis.call('JSON.MGET', ...driverIds, "$");

            const drivers = jsonStrings
                .map((item: any, i: number) => {
                    const driverData = JSON.parse(item)?.[0];
                    if (!driverData) return null;

                    const driverApprovedZones = Array.isArray(driverData.approved_zones)
                        ? driverData.approved_zones.map(String)
                        : [];

                    if (driverApprovedZones.length > 0 &&
                        !jobZoneIds.some(zoneId => driverApprovedZones.includes(zoneId))) {
                        return null;
                    }

                    const [lng, lat] = driverData.location?.coordinates || [];

                    return {
                        driverId: driverIds[i].replace('driver:', ''),
                        lat,
                        lng,
                        // Handle both field names for busy status
                        isBusy: driverData.iAmBusy || driverData.isBusy || false,
                        isNew: driverData.isNew || false,
                        distance: distances[i],
                        priorityScore: driverData.priorityScore
                    };
                })
                .filter((item: any) => item !== null);

            logger.debug(`Found ${drivers.length} drivers within ${radiusKm}km in zone search`);
            return drivers;
        } catch (error: any) {
            logger.error(`GEOSEARCH failed - Radius: ${radiusKm}km, Error: ${error.message}`);
            return [];
        }
    }

    private async findFreeDrivers(
        jobLat: number,
        jobLng: number,
        jobZoneIds: string[],
        blockedSet: Set<string>
    ): Promise<string[]> {
        try {
            logger.info(`Searching for free drivers within ${this.FREE_DRIVERS_RADIUS}km`);

            const drivers = await this.getNearbyDriversInZone(
                jobLat,
                jobLng,
                jobZoneIds,
                this.FREE_DRIVERS_RADIUS
            );

            // Filter to only free (not busy) and not blocked drivers
            const freeDrivers = drivers
                .filter(d => !d.isBusy && !blockedSet.has(d.driverId))
                .sort((a, b) => a.distance - b.distance)
                .map(d => d.driverId);

            logger.info(`Found ${freeDrivers.length} free drivers within ${this.FREE_DRIVERS_RADIUS}km`);

            if (freeDrivers.length > 0) {
                logger.debug(`Free drivers (sorted by distance): ${freeDrivers.slice(0, 5).join(', ')}${freeDrivers.length > 5 ? '...' : ''}`);
            }

            return freeDrivers;
        } catch (error: any) {
            logger.error(`Failed to find free drivers: ${error.message}`);
            return [];
        }
    }

    private async getCustomerBlockedDrivers(customerId: string): Promise<Set<string>> {
        try {
            const db = getMongoDB();
            const user = await db.collection('users').findOne(
                {_id: new ObjectId(customerId)},
                {projection: {blockDrivers: 1}}
            );

            if (!user) {
                return new Set();
            }

            const blocked = Array.isArray(user?.blockDrivers) ? user.blockDrivers.map(String) : [];

            if (blocked.length > 0) {
                logger.debug(`Customer ${customerId} has ${blocked.length} blocked drivers`);
            }

            return new Set(blocked);
        } catch (error: any) {
            logger.error(`Failed to fetch customer blocked drivers: ${error.message}`);
            return new Set();
        }
    }
}