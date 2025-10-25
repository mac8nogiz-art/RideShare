import { SpatialService } from "../infrastructure/spatial";
import { Job, Zone } from "../types";
import { redis } from "../infrastructure/redis";
import logger from "../logger";
import { Db } from "mongodb";
import { getMongoDB, connectMongo } from "../infrastructure/mongo";

/**
 * ZoneService - Manages geographic zones for ride matching
 *
 * Flow:
 * 1. Customer requests ride → Get zone based on pickup location
 * 2. Find nearby drivers in that location
 * 3. Check if driver is approved for that zone
 *    - Empty approved_zones[] = driver approved for ALL zones
 *    - Has zones in array = driver only approved for those specific zones
 */
export class ZoneService {
    private spatialService: SpatialService;
    private zoneCache = new Map<string, Zone>();
    private db!: Db;

    constructor() {
        this.spatialService = new SpatialService();
    }

    public async init(): Promise<void> {
        try {
            await connectMongo();
            this.db = getMongoDB();

            await this.loadZonesFromMongoToRedis();


            await this.refreshZoneCache();


            this.startZoneCacheRefresh();

            logger.info("ZoneService initialized successfully");
        } catch (error: any) {
            logger.error("ZoneService initialization failed:", error);
            throw error;
        }
    }

    /**
     * Load zones from MongoDB and cache in Redis
     * This ensures Redis always has the latest zones on startup
     */
    private async loadZonesFromMongoToRedis(): Promise<void> {
        try {
            const zones = await this.mongoFetchActiveZones();

            if (zones && zones.length > 0) {
                await redis.set('zones:active', JSON.stringify(zones));
                logger.info(`Loaded ${zones.length} zones from MongoDB to Redis`);
            } else {
                logger.warn("No zones found in MongoDB - system will operate without zone restrictions");
            }
        } catch (error: any) {
            logger.error("Error loading zones to Redis:", error);
        }
    }

    public startZoneCacheRefresh(): void {
        setInterval(async () => {
            await this.loadZonesFromMongoToRedis();
            await this.refreshZoneCache();
        }, 5 * 60 * 1000); // Every 5 minutes
    }

    /**
     * STEP 1: Get zone for customer's pickup location
     * This is called when a customer requests a ride
     *
     * @param job - The job/booking request with pickup coordinates
     * @returns Zone object or null if no zones configured
     */


    public async getZoneForJob(job: Job): Promise<Zone | null> {
        logger.info(` Finding zone for pickup location: [${job.pickupLat}, ${job.pickupLng}]`);

        const cachedZones = await redis.get('zones:active');

        if (!cachedZones) {
            logger.warn("No zones configured in system - all drivers can accept all rides");
            return null;
        }

        const zones: Zone[] = JSON.parse(cachedZones);
        logger.info(`Checking against ${zones.length} active zones`);


        const zone = this.findZoneByCoordinates(job.pickupLat, job.pickupLng, zones);

        if (zone) {
            logger.info(`Pickup location is in zone: "${zone.name}" (${zone._id})`);
        } else {
            logger.warn(`Pickup location not in any configured zone - may not find drivers`);
        }

        return zone;
    }

    /**
     * STEP 3: Check if driver is approved for the zone
     * Called after finding nearby drivers to filter by zone approval
     *
     * Logic:
     * - If approved_zones is empty [] → Driver approved for ALL zones
     * - If approved_zones has values → Check if zone is in the list
     * - If no zone (null) → All drivers approved (no zone restrictions)
     *
     * @param driverId - The driver ID to check
     * @param zoneId - The zone ID (or null if no zones configured)
     * @returns true if driver can accept rides in this zone
     */

    public async isDriverApprovedForZone(driverId: string, zoneId: string | null): Promise<boolean> {
        try {

            if (!zoneId) {
                logger.info(` No zone restrictions - Driver ${driverId} approved`);
                return true;
            }

            const approvedZones = await redis.smembers(`driver:${driverId}:approved_zones`);

            if (approvedZones.length === 0) {
                logger.info(` Driver ${driverId} has empty approved_zones - approved for ALL zones including ${zoneId}`);
                return true;
            }


            const isApproved = approvedZones.includes(zoneId);

            if (isApproved) {
                logger.info(` Driver ${driverId} is approved for zone ${zoneId}`);
            } else {
                logger.warn(` Driver ${driverId} is NOT approved for zone ${zoneId} (approved for: ${approvedZones.join(', ')})`);
            }

            return isApproved;
        } catch (error: any) {
            logger.error(`Error checking driver ${driverId} approval for zone ${zoneId}:`, error);
            return false;
        }
    }

    /**
     * Approve a driver for a specific zone
     * Admin function to add zone to driver's approved list
     */
    public async approveDriverForZone(driverId: string, zoneId: string): Promise<void> {
        await redis.sadd(`driver:${driverId}:approved_zones`, zoneId);
        logger.info(` Driver ${driverId} approved for zone ${zoneId}`);
    }

    /**
     * Remove driver's approval for a specific zone
     */
    public async removeDriverZoneApproval(driverId: string, zoneId: string): Promise<void> {
        await redis.srem(`driver:${driverId}:approved_zones`, zoneId);
        logger.info(`Driver ${driverId} removed from zone ${zoneId}`);
    }

    /**
     * Get all zones a driver is approved for
     * Empty array means approved for ALL zones
     */

    public async getDriverApprovedZones(driverId: string): Promise<string[]> {
        const zones = await redis.smembers(`driver:${driverId}:approved_zones`);

        if (zones.length === 0) {
            logger.info(`Driver ${driverId} approved for ALL zones (empty list)`);
        } else {
            logger.info(`Driver ${driverId} approved for zones: ${zones.join(', ')}`);
        }

        return zones;
    }

    /**
     * Refresh in-memory cache from Redis
     */
    private async refreshZoneCache(): Promise<void> {
        try {
            const zonesData = await redis.get('zones:active');

            if (zonesData) {
                const zones: Zone[] = JSON.parse(zonesData);
                this.zoneCache.clear();
                zones.forEach(zone => this.zoneCache.set(zone._id, zone));
                logger.info(` Zone cache refreshed: ${zones.length} zones in memory`);
            } else {
                this.zoneCache.clear();
                logger.warn(' No zones in Redis - cache cleared');
            }
        } catch (error: any) {
            logger.error('Zone cache refresh error:', error);
        }
    }

    /**
     * Fetch active zones from MongoDB
     */

    private async mongoFetchActiveZones(): Promise<Zone[]> {
        if (!this.db) {
            throw new Error("MongoDB not connected yet");
        }

        try {
            const zones = await this.db
                .collection<Zone>('zones')
                .find({ active: true })
                .toArray();

            logger.info(`Fetched ${zones.length} active zones from MongoDB`);
            return zones;
        } catch (error: any) {
            logger.error(' Error fetching zones from MongoDB:', error);
            return [];
        }
    }

    /**
     * Find which zone contains the given coordinates
     * Uses spatial polygon checking
     */

    private findZoneByCoordinates(lat: number, lng: number, zones: Zone[]): Zone | null {
        for (const zone of zones) {
            try {

                if (this.spatialService.isPointInPolygon([lng, lat], zone.location.coordinates)) {
                    return zone;
                }
            } catch (error: any) {
                logger.error(`Error checking zone ${zone._id}:`, error);
            }
        }
        return null;
    }


    public getZoneById(zoneId: string): Zone | null {
        return this.zoneCache.get(zoneId) || null;
    }

    public getAllZones(): Zone[] {
        return Array.from(this.zoneCache.values());
    }
}