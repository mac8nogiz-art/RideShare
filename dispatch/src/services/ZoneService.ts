import {Job, Zone} from "../types";
import {redis} from "../infrastructure/redis";
import logger from "../logger";
import {Db} from "mongodb";
import {connectMongo, getMongoDB} from "../infrastructure/mongo";


export class ZoneService {
    private db!: Db;

    public async init(): Promise<void> {
        try {
            await connectMongo();
            this.db = getMongoDB();
            await this.ensureGeospatialIndex();
            logger.info("ZoneService initialized successfully");
        } catch (error: any) {
            logger.error("ZoneService initialization failed:", error);
            throw error;
        }
    }

    /**
     * STEP 1: Get zone for customer's pickup location
     * Uses MongoDB geospatial query for efficient polygon matching
     *
     * @param job - The job/booking request with pickup coordinates
     * @returns Zone object or null if no zones configured
     */

    public async getZoneForJob(job: Job): Promise<Zone | null> {
        logger.info(`Finding zone for pickup location: [${job.pickupLat}, ${job.pickupLng}]`);

        try {
            // MongoDB $geoIntersects ---> spatial services query ---->to find zone containing the point
            const zone = await this.db
                .collection<Zone>('drivergeoareas')
                .findOne({
                    status: true,
                    location: {
                        $geoIntersects: {
                            $geometry: {
                                type: "Point",
                                coordinates: [job.pickupLng, job.pickupLat]
                            }
                        }
                    }
                });
            //todo find one to find

            if (zone) {
                logger.info(`Pickup location is in zone: "${zone.name}" (${zone._id})`);
                return zone;
            } else {
                logger.warn(`Pickup location [${job.pickupLat}, ${job.pickupLng}] not in any configured zone`);
                return null;
            }

        } catch (error: any) {
            logger.error(`Error finding zone for job ${job.id}:`, error);
            return null

        }
    }

    /**
     * STEP 2: Check if driver is approved for the zone
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
                logger.info(`No zone restrictions - Driver ${driverId} approved`);
                return true;
            }

            const approvedZones = await redis.smembers(`driver:${driverId}:approved_zones`);

            if (approvedZones.length === 0) {
                logger.info(`Driver ${driverId} has empty approved_zones - approved for ALL zones including ${zoneId}`);
                return true;
            }

            const isApproved = approvedZones.includes(zoneId);

            if (isApproved) {
                logger.info(`✓ Driver ${driverId} is approved for zone ${zoneId}`);
            } else {
                logger.warn(`✗ Driver ${driverId} is NOT approved for zone ${zoneId} (approved for: ${approvedZones.join(', ')})`);
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
        logger.info(`✓ Driver ${driverId} approved for zone ${zoneId}`);
    }

    /**
     * Remove driver's approval for a specific zone
     */
    public async removeDriverZoneApproval(driverId: string, zoneId: string): Promise<void> {
        await redis.srem(`driver:${driverId}:approved_zones`, zoneId);
        logger.info(`✓ Driver ${driverId} removed from zone ${zoneId}`);
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
     * Get zone by ID from MongoDB
     */
    public async getZoneById(zoneId: string): Promise<Zone | null> {
        try {
            const zone = await this.db
                .collection<Zone>('drivergeoareas')
                .findOne({_id: zoneId, status: true});

            return zone;
        } catch (error: any) {
            logger.error(`Error fetching zone ${zoneId}:`, error);
            return null;
        }
    }

    /**
     * Get all active zones from MongoDB
     */
    public async getAllZones(): Promise<Zone[]> {
        try {
            const zones = await this.db
                .collection<Zone>('drivergeoareas')
                .find({status: true})
                .toArray();

            return zones;
        } catch (error: any) {
            logger.error('Error fetching all zones:', error);
            return [];
        }
    }

    /**
     * Find zones within a radius of a point (useful for nearby zone searches)
     */
    public async findZonesNearPoint(lat: number, lng: number, radiusInKm: number): Promise<Zone[]> {
        try {
            const zones = await this.db
                .collection<Zone>('drivergeoareas')
                .find({
                    status: true,
                    location: {
                        $near: {
                            $geometry: {
                                type: "Point",
                                coordinates: [lng, lat]
                            },
                            $maxDistance: radiusInKm * 1000 // Convert km to meters
                        }
                    }
                })
                .toArray();

            logger.info(`Found ${zones.length} zones within ${radiusInKm}km of [${lat}, ${lng}]`);
            return zones;
        } catch (error: any) {
            logger.error('Error finding zones near point:', error);
            return [];
        }
    }

    /**
     * Ensure 2dsphere index exists for geospatial queries
     */

    private async ensureGeospatialIndex(): Promise<void> {
        try {
            await this.db.collection('drivergeoareas').createIndex({location: "2dsphere"});
            logger.info("✓ Geospatial index verified on drivergeoareas.location");
        } catch (error: any) {
            logger.warn(`Geospatial index creation skipped (may already exist): ${error.message}`);
        }
    }
}