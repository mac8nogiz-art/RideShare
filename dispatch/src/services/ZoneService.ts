import {Job, Zone} from "../types";
import {redis} from "../infrastructure/redis";
import logger from "../logger";
import {Db} from "mongodb";
import {connectMongo, getMongoDB} from "../infrastructure/mongo";

export class ZoneService {
    private db: Db | null = null;
    private isInitialized: boolean = false;

    public async init(): Promise<void> {
        try {
            logger.info("🔌 Connecting to MongoDB for ZoneService...");

            await connectMongo();
            this.db = getMongoDB();

            if (!this.db) {
                throw new Error("MongoDB connection failed - db is null");
            }

            await this.ensureGeospatialIndex();

            this.isInitialized = true;
            logger.info("✅ ZoneService initialized successfully");
        } catch (error: any) {
            logger.error(`❌ ZoneService initialization failed: ${error.message}`);
            logger.error(`Stack: ${error.stack}`);
            throw error;
        }
    }

    /**
     * Check if ZoneService is ready to use
     */
    private ensureInitialized(): void {
        if (!this.isInitialized || !this.db) {
            throw new Error("ZoneService not initialized - call init() first");
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
            this.ensureInitialized();

            // MongoDB $geoIntersects spatial query to find zone containing the point
            const zone = await this.db!
                .collection<Zone>('geoareas')
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

            if (zone) {
                logger.info(`✅ Pickup location is in zone: "${zone.name}" (${zone._id})`);
                return zone;
            } else {
                logger.warn(`⚠️ No zone found for job ${job.id} at ${job.pickupLat}, ${job.pickupLng}`);
                return null;
            }

        } catch (error: any) {
            logger.error(`❌ Error finding zone for job ${job.id}:`, error);

            // Check if it's an initialization error
            if (error.message.includes('not initialized')) {
                logger.error('⚠️ ZoneService was called before initialization!');
            }

            return null;
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
                logger.info(`✅ Driver ${driverId} is approved for zone ${zoneId}`);
            } else {
                logger.warn(`⚠️ Driver ${driverId} is NOT approved for zone ${zoneId} (approved for: ${approvedZones.join(', ')})`);
            }

            return isApproved;
        } catch (error: any) {
            logger.error(`❌ Error checking driver ${driverId} approval for zone ${zoneId}: ${error.message}`);
            return false;
        }
    }

    /**
     * Approve a driver for a specific zone
     * Admin function to add zone to driver's approved list
     */
    public async approveDriverForZone(driverId: string, zoneId: string): Promise<void> {
        try {
            await redis.sadd(`driver:${driverId}:approved_zones`, zoneId);
            logger.info(`✅ Driver ${driverId} approved for zone ${zoneId}`);
        } catch (error: any) {
            logger.error(`❌ Failed to approve driver ${driverId} for zone ${zoneId}: ${error.message}`);
            throw error;
        }
    }

    /**
     * Remove driver's approval for a specific zone
     */
    public async removeDriverZoneApproval(driverId: string, zoneId: string): Promise<void> {
        try {
            await redis.srem(`driver:${driverId}:approved_zones`, zoneId);
            logger.info(`✅ Driver ${driverId} removed from zone ${zoneId}`);
        } catch (error: any) {
            logger.error(`❌ Failed to remove driver ${driverId} from zone ${zoneId}: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get all zones a driver is approved for
     * Empty array means approved for ALL zones
     */
    public async getDriverApprovedZones(driverId: string): Promise<string[]> {
        try {
            const zones = await redis.smembers(`driver:${driverId}:approved_zones`);

            if (zones.length === 0) {
                logger.info(`Driver ${driverId} approved for ALL zones (empty list)`);
            } else {
                logger.info(`Driver ${driverId} approved for zones: ${zones.join(', ')}`);
            }

            return zones;
        } catch (error: any) {
            logger.error(`❌ Error fetching approved zones for driver ${driverId}: ${error.message}`);
            return [];
        }
    }

    /**
     * Get zone by ID from MongoDB
     */
    public async getZoneById(zoneId: string): Promise<Zone | null> {
        try {
            this.ensureInitialized();

            const zone = await this.db!
                .collection<Zone>('geoareas')
                .findOne({_id: zoneId, status: true});

            return zone;
        } catch (error: any) {
            logger.error(`❌ Error fetching zone ${zoneId}: ${error.message}`);
            return null;
        }
    }

    /**
     * Get all active zones from MongoDB
     */
    public async getAllZones(): Promise<Zone[]> {
        try {
            this.ensureInitialized();

            const zones = await this.db!
                .collection<Zone>('geoareas')
                .find({status: true})
                .toArray();

            logger.info(`📍 Found ${zones.length} active zones`);
            return zones;
        } catch (error: any) {
            logger.error(`❌ Error fetching all zones: ${error.message}`);
            return [];
        }
    }

    /**
     * Find zones within a radius of a point (useful for nearby zone searches)
     */
    public async findZonesNearPoint(lat: number, lng: number, radiusInKm: number): Promise<Zone[]> {
        try {
            this.ensureInitialized();

            const zones = await this.db!
                .collection<Zone>('geoareas')
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

            logger.info(`📍 Found ${zones.length} zones within ${radiusInKm}km of [${lat}, ${lng}]`);
            return zones;
        } catch (error: any) {
            logger.error(`❌ Error finding zones near point: ${error.message}`);
            return [];
        }
    }

    /**
     * Ensure 2dsphere index exists for geospatial queries
     * FIXED: Now creates index on the correct 'geoareas' collection
     */
    private async ensureGeospatialIndex(): Promise<void> {
        try {
            if (!this.db) {
                throw new Error("Cannot create index - db is null");
            }

            // Create index on 'geoareas' collection (not 'drivergeoareas')
            await this.db.collection('geoareas').createIndex({location: "2dsphere"});
            logger.info("✅ Geospatial index verified on geoareas.location");
        } catch (error: any) {
            // Index might already exist - this is not a critical error
            if (error.code === 85 || error.message.includes('already exists')) {
                logger.info("ℹ️ Geospatial index already exists on geoareas.location");
            } else {
                logger.warn(`⚠️ Geospatial index creation warning: ${error.message}`);
            }
        }
    }

    /**
     * Get initialization status
     */
    public isReady(): boolean {
        return this.isInitialized && this.db !== null;
    }
}