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
            logger.info(" Connecting to MongoDB for ZoneService...");

            await connectMongo();
            this.db = getMongoDB();

            if (!this.db) {
                throw new Error("MongoDB connection failed - db is null");
            }

            await this.ensureGeospatialIndex();

            this.isInitialized = true;
            logger.info(" ZoneService initialized successfully");
        } catch (error: any) {
            logger.error(` ZoneService initialization failed: ${error.message}`);
            logger.error(`Stack: ${error.stack}`);
            throw error;
        }
    }


    private ensureInitialized(): void {
        if (!this.isInitialized || !this.db) {
            throw new Error("ZoneService not initialized - call init() first");
        }
    }

    public async getZoneForJob(job: Job): Promise<string[]> {
        logger.info(`Finding zone for pickup location: [${job.pickupLat}, ${job.pickupLng}]`);

        try {
            this.ensureInitialized();

            const cursor = await this.db!
                .collection('drivergeoareas')
                .find(
                    {
                        status: true,
                        location: {
                            $geoIntersects: {
                                $geometry: {
                                    type: "Point",
                                    coordinates: [job.pickupLng, job.pickupLat]
                                }
                            }
                        }
                    },
                    {
                        projection: { _id: 1 }
                    }
                ).toArray();

            return cursor?.map(z => z._id.toString()) || [];
        } catch (error: any) {
            logger.error(`Error finding zone for job ${job.id}: ${error.message}`);
            if (error.message.includes('not initialized')) {
                logger.error('ZoneService was called before initialization!');
            }
            return [];
        }
    }




    private async ensureGeospatialIndex(): Promise<void> {
        try {
            if (!this.db) {
                throw new Error("Cannot create index - db is null");
            }

            await this.db.collection('drivergeoareas').createIndex({location: "2dsphere"});
            logger.info(" Geospatial index verified on geoareas.location");
        } catch (error: any) {

            if (error.code === 85 || error.message.includes('already exists')) {
                logger.info("Geospatial index already exists on geoareas.location");
            } else {
                logger.warn(`Geospatial index creation warning: ${error.message}`);
            }
        }
    }

    public isReady(): boolean {
        return this.isInitialized && this.db !== null;
    }
}