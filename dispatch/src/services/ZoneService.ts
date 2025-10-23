import {SpatialService} from "../infrastructure/spatial";
import {Job, Zone} from "../types";
import {redis} from "../infrastructure/redis";
import logger from "../logger";


export class ZoneService {
    private spatialService: SpatialService;
    private zoneCache = new Map<string, Zone>();

    constructor() {
        this.spatialService = new SpatialService();
        this.startZoneCacheRefresh();
    }

    async startZoneCacheRefresh(): Promise<void> {
        await this.refreshZoneCache();
        setInterval(async () => {
            await this.refreshZoneCache();
        }, 300000);
    }

    async getZoneForJob(job: Job): Promise<Zone | null> {
        if (job.zoneId && this.zoneCache.has(job.zoneId)) {
            return this.zoneCache.get(job.zoneId)!;
        }
        const zone = await this.spatialService.findZoneForPoint(job.pickupLat, job.pickupLng);
        if (zone) {
            (job as any).zoneId = zone._id;
        }
        return zone;
    }

    async isDriverApprovedForZone(driverId: string, zoneId: string): Promise<boolean> {
        try {

            const approvedZones = await redis.smembers(`driver:${driverId}:approved_zones`);


            if (approvedZones.length === 0) return true;

            return approvedZones.includes(zoneId);
        } catch (error) {
            logger.error(`Error checking driver approval: ${error}`);
            return false;
        }
    }

    async approveDriverForZone(driverId: string, zoneId: string): Promise<void> {
        await redis.sadd(`driver:${driverId}:approved_zones`, zoneId);
        logger.info(`Driver ${driverId} approved for zone ${zoneId}`);
    }

    async getDriverApprovedZones(driverId: string): Promise<string[]> {
        return await redis.smembers(`driver:${driverId}:approved_zones`);
    }

    private async refreshZoneCache(): Promise<void> {
        try {

            const zonesData = await redis.get('zones:active');
            if (zonesData) {
                const zones: Zone[] = JSON.parse(zonesData);
                this.zoneCache.clear();
                zones.forEach(zone => this.zoneCache.set(zone._id, zone));
                logger.info(`Zone cache refreshed: ${zones.length} zones`);
            }
        } catch (error: any) {
            logger.error('Zone cache refresh error:', error);
        }
    }
}