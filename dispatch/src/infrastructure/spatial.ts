import {Zone} from "../types";
import {redis} from "../infrastructure/redis";
import {logger} from "../logger";

export class SpatialService {

    // Ray casting algorithm for point-in-polygon check
    isPointInPolygon(point: [number, number], polygon: number[][][]): boolean {
        const [lng, lat] = point;
        const outerRing = polygon[0]; // First ring is outer boundary

        let inside = false;
        for (let i = 0, j = outerRing.length - 1; i < outerRing.length; j = i++) {
            const [xi, yi] = outerRing[i];
            const [xj, yj] = outerRing[j];

            const intersect = ((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi);

            if (intersect) inside = !inside;
        }
        return inside;
    }

    // Calculate distance between two points (Haversine)
    calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
        const R = 6371; // Earth's radius in km
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLng = (lng2 - lng1) * Math.PI / 180;
        const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }

    // Find which zone a point belongs to
    async findZoneForPoint(lat: number, lng: number): Promise<Zone | null> {
        try {
            // Get all active zones from Redis cache
            const zonesData = await redis.get('zones:active');
            if (!zonesData) return null;

            const zones: Zone[] = JSON.parse(zonesData);

            for (const zone of zones) {
                if (this.isPointInPolygon([lng, lat], zone.location.coordinates)) {
                    return zone;
                }
            }
            return null;
        } catch (error: any) {
            logger.error('Error finding zone for point:', error);
            return null;
        }
    }
}