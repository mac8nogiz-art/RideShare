import axios from "axios";
import { logger } from "../logger";

interface DistanceDurationResult {
    distanceKm: number;
    durationMin: number;
    distanceText: string;
    durationText: string;
}

export class MapboxService {
    private readonly baseUrl = "https://api.mapbox.com/directions/v5/mapbox/driving";
    private readonly token = process.env.MAPBOX_TOKEN!;

    /**
     * Fetch distance and duration between pickup and drop coordinates using Mapbox Directions API.
     */
    async getDistanceAndDuration(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {
        try {
            const url = `${this.baseUrl}/${pickupLng},${pickupLat};${dropLng},${dropLat}?geometries=geojson&access_token=${this.token}`;
            const response = await axios.get(url);
            const route = response.data?.routes?.[0];

            if (!route) {
                throw new Error("No valid route returned from Mapbox");
            }

            const distanceKm = route.distance / 1000;
            const durationMin = route.duration / 60;

            return {
                distanceKm,
                durationMin,
                distanceText: `${distanceKm.toFixed(2)} km`,
                durationText: `${Math.round(durationMin)} mins`,
            };
        } catch (err: any) {
            logger.error(`Mapbox Error: ${err.message}`);

            return {
                distanceKm: 0,
                durationMin: 0,
                distanceText: "0 km",
                durationText: "0 mins",
            };
        }
    }
}
