import axios from "axios";
import { logger } from "../logger";

interface DistanceDurationResult {
    distanceKm: number;
    durationMin: number;
    distanceText: string;
    durationText: string;
}

export class MapboxService {
    private readonly mapboxBase = "https://api.mapbox.com/directions/v5/mapbox/driving";
    private readonly osrmBase = process.env.OSRM_BASE_URL || "http://172.105.98.102:5000/route/v1/driving";
    private readonly mapboxToken = process.env.MAPBOX_TOKEN;

    async getDistanceAndDuration(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {

        try {
            logger.info('Attempting to get route from OSRM');
            return await this.getOSRMRoute(pickupLat, pickupLng, dropLat, dropLng);
        } catch (osrmError: any) {
            logger.warn(`OSRM failed: ${osrmError.message}, trying Mapbox fallback`);


            if (this.mapboxToken) {
                try {
                    logger.info('Attempting to get route from Mapbox');
                    return await this.getMapboxRoute(pickupLat, pickupLng, dropLat, dropLng);
                } catch (mapboxError: any) {
                    logger.error(`Mapbox also failed: ${mapboxError.message}`);
                    throw new Error(`Both OSRM and Mapbox failed. OSRM: ${osrmError.message}, Mapbox: ${mapboxError.message}`);
                }
            } else {
                logger.error('No Mapbox token available for fallback');
                throw new Error(`OSRM failed and no Mapbox token configured: ${osrmError.message}`);
            }
        }
    }

    private async getMapboxRoute(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {

        if (!this.mapboxToken) {
            throw new Error("Mapbox token is not configured");
        }

        const url = `${this.mapboxBase}/${pickupLng},${pickupLat};${dropLng},${dropLat}?geometries=geojson&access_token=${this.mapboxToken}`;


        const response = await axios.get(url);
        const route = response.data?.routes?.[0];

        if (!route) {
            throw new Error("No valid route returned from Mapbox");
        }

        const distanceKm = route.distance / 1000;
        const durationMin = route.duration / 60;

        logger.info(`Mapbox route found: ${distanceKm.toFixed(2)} km, ${Math.round(durationMin)} mins`);

        return {
            distanceKm,
            durationMin,
            distanceText: `${distanceKm.toFixed(2)} km`,
            durationText: `${Math.round(durationMin)} mins`,
        };
    }

    private async getOSRMRoute(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {
        const coordinates = `${pickupLng},${pickupLat};${dropLng},${dropLat}`;
        const url = `${this.osrmBase}/${coordinates}`;

        logger.info(`OSRM request URL: ${url}`);

        try {
            const response = await axios.get(url, {
                params: {
                    geometries: 'polyline',
                    overview: 'full'
                },
                timeout: 5000 // 5 second timeout
            });

            const data = response.data;

            if (data.code !== "Ok" || !data.routes?.[0]) {
                throw new Error(`OSRM returned code: ${data.code}, message: ${data.message || 'No routes found'}`);
            }

            const route = data.routes[0];
            const distanceKm = route.distance / 1000;
            const durationMin = route.duration / 60;

            logger.info(`OSRM route found: ${distanceKm.toFixed(2)} km, ${Math.round(durationMin)} mins`);

            return {
                distanceKm,
                durationMin,
                distanceText: `${distanceKm.toFixed(2)} km`,
                durationText: `${Math.round(durationMin)} mins`,
            };
        } catch (error: any) {
            if (error.response) {
                throw new Error(`OSRM HTTP ${error.response.status}: ${error.response.statusText}`);
            } else if (error.request) {
                throw new Error(`OSRM no response - server may be down`);
            } else {
                throw new Error(`OSRM error: ${error.message}`);
            }
        }
    }
}