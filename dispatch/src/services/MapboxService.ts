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

    /**
     * Get route using either Mapbox (if token available) or OSRM as fallback.
     */
    async getDistanceAndDuration(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {
        if (this.mapboxToken) {
            try {
                return await this.getMapboxRoute(pickupLat, pickupLng, dropLat, dropLng);
            } catch (error: any) {
                logger.warn(`Mapbox failed, using OSRM fallback: ${error.message}`);
                return await this.getOSRMRoute(pickupLat, pickupLng, dropLat, dropLng);
            }
        } else {
            return await this.getOSRMRoute(pickupLat, pickupLng, dropLat, dropLng);
        }
    }

    /**
     * ============= MAPBOX IMPLEMENTATION =============
     */
    private async getMapboxRoute(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {
        const url = `${this.mapboxBase}/${pickupLng},${pickupLat};${dropLng},${dropLat}?geometries=geojson&access_token=${this.mapboxToken}`;
        const response = await axios.get(url);
        const route = response.data?.routes?.[0];

        if (!route) throw new Error("No valid route returned from Mapbox");

        const distanceKm = route.distance / 1000;
        const durationMin = route.duration / 60;

        return {
            distanceKm,
            durationMin,
            distanceText: `${distanceKm.toFixed(2)} km`,
            durationText: `${Math.round(durationMin)} mins`,
        };
    }

    /**
     * ============= OSRM IMPLEMENTATION =============
     * Local fallback if Mapbox not available.
     */
    private async getOSRMRoute(
        pickupLat: number,
        pickupLng: number,
        dropLat: number,
        dropLng: number
    ): Promise<DistanceDurationResult> {
        try {
            const coordinates = `${pickupLng},${pickupLat};${dropLng},${dropLat}`;
            const url = new URL(`${this.osrmBase}/${coordinates}`);
            url.searchParams.append("geometries", "polyline");
            url.searchParams.append("overview", "full");

            const response = await fetch(url.toString());
            const data = await response.json();

            if (data.code !== "Ok" || !data.routes?.[0]) {
                throw new Error("No valid route returned from OSRM");
            }

            const route = data.routes[0];
            const distanceKm = route.distance / 1000;
            const durationMin = route.duration / 60;

            return {
                distanceKm,
                durationMin,
                distanceText: `${distanceKm.toFixed(2)} km`,
                durationText: `${Math.round(durationMin)} mins`,
            };
        } catch (error: any) {
            logger.error(`OSRM Error: ${error.message}`);
            return {
                distanceKm: 0,
                durationMin: 0,
                distanceText: "0 km",
                durationText: "0 mins",
            };
        }
    }

    /**
     * ============= COMPLEX ROUTES (with multiple waypoints) =============
     * Similar to your `getDirectionsWithoutMapbox`, used for trip planning.
     */
    async getRouteWithWaypoints(addresses: any[] = [], isOrigin = false): Promise<any> {
        try {
            let origin, destination;

            if (isOrigin) {
                destination = addresses.shift();
                origin = addresses.pop();
            } else {
                origin = addresses.shift();
                destination = addresses.pop();
            }

            if (
                !origin?.location?.latitude ||
                !origin?.location?.longitude ||
                !destination?.location?.latitude ||
                !destination?.location?.longitude
            ) {
                throw new Error("Invalid coordinates");
            }

            const waypoints = addresses.map(
                (p) => `${p.location.longitude},${p.location.latitude}`
            );

            const coordinates = [
                `${origin.location.longitude},${origin.location.latitude}`,
                ...waypoints,
                `${destination.location.longitude},${destination.location.latitude}`,
            ].join(";");

            const url = new URL(`${this.osrmBase}/${coordinates}`);
            url.searchParams.append("geometries", "polyline");
            url.searchParams.append("steps", "true");
            url.searchParams.append("overview", "full");

            const response = await fetch(url.toString());
            const data = await response.json();

            if (data.code !== "Ok") throw new Error("OSRM routing failed");

            const routes = data.routes.map((route: any) => ({
                legs: route.legs.map((leg: any) => ({
                    steps: leg.steps.map((step: any) => ({
                        ...step,
                        maneuver: step.maneuver?.modifier || step.maneuver?.type || "",
                        distance: { value: step.distance },
                        duration: { value: step.duration },
                    })),
                    distance: { value: leg.distance },
                    duration: { value: leg.duration },
                })),
                overview_polyline: { points: route.geometry },
            }));

            // Ferry route check
            for (const leg of routes[0].legs) {
                for (const step of leg.steps) {
                    if (step.maneuver === "ferry") return false;
                }
            }

            return routes;
        } catch (error: any) {
            logger.error(`RouteWithWaypoints Error: ${error.message}`);
            return false;
        }
    }
}
