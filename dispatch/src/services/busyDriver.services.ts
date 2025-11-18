import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {DriverWithDistance, Job} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {ZoneService} from './ZoneService';
import {SpatialService} from '../infrastructure/spatial';
import {MapboxService} from './MapboxService';
import {ObjectId} from 'mongodb';
import {getMongoDB} from "../infrastructure/mongo";

interface AssignedBooking {
    jobId: string;
    bookingId: string;
    driverId: string;
    dropLat: number;
    dropLng: number;
    pickupLat: number;
    pickupLng: number;
    status: string;
}

export class BusyDriverService {
    private driverLocationService: DriverLocationService;
    private zoneService: ZoneService;
    private spatialService: SpatialService;
    private mapboxService: MapboxService;
    private readonly DROP_TO_PICKUP_RADIUS = 15; // 5 km between current drop and new pickup
    private readonly DRIVER_TO_DROP_THRESHOLD = 2; // 2 km - driver should be near their drop point

    constructor(driverLocationService: DriverLocationService, zoneService: ZoneService) {
        this.driverLocationService = driverLocationService;
        this.zoneService = zoneService;
        this.spatialService = new SpatialService();
        this.mapboxService = new MapboxService();
    }

    async getBusyDriversForJob(job: Job, customerId: string): Promise<string[] | null> {
        const startTime = Date.now();

        try {
            const [zoneIds, blockedSet] = await Promise.all([this.zoneService.getZoneForJob(job), this.getCustomerBlockedDrivers(customerId)]);

            if (!zoneIds || zoneIds.length === 0) {
                logger.warn(`No zone found for job ${job.id}`);
                return null;
            }

            logger.info(`Looking for busy drivers with matching drop-to-pickup routes in zones: ${zoneIds.join(', ')}`);

            const busyDrivers = await this.findBusyDriversNearingCompletion(job.pickupLat, job.pickupLng, zoneIds, blockedSet);

            if (busyDrivers && busyDrivers.length > 0) {
                logger.info(`Found ${busyDrivers.length} busy drivers nearing completion in ${Date.now() - startTime}ms`);
                return busyDrivers;
            }

            logger.info(`No matching busy drivers found for job ${job.id} in ${Date.now() - startTime}ms`);
            return [];
        } catch (error: any) {
            logger.error(`Busy driver search failed for job ${job.id}: ${error.message}`);
            return null;
        }
    }

    private async findBusyDriversNearingCompletion(newJobPickupLat: number, newJobPickupLng: number, jobZoneIds: string[], blockedSet: Set<string>): Promise<string[]> {
        try {
            logger.info(`Searching for busy drivers whose drop point is within ${this.DROP_TO_PICKUP_RADIUS}km of new job pickup`);

            const assignedBookings = await this.getAssignedBookings();

            if (assignedBookings.length === 0) {
                logger.info('No assigned bookings found');
                return [];
            }

            logger.info(`Found ${assignedBookings.length} assigned bookings to analyze`);


            const matchingBookings: Array<{
                booking: AssignedBooking; dropToPickupDistance: number;
            }> = [];

            for (const booking of assignedBookings) {

                try {
                    const mapboxResult = await this.mapboxService.getDistanceAndDuration(booking.dropLat, booking.dropLng, newJobPickupLat, newJobPickupLng);
                    const dropToPickupDistance = mapboxResult.distanceKm;
                    if (dropToPickupDistance <= this.DROP_TO_PICKUP_RADIUS) {
                        matchingBookings.push({
                            booking, dropToPickupDistance
                        });

                        logger.info(`Booking ${booking.bookingId} matches: Driver ${booking.driverId}, ` + `drop-to-pickup distance=${dropToPickupDistance.toFixed(2)}km (via Mapbox)`);
                    }
                } catch (error: any) {
                    logger.warn(`Mapbox API failed for booking ${booking.bookingId}, falling back to straight-line distance: ${error.message}`);
                    const dropToPickupDistance = this.spatialService.calculateDistance(booking.dropLat, booking.dropLng, newJobPickupLat, newJobPickupLng);

                    if (dropToPickupDistance <= this.DROP_TO_PICKUP_RADIUS) {
                        matchingBookings.push({
                            booking, dropToPickupDistance
                        });

                        logger.info(`Booking ${booking.bookingId} matches: Driver ${booking.driverId}, ` + `drop-to-pickup distance=${dropToPickupDistance.toFixed(2)}km (straight-line)`);
                    }
                }
            }
            if (matchingBookings.length === 0) {
                logger.info('No bookings found with matching drop-to-pickup routes');
                return [];
            }
            logger.info(`Found ${matchingBookings.length} bookings with drop points near new job pickup`);
            const eligibleDrivers: Array<{
                driverId: string; dropToPickupDistance: number; driverToDropDistance: number;
            }> = [];

            for (const {booking, dropToPickupDistance} of matchingBookings) {
                if (blockedSet.has(booking.driverId)) {
                    logger.debug(`Driver ${booking.driverId} is blocked by customer`);
                    continue;
                }
                const driverData = await this.getDriverLocation(booking.driverId);

                if (!driverData) {
                    logger.debug(`Could not fetch location for driver ${booking.driverId}`);
                    continue;
                }
                const driverApprovedZones = Array.isArray(driverData.approved_zones) ? driverData.approved_zones.map(String) : [];
                if (driverApprovedZones.length > 0 && !jobZoneIds.some(zoneId => driverApprovedZones.includes(zoneId))) {
                    logger.debug(`Driver ${booking.driverId} not in approved zones`);
                    continue;
                }
                const [lng, lat] = driverData.location?.coordinates || [];
                if (!lat || !lng) {
                    logger.debug(`Invalid coordinates for driver ${booking.driverId}`);
                    continue;
                }
                let driverToDropDistance: number;
                try {
                    const mapboxResult = await this.mapboxService.getDistanceAndDuration(lat, lng, booking.dropLat, booking.dropLng);
                    driverToDropDistance = mapboxResult.distanceKm;
                    logger.debug(`Driver ${booking.driverId} to drop distance: ${driverToDropDistance.toFixed(2)}km (via Mapbox)`);
                } catch (error: any) {
                    logger.warn(`Mapbox API failed for driver ${booking.driverId}, falling back to straight-line distance: ${error.message}`);
                    driverToDropDistance = this.spatialService.calculateDistance(lat, lng, booking.dropLat, booking.dropLng);
                    logger.debug(`Driver ${booking.driverId} to drop distance: ${driverToDropDistance.toFixed(2)}km (straight-line)`);
                }

                if (driverToDropDistance <= this.DRIVER_TO_DROP_THRESHOLD) {
                    eligibleDrivers.push({
                        driverId: booking.driverId, dropToPickupDistance, driverToDropDistance
                    });

                    logger.info(`Driver ${booking.driverId} is eligible: ` + `current-to-drop=${driverToDropDistance.toFixed(2)}km, ` + `drop-to-newPickup=${dropToPickupDistance.toFixed(2)}km`);
                } else {
                    logger.debug(`Driver ${booking.driverId} is too far from drop point: ` + `${driverToDropDistance.toFixed(2)}km (threshold: ${this.DRIVER_TO_DROP_THRESHOLD}km)`);
                }
            }


            const sortedDrivers = eligibleDrivers
                .sort((a, b) => a.dropToPickupDistance - b.dropToPickupDistance)
                .map(d => d.driverId);

            logger.info(`Found ${sortedDrivers.length} eligible busy drivers nearing completion ` + `with matching routes`);
            console.log('Eligible busy drivers (sorted by route efficiency):', sortedDrivers);

            return sortedDrivers;
        } catch (error: any) {
            logger.error(`Failed to find busy drivers: ${error.message}`);
            return [];
        }
    }

    private async getAssignedBookings(): Promise<AssignedBooking[]> {
        try {

            const bookingKeys = await redis.keys('booking:*');

            if (bookingKeys.length === 0) {
                return [];
            }

            logger.info(`Found ${bookingKeys.length} booking keys in Redis`);

            const bookingPromises = bookingKeys.map(async (key) => {
                try {

                    const bookingDataRaw = await redis.call('JSON.GET', key);
                    if (!bookingDataRaw) {
                        logger.debug(`No data found for booking key: ${key}`);
                        return null;
                    }
                    const bookingData = typeof bookingDataRaw === 'string' ? JSON.parse(bookingDataRaw) : bookingDataRaw;

                    if (!bookingData) return null;

                    const status = bookingData.status || bookingData.bookingStatus;
                    if (status === 'finding_driver' || status === 'pending') {
                        logger.debug(`Skipping booking ${key} with status: ${status}`);
                        return null;
                    }
                    const driverId = bookingData.assignedDriver || bookingData.driver?._id || bookingData.driverId;
                    if (!driverId) {
                        logger.debug(`No assigned driver found for booking ${key}`);
                        return null;
                    }

                    const tripAddress = bookingData.tripAddress || [];
                    if (!Array.isArray(tripAddress) || tripAddress.length === 0) {
                        logger.debug(`No trip address found for booking ${key}`);
                        return null;
                    }
                    const dropLocation = tripAddress[tripAddress.length - 1]?.location;
                    if (!dropLocation?.latitude || !dropLocation?.longitude) {
                        logger.debug(`Invalid drop location for booking ${key}`);
                        return null;
                    }
                    const pickupLocation = tripAddress[0]?.location;
                    if (!pickupLocation?.latitude || !pickupLocation?.longitude) {
                        logger.debug(`Invalid pickup location for booking ${key}`);
                        return null;
                    }

                    const keyParts = key.split(':');
                    const bookingId = keyParts[1];
                    const jobId = bookingData._id || bookingData.jobId || bookingId.split('-')[0];
                    logger.debug(`✓ Valid assigned booking found: ${key}, ` + `Driver: ${driverId}, Status: ${status}`);
                    return {
                        jobId: String(jobId),
                        bookingId: bookingId,
                        driverId: String(driverId),
                        dropLat: dropLocation.latitude,
                        dropLng: dropLocation.longitude,
                        pickupLat: pickupLocation.latitude,
                        pickupLng: pickupLocation.longitude,
                        status: status
                    } as AssignedBooking;
                } catch (err: any) {
                    logger.error(`Failed to parse booking ${key}: ${err.message}`);
                    return null;
                }
            });
            const bookings = (await Promise.all(bookingPromises))
                .filter((booking): booking is AssignedBooking => Boolean(booking && booking.driverId && booking.dropLat && booking.dropLng));
            logger.info(`Successfully parsed ${bookings.length} assigned bookings with drivers`);

            return bookings;
        } catch (error: any) {
            logger.error(`Failed to fetch assigned bookings: ${error.message}`);
            return [];
        }
    }

    private async getDriverLocation(driverId: string): Promise<any> {
        try {
            const driverKey = `driver:${driverId}`;
            const driverData = await redis.call('JSON.GET', driverKey, '$');

            if (!driverData) return null;

            const parsed = JSON.parse(driverData as string)?.[0];
            return parsed;
        } catch (error: any) {
            logger.error(`Failed to fetch driver location for ${driverId}: ${error.message}`);
            return null;
        }
    }

    private async getCustomerBlockedDrivers(customerId: string): Promise<Set<string>> {
        try {
            const db = getMongoDB();
            const user = await db.collection('users').findOne({_id: new ObjectId(customerId)}, {projection: {blockDrivers: 1}});

            if (!user) {
                return new Set();
            }

            const blocked = Array.isArray(user?.blockDrivers) ? user.blockDrivers.map(String) : [];

            return new Set(blocked);
        } catch (error: any) {
            logger.error(`Failed to fetch customer blocked drivers: ${error.message}`);
            return new Set();
        }
    }
}