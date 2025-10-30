import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Job, ProcessingMetrics} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {JobProcessingService} from './JobProcessingService';
import {DriverMatchingService} from './DriverMatching.Service';
import {OfferManagementService} from './OfferManagement.Service';
import {ZoneService} from './ZoneService';
import {MapboxService} from './MapboxService'

interface ParsedEventType {
    eventName: string;
    bookingId: string | null;
    isBookingEvent: boolean;
}

export class JobOrchestratorService {
    private driverLocationService: DriverLocationService;
    private jobProcessingService: JobProcessingService;
    private driverMatchingService: DriverMatchingService;
    private offerManagementService: OfferManagementService;
    private zoneService: ZoneService;
    private mapboxService: MapboxService
    private isInitialized: boolean = false;

    private metrics: ProcessingMetrics = {
        rpcRequests: 0,
        jobsProcessed: 0,
        driversMatched: 0,
        offersSent: 0,
        errors: 0,
        apiCalls: {
            handlePaymentCompleted: 0,
            handleDriverResponse: 0,
            handleGetStats: 0,
            handleHealthCheck: 0,
            addJobs: 0,
            addJob: 0
        }
    };

    constructor() {
        this.driverLocationService = new DriverLocationService();
        this.zoneService = new ZoneService();
        this.jobProcessingService = new JobProcessingService();
        this.driverMatchingService = new DriverMatchingService(
            this.driverLocationService,
            this.zoneService
        );
        this.offerManagementService = new OfferManagementService();
        this.mapboxService = new MapboxService();
    }

    async start(): Promise<void> {
        if (this.isInitialized) {
            logger.info('Job Orchestrator already running — skipping re-initialization');
            return;
        }

        if ((this as any)._starting) {
            logger.info('Job Orchestrator is currently starting — please wait');
            return;
        }

        (this as any)._starting = true;

        try {
            logger.info('Starting Job Orchestrator...');


            if (!this.zoneService.isReady || !(await this.zoneService.isReady())) {
                logger.info('Initializing ZoneService...');
                await this.zoneService.init();
                logger.info('ZoneService initialized');
            } else {
                logger.info('ZoneService already ready');
            }

            //  Refresh driver cache with timeout guard
            logger.info(' Refreshing driver cache...');
            const refreshPromise = this.driverLocationService.refreshDriverCache();
            const timeoutPromise = new Promise((_, reject) =>
                setTimeout(() => reject(new Error('Driver cache refresh timed out after 10s')), 10_000)
            );

            await Promise.race([refreshPromise, timeoutPromise]);
            logger.info('Driver cache refreshed');


            if (!(this as any)._cacheIntervalSet) {
                const refreshInterval = 30_000;
                setInterval(async () => {
                    try {
                        logger.info('Periodic driver cache refresh triggered');
                        await this.driverLocationService.refreshDriverCache();
                        logger.info('Periodic driver cache refresh done');
                    } catch (error: any) {
                        logger.error(`Driver cache refresh failed: ${error.message}`);
                    }
                }, refreshInterval);

                (this as any)._cacheIntervalSet = true;
            }

            this.isInitialized = true;
            logger.info('Job Orchestrator started successfully');
        } catch (error: any) {
            logger.error(`Job Orchestrator startup failed: ${error.message}`);
            this.isInitialized = false;
            throw error;
        } finally {
            (this as any)._starting = false;
        }
    }


    stop(): void {
        this.isInitialized = false;
        logger.info(' Job Orchestrator stopped');
    }

    isReady(): boolean {
        return this.isInitialized;
    }

    // ----------------- Event processng and type  -----------------

    async handleRPCRequest(data: any): Promise<any> {
        if (!this.isInitialized) {
            logger.error('Job Orchestrator not initialized - rejecting request');
            return {
                success: false,
                error: 'Service not initialized',
                status: 'SERVICE_NOT_READY'
            };
        }

        const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const eventType = data.type;
        logger.info(`RPC Request Received - Type: ${eventType}, RequestId: ${requestId}`);
        this.metrics.rpcRequests++;
        try {
            let result;
            if (
                eventType.startsWith('newBookingPlaced') ||
                eventType.startsWith('newJob.request') ||
                eventType.startsWith('booking.created') ||
                eventType.startsWith('ride.requested')
            ) {
                this.metrics.apiCalls.handlePaymentCompleted++;
                result = await this.handleNewJobEvent(data, data.bookingId || null);
            } else {
                switch (eventType) {
                    case 'driver.response':
                        this.metrics.apiCalls.handleDriverResponse++;
                        result = await this.handleDriverResponse(data);
                        break;

                    default:
                        logger.warn(`Unknown RPC request type: ${eventType}`);
                        return {
                            success: false,
                            error: 'Unknown request type',
                            receivedType: eventType
                        };
                }
            }
            logger.info(`RPC Request Completed - Type: ${eventType}, RequestId: ${requestId}, Success: ${result.success}`);
            return result;
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`RPC Request Failed - Type: ${eventType}, RequestId: ${requestId}, Error: ${error.message}`);
            return {
                success: false,
                error: error.message,
                type: eventType,
                requestId
            };
        }
    }


    // ----------------- Driver Response Handler -----------------

    async handleDriverResponse(data: any): Promise<any> {
        const {driverId, jobId, action, reason} = data;
        logger.info(`Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}, Reason: ${reason || 'N/A'}`);

        try {
            if (action === 'accept') {
                await this.offerManagementService.assignDriverToJob(jobId, driverId);
                this.jobProcessingService.removeJob(jobId);
                logger.info(`Driver Accepted - Job: ${jobId}, Driver: ${driverId}`);
                return {
                    success: true,
                    message: 'Driver assigned to job',
                    jobId,
                    driverId
                };
            } else if (action === 'reject') {
                await this.offerManagementService.handleDriverRejection(jobId, driverId, reason);

                const job = this.jobProcessingService.getJob(jobId);
                if (job) {
                    const nearbyDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
                    const alternativeDrivers = await this.offerManagementService.findAlternativeDrivers(jobId, nearbyDrivers);

                    if (alternativeDrivers.length > 0) {
                        await this.offerManagementService.sendOffers(job, alternativeDrivers);
                        logger.info(` Alternative offers sent - Job: ${jobId}, Drivers: ${alternativeDrivers.length}`);
                        return {
                            success: true,
                            message: 'Searching for alternative drivers',
                            jobId,
                            alternativeDriversFound: alternativeDrivers.length
                        };
                    } else {
                        logger.warn(` No alternative drivers found - Job: ${jobId}`);
                        return {
                            success: false,
                            message: 'No alternative drivers available',
                            jobId
                        };
                    }
                } else {
                    logger.warn(`Job not found - Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Job not found',
                        jobId
                    };
                }
            } else {
                logger.warn(`Unknown Driver Action - Action: ${action}`);
                return {
                    success: false,
                    error: 'Unknown driver action',
                    action
                };
            }
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(` Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
            return {
                success: false,
                error: error.message,
                jobId,
                driverId
            };
        }
    }

    // ----------------- New Job Event Handler -----------------

    private async handleNewJobEvent(data: any, bookingId: string | null): Promise<any> {
        const startTime = Date.now();

        if (!data?.payload) {
            logger.error(' No payload in booking event');
            return { success: false, error: 'No payload in booking event', receivedData: data };
        }

        const payload = data.payload;
        const jobId = bookingId || payload._id;

        if (!jobId) {
            logger.error(' No booking ID found in event type or payload');
            return { success: false, error: 'Missing booking ID', eventType: data.type };
        }

        const pickupData = payload.tripAddress?.[0];
        const dropData = payload.tripAddress?.[payload.tripAddress.length - 1];

        const pickup = pickupData?.location
            ? { latitude: pickupData.location.latitude, longitude: pickupData.location.longitude }
            : null;

        const drop = dropData?.location
            ? { latitude: dropData.location.latitude, longitude: dropData.location.longitude }
            : null;

        if (!pickup) {
            logger.error(`Missing pickup coordinates for Job ${jobId}`);
            return { success: false, error: "Pickup coordinates missing", jobId };
        }

        const customer = payload.customer;

        if (!customer?._id) {
            logger.error(` Invalid customer data — Job ${jobId}`);
            return { success: false, error: "Invalid customer data", jobId };
        }


        const job: Job = {
            id: jobId,
            customerId: payload.customer._id,
            pickupLat: pickup.latitude,
            pickupLng: pickup.longitude,
            fare: payload.grandTotal || 0,
            vehicleType: payload.selectedVehicle?.name || 'Unknown',
            tripAddress: payload.tripAddress || '',
            timestamp: payload.createdAt ? new Date(payload.createdAt).getTime() : Date.now(),

        };

        try {
            if (drop) {   /// it can be drop just to process
                const eta = await this.mapboxService.getDistanceAndDuration(
                    pickup.latitude,
                    pickup.longitude,
                    drop.latitude,
                    drop.longitude
                );
                job.dropLat = drop.latitude;
                job.dropLng = drop.longitude;
                job.customer = {
                    time: eta.durationText,
                    distance: eta.distanceText,
                    fullName: payload.customer.fullName || 'Unknown',
                    avatar: payload.customer.avatar || '',
                };

                logger.info(
                    ` ETA Calculated for Job ${job.id} — ${eta.distanceText}, ${eta.durationText}`
                );
            } else {
                logger.warn(`Pickup or drop missing — Skipping ETA for Job ${job.id}`);
            }
        } catch (error: any) {
            logger.error(` Mapbox ETA fetch failed for Job ${job.id}: ${error.message}`);
            job.rideDetails = { estimatedTime: "0 mins", estimatedDistance: "0 km" };
        }

        try {
            const matchedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
            const searchTime = Date.now() - startTime;

            if (matchedDrivers.length > 0) {
                const driversWithEta = await Promise.all(
                    matchedDrivers.map(async (driverId) => {
                        try {
                            // we get drivers id only form
                            const driverDataJson = await redis.call('JSON.GET', `driver:${driverId}`);
                            if (!driverDataJson) {
                                logger.warn(`Driver data not found in Redis in matched service part - ${driverId}`);
                                return null;
                            }

                            const driverData = typeof driverDataJson === 'string'
                                ? JSON.parse(driverDataJson)
                                : driverDataJson;

                            const lat = driverData?.location?.coordinates?.[1];
                            const lng = driverData?.location?.coordinates?.[0];

                            if (!lat || !lng) {
                                logger.warn(`Invalid driver coordinates - ${driverId}`);
                                return null;
                            }

                            // Get ETA from driver → pickup location
                            const estimatedArrival = await this.mapboxService.getDistanceAndDuration(
                                lat,
                                lng,
                                pickup.latitude,
                                pickup.longitude
                            );
                            job.rideDetails = {
                                estimatedTime: estimatedArrival.durationText,
                                estimatedDistance: estimatedArrival.distanceText,
                            };

                            return {
                                driverId,
                                distanceToPickup: estimatedArrival.distanceText,
                                etaToPickup: estimatedArrival.durationText,
                            };
                        } catch (error: any) {
                            logger.error(`ETA calculation failed for driver ${driverId}: ${error.message}`);
                            return null;
                        }
                    })
                );

                const offerResult = await this.offerManagementService.sendOffers(job, matchedDrivers);


                this.metrics.driversMatched += matchedDrivers.length;
                this.metrics.offersSent += offerResult.successful;

                logger.info(
                    `Job Matched - JobId: ${job.id}, Drivers: ${matchedDrivers.length}, Offers Sent: ${offerResult.successful}, Search Time: ${searchTime}ms`
                );

                return {
                    success: true,
                    message: 'Drivers found and offers sent',
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: matchedDrivers.length,
                    driverIds: matchedDrivers,
                    offersSent: offerResult.successful,
                    offersFailed: offerResult.failed,
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            } else {
                logger.warn(`No Drivers Found - JobId: ${job.id}`);
                return {
                    success: false,
                    message: 'No drivers available',
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: 0,
                    driverIds: [],
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            }
        } catch (error: any) {
            this.metrics.errors++;
            const searchTime = Date.now() - startTime;

            logger.error(
                `Driver Search Failed - JobId: ${job.id}, Error: ${error.message}, Search Time: ${searchTime}ms`
            );

            return {
                success: false,
                message: 'Driver search failed',
                error: error.message,
                jobId: job.id,
                orderNo: payload.orderNo,
                driversFound: 0,
                driverIds: [],
                searchTimeMs: searchTime,
                timestamp: new Date().toISOString(),
            };
        }
    }

}

export const jobOrchestratorService = new JobOrchestratorService();