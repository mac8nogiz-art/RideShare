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
            logger.info('⏳ Job Orchestrator is currently starting — please wait');
            return;
        }

        (this as any)._starting = true;

        try {
            logger.info('🚀 Starting Job Orchestrator...');


            if (!this.zoneService.isReady || !(await this.zoneService.isReady())) {
                logger.info('🧭 Initializing ZoneService...');
                await this.zoneService.init();
                logger.info('ZoneService initialized');
            } else {
                logger.info('ZoneService already ready');
            }

            //  Refresh driver cache with timeout guard
            logger.info('🔄 Refreshing driver cache...');
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

    // ----------------- Event Type Parser -----------------

    private parseEventType(type: string): ParsedEventType {
        // Handle dynamic event types like "newBookingPlaced-6900561da9ea6f1305d29aba"
        const bookingEventPatterns = [
            'newBookingPlaced',
            'newJob.request',
            'booking.created',
            'ride.requested'
        ];

        // Check if this is a booking event
        const isBookingEvent = bookingEventPatterns.some(pattern => type.startsWith(pattern));

        if (isBookingEvent) {
            const parts = type.split('-');

            if (parts.length > 1) {
                // Extract booking ID from type like "newBookingPlaced-6900561da9ea6f1305d29aba"
                return {
                    eventName: parts[0],
                    bookingId: parts.slice(1).join('-'), // Handle multiple dashes
                    isBookingEvent: true
                };
            } else {
                // Legacy format without booking ID in type
                return {
                    eventName: type,
                    bookingId: null,
                    isBookingEvent: true
                };
            }
        }

        // Handle other event types like "driver.response", "get.stats", etc.
        return {
            eventName: type,
            bookingId: null,
            isBookingEvent: false
        };
    }

    // ----------------- RPC Event Handlers -----------------

    async handleRPCRequest(data: any): Promise<any> {
        // Check if service is initialized
        if (!this.isInitialized) {
            logger.error('❌ Job Orchestrator not initialized - rejecting request');
            return {
                success: false,
                error: 'Service not initialized',
                status: 'SERVICE_NOT_READY'
            };
        }

        const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

        // Parse the event type dynamically
        const parsed = this.parseEventType(data.type);

        logger.info(`RPC Request Received - OriginalType: ${data.type}, EventName: ${parsed.eventName}, BookingId: ${parsed.bookingId || 'N/A'}, IsBookingEvent: ${parsed.isBookingEvent}, RequestId: ${requestId}`);

        this.metrics.rpcRequests++;

        try {
            let result;

            // Route based on event type
            if (parsed.isBookingEvent) {
                // Handle all booking-related events
                this.metrics.apiCalls.handlePaymentCompleted++;
                result = await this.handleNewJobEvent(data, parsed.bookingId);
            } else {
                // Handle other event types
                switch (parsed.eventName) {
                    case 'driver.response':
                        this.metrics.apiCalls.handleDriverResponse++;
                        result = await this.handleDriverResponse(data);
                        break;
                    case 'get.stats':
                        this.metrics.apiCalls.handleGetStats++;
                        result = await this.handleGetStats(data);
                        break;
                    case 'health.check':
                        this.metrics.apiCalls.handleHealthCheck++;
                        result = await this.handleHealthCheck(data);
                        break;
                    default:
                        logger.warn(`Unknown RPC request type: ${data.type} (parsed as: ${parsed.eventName})`);
                        return {
                            success: false,
                            error: 'Unknown request type',
                            receivedType: data.type,
                            parsedEventName: parsed.eventName
                        };
                }
            }

            logger.info(`RPC Request Completed - Type: ${data.type}, RequestId: ${requestId}, Success: ${result.success}`);
            return result;
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`RPC Request Failed - Type: ${data.type}, RequestId: ${requestId}, Error: ${error.message}, Stack: ${error.stack}`);
            return {
                success: false,
                error: error.message,
                type: data.type,
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
                        logger.info(`🔄 Alternative offers sent - Job: ${jobId}, Drivers: ${alternativeDrivers.length}`);
                        return {
                            success: true,
                            message: 'Searching for alternative drivers',
                            jobId,
                            alternativeDriversFound: alternativeDrivers.length
                        };
                    } else {
                        logger.warn(`⚠️ No alternative drivers found - Job: ${jobId}`);
                        return {
                            success: false,
                            message: 'No alternative drivers available',
                            jobId
                        };
                    }
                } else {
                    logger.warn(`⚠️ Job not found - Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Job not found',
                        jobId
                    };
                }
            } else {
                logger.warn(`❌ Unknown Driver Action - Action: ${action}`);
                return {
                    success: false,
                    error: 'Unknown driver action',
                    action
                };
            }
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`❌ Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
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


        let pickup: { latitude: number; longitude: number } | null = null;
        let drop: { latitude: number; longitude: number } | null = null;

        if (Array.isArray(payload.tripAddress) && payload.tripAddress.length > 0) {
            const pickupAddress =
                payload.tripAddress.find(
                    (a: any) =>
                        a.markerType === 'pickup' ||
                        a.markerType === 'origin' ||
                        a.sequenceNumber === 1
                ) || payload.tripAddress[0];

            const dropAddress =
                payload.tripAddress.find(
                    (a: any) =>
                        a.markerType === 'drop' ||
                        a.markerType === 'destination' ||
                        a.sequenceNumber === payload.tripAddress.length
                ) || payload.tripAddress[payload.tripAddress.length - 1];

            if (pickupAddress?.location?.latitude && pickupAddress?.location?.longitude) {
                pickup = {
                    latitude: pickupAddress.location.latitude,
                    longitude: pickupAddress.location.longitude,
                };
            }

            if (dropAddress?.location?.latitude && dropAddress?.location?.longitude) {
                drop = {
                    latitude: dropAddress.location.latitude,
                    longitude: dropAddress.location.longitude,
                };
            }
        }

        if ((!pickup || !pickup.latitude || !pickup.longitude) && payload.firstTripAddressGeoLocation?.coordinates) {
            pickup = {
                latitude: payload.firstTripAddressGeoLocation.coordinates[1],
                longitude: payload.firstTripAddressGeoLocation.coordinates[0],
            };
        }

        if ((!drop || !drop.latitude || !drop.longitude) && payload.lastTripAddressGeoLocation?.coordinates) {
            drop = {
                latitude: payload.lastTripAddressGeoLocation.coordinates[1],
                longitude: payload.lastTripAddressGeoLocation.coordinates[0],
            };
        }

        if (!pickup) {
            logger.error(`Missing pickup coordinates for booking ${jobId}`);
            return { success: false, error: 'Pickup coordinates missing', jobId };
        }


        if (!payload.customer || !payload.customer._id) {
            logger.error(`Invalid customer data - BookingId: ${jobId}`);
            return { success: false, error: 'Invalid customer data', jobId };
        }

        const job: Job = {
            id: jobId,
            customerId: payload.customer._id,
            pickupLat: pickup.latitude,
            pickupLng: pickup.longitude,
            fare: payload.grandTotal || 0,
            vehicleType: payload.selectedVehicle?.name || 'Unknown',
            timestamp: payload.createdAt ? new Date(payload.createdAt).getTime() : Date.now(),
        };

        try {
            if (drop?.latitude && drop?.longitude && pickup?.latitude && pickup?.longitude) {
                const eta = await this.mapboxService.getDistanceAndDuration(
                    pickup.latitude,
                    pickup.longitude,
                    drop.latitude,
                    drop.longitude
                );

                job.dropLat = drop.latitude;
                job.dropLng = drop.longitude;


                job.rideDetails = {
                    estimatedTime: eta.durationText,
                    estimatedDistance: eta.distanceText,
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
            await this.jobProcessingService.addJob(job);
            logger.info(`Job added to processing queue - JobId: ${job.id}`);
        } catch (error: any) {
            logger.error(` Failed to add job to queue - JobId: ${job.id}, Error: ${error.message}`);
            return {
                success: false,
                error: 'Failed to add job to queue',
                jobId: job.id,
                errorDetails: error.message,
            };
        }

        try {
            const matchedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
            const searchTime = Date.now() - startTime;

            if (matchedDrivers.length > 0) {
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


    // ----------------- Stats & Health -----------------

    getStats() {
        return {
            activeJobs: this.jobProcessingService.getActiveJobsCount(),
            cachedDrivers: this.driverLocationService.getDriverCacheSize(),
            processingRate: Math.round(this.jobProcessingService.getActiveJobsCount() * 10),
            cacheHitRate: this.driverLocationService.getDriverCacheSize() > 0 ? 0.95 : 0,
            metrics: this.metrics,
            isInitialized: this.isInitialized,
            timestamp: new Date().toISOString()
        };
    }

    private async handleGetStats(data: any): Promise<any> {
        logger.info(`Stats Request - Client: ${data.clientId || 'unknown'}`);
        return {
            success: true,
            stats: this.getStats()
        };
    }

    private async handleHealthCheck(data: any): Promise<any> {
        logger.info(' Health Check Request');

        try {
            const redisStart = Date.now();
            await redis.ping();
            const redisLatency = Date.now() - redisStart;

            const healthData = {
                success: true,
                status: 'healthy',
                redis: 'connected',
                redisLatencyMs: redisLatency,
                zoneService: this.zoneService.isReady() ? 'ready' : 'not_initialized',
                orchestratorInitialized: this.isInitialized,
                ...this.getStats()
            };

            logger.info(` Health Check Passed - Redis Latency: ${redisLatency}ms`);
            return healthData;
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(` Health Check Failed - Redis: disconnected, Error: ${error.message}`);
            return {
                success: false,
                status: 'unhealthy',
                redis: 'disconnected',
                error: error.message
            };
        }
    }
}

export const jobOrchestratorService = new JobOrchestratorService();