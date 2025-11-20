import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Job, ProcessingMetrics} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {DriverMatchingService} from './DriverMatching.Service';
import {OfferManagementService} from './OfferManagement.Service';
import {MatchedDriverService} from './MatchedDriver.services';
import {BusyDriverService} from "./busyDriver.services";
import {FreeDriverService} from "./freeDriverService";
import {ZoneService} from './ZoneService';
import {MapboxService} from './MapboxService'
import {
    newJobEventSchema, driverResponseSchema, validateSchema
} from './validation';
import {
    registerOfferExpiryCallback,
    registerMatchedBucketExpiryCallback,
    registerMatchedDriverTriggerCallback,
    triggerMatchedDriverFlow
} from '../infrastructure/bullmq';

export class JobOrchestratorService {
    private driverLocationService: DriverLocationService;
    private driverMatchingService: DriverMatchingService;
    private offerManagementService: OfferManagementService;
    private matchedDriverService: MatchedDriverService;
    private zoneService: ZoneService;
    private busyDriverService: BusyDriverService;
    private freeDriverService: FreeDriverService;
    private mapboxService: MapboxService;
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
        this.mapboxService = new MapboxService();

        this.driverMatchingService = new DriverMatchingService(
            this.driverLocationService,
            this.zoneService
        );

        this.busyDriverService = new BusyDriverService(
            this.driverLocationService,
            this.zoneService
        );

        this.freeDriverService = new FreeDriverService(
            this.driverLocationService,
            this.zoneService
        );

        // Initialize services without circular dependencies
        this.offerManagementService = new OfferManagementService();
        
        this.matchedDriverService = new MatchedDriverService(
            this.busyDriverService,
            this.freeDriverService,
            this.offerManagementService
        );
        
        // Inject matchedDriverService into offerManagementService
        this.offerManagementService.setMatchedDriverService(this.matchedDriverService);

        logger.info('All services initialized with proper dependencies');
    }


    getOfferManagementService(): OfferManagementService {
        return this.offerManagementService;
    }

    async start(): Promise<void> {
        if (this.isInitialized) {
            logger.info('Job Orchestrator already running - skipping re-initialization');
            return;
        }

        if ((this as any)._starting) {
            logger.info('Job Orchestrator is currently starting - please wait');
            return;
        }

        (this as any)._starting = true;

        try {
            logger.info('Starting Job Orchestrator...');

            // Register BullMQ callbacks
            this.registerBullMQCallbacks();

            if (!this.zoneService.isReady || !(await this.zoneService.isReady())) {
                logger.info('Initializing ZoneService...');
                await this.zoneService.init();
                logger.info('ZoneService initialized');
            } else {
                logger.info('ZoneService already ready');
            }

            logger.info('Refreshing driver cache...');

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

            // Queue exhaustion is now handled via BullMQ callbacks

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

    /**
     * Register BullMQ callbacks for offer and bucket expiry tracking
     */
    private registerBullMQCallbacks(): void {
        logger.info('Registering BullMQ callbacks...');

        // Register offer expiry callback
        registerOfferExpiryCallback(async (jobId: string, driverId: string, remainingDrivers: number) => {
            try {
                logger.info(`Offer expiredaaaaa - Job: ${jobId}, Driver: ${driverId}`);
                
                // Get remaining drivers count
                const queueLength = await redis.llen(`job:${jobId}:driver_queue`);
                await this.trackOfferExpiry(jobId, driverId, queueLength);
                
                await this.offerManagementService.handleRegularOfferExpiry(jobId, driverId);
            } catch (error: any) {
                logger.error(`Error handling offer expiry: ${error.message}`);
            }
        });

        // Register matched bucket expiry callback
        registerMatchedBucketExpiryCallback(async (jobId: string, bucketIndex: number) => {
            try {
                logger.info(`Matched bucket expired - Job: ${jobId}, Bucket: ${bucketIndex}`);

                // Track bucket expiry
                await this.trackBucketExpiry(jobId, bucketIndex);

                // Handle bucket expiry via MatchedDriverService
                await this.handleMatchedBucketExpiry(jobId);
            } catch (error: any) {
                logger.error(`Error handling bucket expiry: ${error.message}`);
            }
        });

        // Register matched driver trigger callback
        registerMatchedDriverTriggerCallback(async (jobId: string, jobData: any, reason: string) => {
            try {
                logger.info(`Matched driver trigger - Job: ${jobId}, Reason: ${reason}`);

                // Check if matched flow is already active
                const matchedFlowActive = await redis.get(`job:${jobId}:matched_flow_active`);
                if (matchedFlowActive === '1') {
                    logger.debug(`Matched flow already active for Job ${jobId} - Skipping`);
                    return;
                }

                // Get job data from booking if not provided
                let job = jobData;
                if (!job) {
                    job = await this.getJobFromBooking(jobId);
                }

                if (job) {
                    await this.matchedDriverService.triggerMatchedDriverFlow(job, jobId);
                    logger.info(`Successfully triggered matched driver flow for Job ${jobId}`);
                } else {
                    logger.error(`Could not find job data for ${jobId}`);
                }
            } catch (error: any) {
                logger.error(`Error handling matched driver trigger: ${error.message}`);
            }
        });

        logger.info('BullMQ callbacks registered successfully');
    }

    /**
     * Track offer expiry metrics and update Redis
     */
    private async trackOfferExpiry(jobId: string, driverId: string, remainingDrivers: number): Promise<void> {
        try {
            const trackingKey = `job:${jobId}:offer_expiry_tracking`;
            const timestamp = new Date().toISOString();

            await redis.hset(trackingKey, {
                lastExpiredDriver: driverId,
                lastExpiryTime: timestamp,
                remainingDrivers: remainingDrivers.toString(),
                totalExpiries: (await redis.hincrby(trackingKey, 'totalExpiries', 1)).toString()
            });

            await redis.expire(trackingKey, 3600);

            // Store expiry event in a list for analytics
            const expiryEvent = JSON.stringify({
                driverId,
                timestamp,
                remainingDrivers,
                jobId
            });

            await redis.lpush(`job:${jobId}:expiry_events`, expiryEvent);
            await redis.ltrim(`job:${jobId}:expiry_events`, 0, 99); // Keep last 100 events
            await redis.expire(`job:${jobId}:expiry_events`, 3600);

            logger.debug(`Tracked offer expiry - Job: ${jobId}, Driver: ${driverId}`);
        } catch (error: any) {
            logger.error(`Error tracking offer expiry: ${error.message}`);
        }
    }

    /**
     * Track matched bucket expiry
     */
    private async trackBucketExpiry(jobId: string, bucketIndex: number): Promise<void> {
        try {
            const trackingKey = `job:${jobId}:bucket_expiry_tracking`;
            const timestamp = new Date().toISOString();

            await redis.hset(trackingKey, {
                lastExpiredBucket: bucketIndex.toString(),
                lastBucketExpiryTime: timestamp,
                totalBucketExpiries: (await redis.hincrby(trackingKey, 'totalBucketExpiries', 1)).toString()
            });

            await redis.expire(trackingKey, 3600);

            logger.debug(`Tracked bucket expiry - Job: ${jobId}, Bucket: ${bucketIndex}`);
        } catch (error: any) {
            logger.error(`Error tracking bucket expiry: ${error.message}`);
        }
    }

    /**
     * Handle queue exhaustion by triggering matched driver flow
     */
    private async handleQueueExhaustion(jobId: string): Promise<void> {
        try {
            logger.info(`Handling queue exhaustion for Job ${jobId}`);

            const matchedFlowActive = await redis.get(`job:${jobId}:matched_flow_active`);
            if (matchedFlowActive === '1') {
                logger.debug(`Matched flow already active for Job ${jobId} - Skipping`);
                return;
            }

            const job = await this.getJobFromBooking(jobId);
            if (!job) {
                logger.error(`[Orchestrator] Could not find job data for ${jobId}`);
                return;
            }

            // Trigger matched driver flow
            await this.matchedDriverService.triggerMatchedDriverFlow(job, jobId);

            logger.info(`Successfully triggered matched driver flow for Job ${jobId}`);
        } catch (error: any) {
            logger.error(`Error handling queue exhaustion: ${error.message}`);
        }
    }

    /**
     * Send offer to the next driver in the queue
     */
    private async sendNextQueuedOffer(jobId: string): Promise<void> {
        try {
            const driverQueueKey = `job:${jobId}:driver_queue`;
            const nextDriverId = await redis.lindex(driverQueueKey, 0);

            if (!nextDriverId) {
                logger.warn(`No next driver available for Job ${jobId}`);
                return;
            }

            const job = await this.getJobFromBooking(jobId);
            if (!job) {
                logger.error(`[Orchestrator] Could not find job data for ${jobId}`);
                return;
            }

            logger.info(`Sending offer to next driver: ${nextDriverId} for Job ${jobId}`);

            // OfferManagementService will handle the actual sending
            await this.offerManagementService.sendOfferToNextDriver(jobId, job);

        } catch (error: any) {
            logger.error(`Error sending next queued offer: ${error.message}`);
        }
    }



    private buildJobFromPayload(payload: any, jobId?: string): Job | null {
        try {
            const id = jobId || payload._id;
            const customer = payload.customer;
            const pickupData = payload.tripAddress?.[0];
            const dropData = payload.tripAddress?.[payload.tripAddress.length - 1];

            if (!pickupData?.location || !customer?._id) {
                logger.error(`Invalid payload data for Job ${id}`);
                return null;
            }

            return {
                id,
                customerId: customer._id,
                pickupLat: pickupData.location.latitude,
                pickupLng: pickupData.location.longitude,
                dropLat: dropData?.location?.latitude,
                dropLng: dropData?.location?.longitude,
                fare: payload.grandTotal || 0,
                vehicleType: payload.selectedVehicle?.name || 'Unknown',
                tripAddress: payload.tripAddress || [],
                timestamp: payload.createdAt ? new Date(payload.createdAt).getTime() : Date.now(),
                customer: {
                    fullName: customer.fullName || 'Unknown',
                    avatar: customer.avatar || '',
                    distance: payload.expectedBilling?.kmText || 0,
                    time: payload.expectedBilling?.durationText || 'N/A'
                },
                rideDetails: payload.rideDetails || {
                    estimatedTime: payload.expectedBilling?.durationText || 'N/A',
                    estimatedDistance: payload.expectedBilling?.km || 0
                }
            };
        } catch (error: any) {
            logger.error(`Failed to build job from payload: ${error.message}`);
            return null;
        }
    }

    private async getJobFromBooking(jobId: string): Promise<Job | null> {
        try {
            const patterns = [
                `booking:${jobId}-*`,
                `booking:${jobId}`,
                `booking-${jobId}*`
            ];

            let bookingKey: string | null = null;
            for (const pattern of patterns) {
                const keys = await redis.keys(pattern);
                if (keys.length > 0) {
                    bookingKey = keys[0];
                    break;
                }
            }

            if (!bookingKey) {
                logger.warn(`No booking found for Job ${jobId}`);
                return null;
            }

            const bookingDataJson = await redis.call('JSON.GET', bookingKey) as any;
            if (!bookingDataJson) return null;

            const payload = typeof bookingDataJson === 'string'
                ? JSON.parse(bookingDataJson)
                : bookingDataJson;

            return this.buildJobFromPayload(payload, jobId);
        } catch (error: any) {
            logger.error(`Failed to get job from booking for ${jobId}: ${error.message}`);
            return null;
        }
    }

    stop(): void {
        this.isInitialized = false;
        logger.info('Job Orchestrator stopped');
    }

    isReady(): boolean {
        return this.isInitialized;
    }

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
        const eventType = data.type || data.headers?.['event-type'];

        logger.info(`RPC Request Received - Type: ${eventType}, RequestId: ${requestId}`);
        this.metrics.rpcRequests++;

        try {
            let result;

            if (eventType.startsWith('newBookingPlaced') || eventType.startsWith('newJob.request')) {
                const validation = await validateSchema(newJobEventSchema, data);
                if (!validation.valid) {
                    logger.error(`Validation failed for new job event: ${validation.errors?.join(', ')}`);
                    return {success: false, error: 'Validation failed', details: validation.errors};
                }
                this.metrics.apiCalls.handlePaymentCompleted++;
                result = await this.handleNewJobEvent(validation.data, data.bookingId || null);
            } else if (eventType === 'newBooking.response') {
                const validation = await validateSchema(driverResponseSchema, data);
                if (!validation.valid) {
                    logger.error(`Validation failed for driver response: ${validation.errors?.join(', ')}`);
                    return {success: false, error: 'Validation failed', details: validation.errors};
                }
                this.metrics.apiCalls.handleDriverResponse++;
                result = await this.handleDriverResponse(validation.data);
            } else {
                logger.warn(`Unknown RPC request type: ${eventType}`);
                return {success: false, error: 'Unknown request type', receivedType: eventType};
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

    async handleMatchedBucketExpiry(jobId: string): Promise<void> {
        try {
            logger.info(`Matched bucket expired for Job ${jobId}`);

            const status = await redis.get(`job:${jobId}:status`);
            if (status === 'assigned' || status === 'cancelled') {
                logger.info(`Job ${jobId} already ${status} - ignoring bucket expiry`);
                await this.matchedDriverService.cleanupMatchedFlow(jobId);
                return;
            }

            const flowActive = await redis.get(`job:${jobId}:matched_flow_active`);
            if (flowActive !== '1') {
                logger.info(`Matched flow not active for Job ${jobId}`);
                return;
            }

            const bucketKey = `job:${jobId}:matched_bucket`;
            const bucketData = await redis.hgetall(bucketKey);

            if (bucketData && bucketData.isLastBucket === 'true') {
                logger.info(`Last bucket expired for Job ${jobId}`);
                await this.matchedDriverService.cleanupMatchedFlow(jobId);
                return;
            }

            const currentIndex = parseInt(await redis.get(`job:${jobId}:current_bucket_index`) || '0');
            const nextIndex = currentIndex + 1;

            await redis.set(`job:${jobId}:current_bucket_index`, nextIndex.toString(), 'EX', 3600);

            logger.info(`Moving to Bucket ${nextIndex + 1} for Job ${jobId}`);

            const job = await this.getJobFromBooking(jobId);
            if (job) {
                await this.offerManagementService.sendNextMatchedBucket(jobId, job);
            } else {
                logger.error(`Could not find job data for ${jobId}`);
                await this.matchedDriverService.cleanupMatchedFlow(jobId);
            }

        } catch (error: any) {
            logger.error(`Bucket expiry error: ${error.message}`);
            await this.matchedDriverService.cleanupMatchedFlow(jobId);
        }
    }

    async handleDriverResponse(data: any): Promise<any> {
        const {driverId, jobId, action, reason} = data;
        logger.info(`Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}`);

        try {
            if (action === 'accept') {
                const bookingPattern = `booking:${jobId}-*`;
                const bookingKeys = await redis.keys(bookingPattern);

                let customerId: string | undefined;
                if (bookingKeys.length > 0) {
                    const bookingData = await redis.call('JSON.GET', bookingKeys[0], '$.customer._id') as any;
                    if (bookingData && typeof bookingData === 'string') {
                        const parsed = JSON.parse(bookingData);
                        customerId = Array.isArray(parsed) ? parsed[0] : parsed;
                    }
                }

                await this.offerManagementService.assignDriverToJob(jobId, driverId, customerId);

                logger.info(`Driver Accepted - Job: ${jobId}, Driver: ${driverId}`);
                return {
                    success: true,
                    message: 'Driver assigned to job',
                    jobId,
                    driverId
                };

            } else if (action === 'reject') {
                const job = await this.getJobFromBooking(jobId);

                if (!job) {
                    logger.warn(`No booking found for Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Job not found in Redis',
                        jobId
                    };
                }

                await this.offerManagementService.handleDriverRejection(jobId, job.customerId, driverId, reason);

                const matchedFlowActive = await redis.get(`job:${jobId}:matched_flow_active`);
                if (matchedFlowActive === '1') {
                    logger.info(`Driver rejected during matched flow - Job: ${jobId}, continuing with remaining batches`);
                    return {
                        success: true,
                        message: 'Driver rejection processed, matched flow continues',
                        jobId,
                        driverId
                    };
                }

                const categorizedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);

                if (!categorizedDrivers) {
                    logger.warn(`No drivers available - Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'No alternative drivers available',
                        jobId
                    };
                }

                const allDrivers = [
                    ...categorizedDrivers.favDriver,
                    ...categorizedDrivers.priorityDrivers,
                    ...categorizedDrivers.newDrivers,
                    ...categorizedDrivers.nonPriorityDrivers,
                    ...categorizedDrivers.remainingDrivers,
                ];

                const alternativeDrivers = await this.offerManagementService.findAlternativeDrivers(jobId, allDrivers);

                if (alternativeDrivers.length > 0) {
                    await this.offerManagementService.sendOffers(job, alternativeDrivers);
                    logger.info(`Alternative offers sent - Job: ${jobId}, Drivers: ${alternativeDrivers.length}`);
                    return {
                        success: true,
                        message: 'Searching for alternative drivers',
                        jobId,
                        alternativeDriversFound: alternativeDrivers.length
                    };
                } else {
                    logger.warn(`No alternative drivers found - Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'No alternative drivers available',
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
            logger.error(`Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
            return {
                success: false,
                error: error.message,
                jobId,
                driverId
            };
        }
    }


    private async handleNewJobEvent(data: any, bookingId: string | null): Promise<any> {
        const startTime = Date.now();
        const payload = data.payload;
        const jobId = payload._id;

        // Extract coordinates
        const pickupData = payload.tripAddress?.[0];
        const dropData = payload.tripAddress?.[payload.tripAddress.length - 1];
        const pickup = pickupData?.location ? {
            latitude: pickupData.location.latitude,
            longitude: pickupData.location.longitude
        } : null;
        const drop = dropData?.location ? {
            latitude: dropData.location.latitude,
            longitude: dropData.location.longitude
        } : null;

        // Validate required fields
        if (!pickup) {
            logger.error(`Missing pickup coordinates for Job ${jobId}`);
            return {success: false, error: 'Pickup coordinates missing', jobId};
        }

        const customer = payload.customer;
        if (!customer?._id) {
            logger.error(`Invalid customer data for Job ${jobId}`);
            return {success: false, error: 'Invalid customer data', jobId};
        }

        // Build initial job object
        const job: Job = {
            id: jobId,
            customerId: customer._id,
            pickupLat: pickup.latitude,
            pickupLng: pickup.longitude,
            fare: payload.grandTotal || 0,
            vehicleType: payload.selectedVehicle?.name || 'Unknown',
            tripAddress: payload.tripAddress || '',
            timestamp: payload.createdAt ? new Date(payload.createdAt).getTime() : Date.now(),
        };

        try {

            await this.storeBookingData(jobId, customer._id, payload);

            if (drop) {
                const tripEta = await this.mapboxService.getDistanceAndDuration(
                    pickup.latitude,
                    pickup.longitude,
                    drop.latitude,
                    drop.longitude
                );

                job.dropLat = drop.latitude;
                job.dropLng = drop.longitude;
                job.rideDetails = {
                    estimatedTime: tripEta.durationText,
                    estimatedDistance: tripEta.distanceKm,
                };

                logger.info(`Trip ETA: ${tripEta.durationText}, ${tripEta.distanceText} for Job ${job.id}`);
            } else {
                job.rideDetails = {estimatedTime: 'N/A', estimatedDistance: 0};
            }


            const categorizedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
            const searchTime = Date.now() - startTime;


            const matchedDrivers = categorizedDrivers ? [
                ...categorizedDrivers.favDriver,
                ...categorizedDrivers.priorityDrivers,
                ...categorizedDrivers.newDrivers,
                ...categorizedDrivers.nonPriorityDrivers,
                ...categorizedDrivers.remainingDrivers,
            ] : [];

            if (matchedDrivers.length === 0) {
                logger.warn(`No drivers found for Job ${job.id}`);

                job.customer = {
                    fullName: customer.fullName || 'Unknown',
                    avatar: customer.avatar || '',
                    distance: 0,
                    time: 'Calculating...',
                };

                // Set flag to trigger matched drivers when queue exhausts
                await redis.set(`job:${job.id}:trigger_matched_on_exhaustion`, '1', 'EX', 3600);
                await triggerMatchedDriverFlow(job.id, job, 'no_drivers_found');

                return {
                    success: false,
                    message: 'No drivers found',
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: 0,
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            }

            logger.info(`Found ${matchedDrivers.length} drivers for Job ${job.id}`);


            let offerETA = null;
            try {
                const firstDriverId = matchedDrivers[0];
                const driverDataJson = await redis.call('JSON.GET', `driver:${firstDriverId}`);

                if (driverDataJson) {
                    const driverData = typeof driverDataJson === 'string' ? JSON.parse(driverDataJson) : driverDataJson;
                    const [lng, lat] = driverData?.location?.coordinates || [];

                    if (lat && lng) {
                        const eta = await this.mapboxService.getDistanceAndDuration(lat, lng, pickup.latitude, pickup.longitude);
                        offerETA = {distanceToPickup: eta.distanceKm, etaToPickup: eta.durationText};
                    }
                }
            } catch (error: any) {
                logger.warn(`ETA calculation failed, using defaults: ${error.message}`);
            }

            // Set customer info with ETA
            job.customer = {
                fullName: customer.fullName || 'Unknown',
                avatar: customer.avatar || '',
                distance: offerETA?.distanceToPickup || 0,
                time: offerETA?.etaToPickup || 'N/A',
            };

            // Send offers to drivers (queue-based flow)
            await this.offerManagementService.sendOffers(job, matchedDrivers);

            this.metrics.driversMatched += matchedDrivers.length;
            this.metrics.offersSent += 1;

            logger.info(`Job ${job.id}: ${matchedDrivers.length} drivers, queue flow started (${searchTime}ms)`);

            return {
                success: true,
                message: 'Drivers found - queue flow started',
                jobId: job.id,
                orderNo: payload.orderNo,
                driversFound: matchedDrivers.length,
                driverIds: matchedDrivers,
                flowType: 'regular',
                searchTimeMs: searchTime,
                timestamp: new Date().toISOString(),
            };

        } catch (error: any) {
            this.metrics.errors++;
            const searchTime = Date.now() - startTime;

            logger.error(`Driver search failed for Job ${job.id}: ${error.message} (${searchTime}ms)`);

            try {
                job.customer = {
                    fullName: customer.fullName || 'Unknown',
                    avatar: customer.avatar || '',
                    distance: 0,
                    time: 'Calculating...',
                };

                // Trigger matched driver flow via BullMQ as fallback
                await triggerMatchedDriverFlow(job.id, job, 'fallback');

                return {
                    success: true,
                    message: 'Driver search failed - matched flow triggered',
                    error: error.message,
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: 0,
                    flowType: 'matched_fallback',
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            } catch (fallbackError: any) {
                logger.error(`All fallback failed for Job ${job.id}: ${fallbackError.message}`);

                return {
                    success: false,
                    message: 'Driver search and fallback failed',
                    error: error.message,
                    fallbackError: fallbackError.message,
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: 0,
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            }
        }
    }

    private async storeBookingData(jobId: string, customerId: string, payload: any): Promise<void> {
        try {
            const bookingKey = `booking:${jobId}-${customerId}-*`;
            await redis.call('JSON.SET', bookingKey, '$', JSON.stringify(payload));
            await redis.expire(bookingKey, 7200);
            logger.debug(`Stored booking data for Job ${jobId}`);
        } catch (error: any) {
            logger.error(`Failed to store booking data for Job ${jobId}: ${error.message}`);
        }
    }
}

export const jobOrchestratorService = new JobOrchestratorService();