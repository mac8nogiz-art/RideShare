import {redis} from '../infrastructure/redis';
import {logger} from '../logger';
import {Job, ProcessingMetrics} from '../types';
import {DriverLocationService} from './DriverLocation.Service';
import {DriverMatchingService} from './DriverMatching.Service';
import {OfferManagementService} from './OfferManagement.Service';
import {ZoneService} from './ZoneService';
import {MapboxService} from './MapboxService'
import {
    newJobEventSchema, driverResponseSchema, validateSchema
} from './validation';

export class JobOrchestratorService {
    private driverLocationService: DriverLocationService;
    private driverMatchingService: DriverMatchingService;
    private offerManagementService: OfferManagementService;
    private zoneService: ZoneService;
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
        this.driverMatchingService = new DriverMatchingService(this.driverLocationService, this.zoneService);
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
        const eventType = data.type;
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

    async handleDriverResponse(data: any): Promise<any> {
        const {driverId, jobId, action, reason} = data;
        logger.info(`Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}, Reason: ${reason || 'N/A'}`);

        try {
            if (action === 'accept') {
                // Get customerId from Redis before assigning
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
                // Get booking data from Redis to reconstruct job
                const bookingPattern = `booking:${jobId}-*`;
                const bookingKeys = await redis.keys(bookingPattern);

                if (bookingKeys.length === 0) {
                    logger.warn(`No booking found for Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Job not found in Redis',
                        jobId
                    };
                }

                const bookingKey = bookingKeys[0];
                const bookingDataJson = await redis.call('JSON.GET', bookingKey) as any;

                if (!bookingDataJson) {
                    logger.warn(`Job data not found - Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Job data not found',
                        jobId
                    };
                }

                const bookingData = typeof bookingDataJson === 'string'
                    ? JSON.parse(bookingDataJson)
                    : bookingDataJson;

                // Extract customer ID from booking pattern or data
                const customerId = bookingData.customer?._id || bookingKey.split('-')[1];

                // Handle rejection in Redis
                await this.offerManagementService.handleDriverRejection(jobId, customerId, driverId, reason);

                // Reconstruct Job object from booking data
                const pickupData = bookingData.tripAddress?.[0];
                const dropData = bookingData.tripAddress?.[bookingData.tripAddress.length - 1];

                if (!pickupData?.location) {
                    logger.error(`Invalid pickup data for Job: ${jobId}`);
                    return {
                        success: false,
                        message: 'Invalid job data',
                        jobId
                    };
                }

                const job: Job = {
                    id: jobId,
                    customerId: customerId,
                    pickupLat: pickupData.location.latitude,
                    pickupLng: pickupData.location.longitude,
                    dropLat: dropData?.location?.latitude,
                    dropLng: dropData?.location?.longitude,
                    fare: bookingData.grandTotal || 0,
                    vehicleType: bookingData.selectedVehicle?.name || 'Unknown',
                    tripAddress: bookingData.tripAddress || [],
                    timestamp: bookingData.createdAt
                        ? new Date(bookingData.createdAt).getTime()
                        : Date.now(),
                    customer: {
                        fullName: bookingData.customer?.fullName || 'Unknown',
                        avatar: bookingData.customer?.avatar || '',
                        distance: bookingData.expectedBilling?.kmText || 'N/A',
                        time: bookingData.expectedBilling?.durationText || 'N/A'
                    }
                };

                // Find alternative drivers
                const categorizedDrivers = await this.driverMatchingService.findBestDrivers(job, customerId);

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
                    ...categorizedDrivers.busyDrivers
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

        if (!pickup) {
            logger.error(`Missing pickup coordinates for Job ${jobId}`);
            return {success: false, error: 'Pickup coordinates missing', jobId};
        }

        const customer = payload.customer;
        if (!customer?._id) {
            logger.error(`Invalid customer data — Job ${jobId}`);
            return {success: false, error: 'Invalid customer data', jobId};
        }

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
            // Calculate trip ETA (pickup to drop) - Store in rideDetails
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

                logger.info(`Trip ETA Calculated for Job ${job.id} — Distance: ${tripEta.distanceText}, Duration: ${tripEta.durationText}`);
            } else {
                logger.warn(`Drop location missing — Skipping trip ETA calculation for Job ${job.id}`);
                job.rideDetails = {
                    estimatedTime: 'N/A',
                    estimatedDistance: 0,
                };
            }

            const categorizedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
            const searchTime = Date.now() - startTime;

            if (!categorizedDrivers) {
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

            const matchedDrivers = [
                ...categorizedDrivers.favDriver,
                ...categorizedDrivers.priorityDrivers,
                ...categorizedDrivers.newDrivers,
                ...categorizedDrivers.nonPriorityDrivers,
                ...categorizedDrivers.remainingDrivers,
                ...categorizedDrivers.busyDrivers
            ].map((d: any) => d.id);

            // Calculate driver-to-pickup ETAs
            const driversWithEta = await Promise.all(
                matchedDrivers.map(async (driverId) => {
                    try {
                        const driverDataJson = await redis.call('JSON.GET', `driver:${driverId}`);
                        if (!driverDataJson) {
                            logger.warn(`Driver data not found in Redis - ${driverId}`);
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

                        const driverToPickupEta = await this.mapboxService.getDistanceAndDuration(
                            lat,
                            lng,
                            pickup.latitude,
                            pickup.longitude
                        );

                        return {
                            driverId,
                            distanceToPickup: driverToPickupEta.distanceKm,
                            etaToPickup: driverToPickupEta.durationText,
                        };
                    } catch (error: any) {
                        logger.error(`ETA calculation failed for driver ${driverId}: ${error.message}`);
                        return null;
                    }
                })
            );

            const validDriverEtas = driversWithEta.filter((d) => d !== null);

            logger.info(`Driver ETAs calculated - Job: ${job.id}, Valid: ${validDriverEtas.length}, Invalid: ${matchedDrivers.length - validDriverEtas.length}`);

            if (validDriverEtas.length === 0) {
                logger.warn(`No valid driver ETA data for Job ${job.id}`);
                return {
                    success: false,
                    message: 'No drivers with valid location data',
                    jobId: job.id,
                    orderNo: payload.orderNo,
                    driversFound: matchedDrivers.length,
                    driverIds: matchedDrivers,
                    searchTimeMs: searchTime,
                    timestamp: new Date().toISOString(),
                };
            }

            const offerResults = await Promise.all(
                validDriverEtas.map(async (driverEta) => {
                    try {
                        // Store driver-to-pickup ETA in customer array
                        const jobWithDriverEta = {
                            ...job,
                            customer: {
                                time: driverEta.etaToPickup,
                                distance: driverEta.distanceToPickup,
                                fullName: customer.fullName || 'Unknown',
                                avatar: customer.avatar || '',
                            }
                        };

                        await this.offerManagementService.sendOffers(jobWithDriverEta, [driverEta.driverId]);
                        logger.info(`Offer sent to driver ${driverEta.driverId} - ETA: ${driverEta.etaToPickup}`);
                        return true;
                    } catch (error: any) {
                        logger.error(`Failed to send offer to driver ${driverEta.driverId}: ${error.message}`);
                        return false;
                    }
                })
            );

            const successful = offerResults.filter(r => r).length;
            const failed = offerResults.length - successful;

            this.metrics.driversMatched += matchedDrivers.length;
            this.metrics.offersSent += successful;

            logger.info(`Job Matched - JobId: ${job.id}, Drivers: ${matchedDrivers.length}, Offers Sent: ${successful}, Failed: ${failed}, Search Time: ${searchTime}ms`);

            return {
                success: true,
                message: 'Drivers found and offers sent',
                jobId: job.id,
                orderNo: payload.orderNo,
                driversFound: matchedDrivers.length,
                driverIds: matchedDrivers,
                offersSent: successful,
                offersFailed: failed,
                searchTimeMs: searchTime,
                timestamp: new Date().toISOString(),
            };

        } catch (error: any) {
            this.metrics.errors++;
            const searchTime = Date.now() - startTime;

            logger.error(`Driver Search Failed - JobId: ${job.id}, Error: ${error.message}, Search Time: ${searchTime}ms`);

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