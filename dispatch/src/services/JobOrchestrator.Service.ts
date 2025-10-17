import { redis } from '../infrastructure/redis';
import { logger } from '../logger';
import { Job, ProcessingMetrics } from '../types';
import { DriverLocationService } from './DriverLocation.Service';
import { JobProcessingService } from './JobProcessingService';
import { DriverMatchingService } from './DriverMatching.Service';
import { OfferManagementService } from './OfferManagement.Service';

export class JobOrchestratorService {
    private processingInterval: NodeJS.Timeout | null = null;
    private readonly PROCESSING_INTERVAL = 100;

    private driverLocationService: DriverLocationService;
    private jobProcessingService: JobProcessingService;
    private driverMatchingService: DriverMatchingService;
    private offerManagementService: OfferManagementService;

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
        // FIX: Initialize services in correct order without circular dependencies
        this.driverLocationService = new DriverLocationService();
        this.jobProcessingService = new JobProcessingService(); // Remove constructor params
        this.driverMatchingService = new DriverMatchingService(this.driverLocationService);
        this.offerManagementService = new OfferManagementService();
    }



    async start(): Promise<void> {
        await this.driverLocationService.startDriverCacheRefresh();
        this.startJobProcessing();

        logger.info(`Job Orchestrator started successfully - ProcessingInterval: ${this.PROCESSING_INTERVAL}ms`);
    }

    stop(): void {
        if (this.processingInterval) {
            clearInterval(this.processingInterval);
        }
        logger.info('Job Orchestrator stopped');
    }

    // RPC Handler methods (minimal changes from original)
    async handleRPCRequest(data: any): Promise<any> {
        const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        logger.info(`RPC Request Received - Type: ${data.type}, RequestId: ${requestId}`);

        this.metrics.rpcRequests++;

        try {
            let result;
            switch (data.type) {
                case 'payment.completed':
                    this.metrics.apiCalls.handlePaymentCompleted++;
                    result = await this.handlePaymentCompleted(data);
                    break;
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
                    logger.warn(`Unknown RPC request type: ${data.type}`);
                    return { success: false, error: 'Unknown request type' };
            }

            logger.info(`RPC Request Completed - Type: ${data.type}, RequestId: ${requestId}, Success: ${result.success}`);
            return result;
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`RPC Request Failed - Type: ${data.type}, RequestId: ${requestId}, Error: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    // async handleDriverResponse(data: any): Promise<any> {
    //     const { driverId, jobId, action, reason } = data;
    //     logger.info(`Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}, Reason: ${reason}`);
    //
    //     try {
    //         if (action === 'accept') {
    //             await this.offerManagementService.assignDriverToJob(jobId, driverId);
    //             this.jobProcessingService.removeJob(jobId);
    //
    //             logger.info(`Driver Accepted - Job: ${jobId}, Driver: ${driverId}`);
    //             return { success: true, message: 'Driver assigned to job' };
    //         } else if (action === 'reject') {
    //             await this.offerManagementService.handleDriverRejection(jobId, driverId, reason);
    //
    //             // Find alternative drivers
    //             const alternativeJob = await this.offerManagementService.findAlternativeDrivers(jobId);
    //             if (alternativeJob) {
    //                 await this.jobProcessingService.addJob(alternativeJob);
    //             }
    //
    //             logger.info(`Driver Rejected - Job: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);
    //             return { success: true, message: 'Searching for alternative drivers' };
    //         } else {
    //             logger.warn(`Unknown Driver Action - Action: ${action}`);
    //             return { success: false, error: 'Unknown driver action' };
    //         }
    //     } catch (error: any) {
    //         this.metrics.errors++;
    //         logger.error(`Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
    //         return { success: false, error: error.message };
    //     }
    // }

    async handleDriverResponse(data: any): Promise<any> {
        const { driverId, jobId, action, reason } = data;
        logger.info(`Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}, Reason: ${reason}`);

        try {
            if (action === 'accept') {
                await this.offerManagementService.assignDriverToJob(jobId, driverId);
                this.jobProcessingService.removeJob(jobId);

                logger.info(`Driver Accepted - Job: ${jobId}, Driver: ${driverId}`);
                return { success: true, message: 'Driver assigned to job' };
            } else if (action === 'reject') {
                await this.offerManagementService.handleDriverRejection(jobId, driverId, reason);

                // Get the job to find alternative drivers
                const job = this.jobProcessingService.getJob(jobId);
                if (job) {
                    // Find nearby drivers excluding rejected ones
                    const nearbyDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);
                    const alternativeDrivers = await this.offerManagementService.findAlternativeDrivers(jobId, nearbyDrivers);

                    if (alternativeDrivers.length > 0) {
                        // Resend offers to alternative drivers
                        await this.offerManagementService.sendOffers(job, alternativeDrivers);
                        logger.info(`Alternative offers sent - Job: ${jobId}, Drivers: ${alternativeDrivers.length}`);
                    } else {
                        logger.info(`No alternative drivers found - Job: ${jobId}`);
                    }
                }

                logger.info(`Driver Rejected - Job: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);
                return { success: true, message: 'Searching for alternative drivers' };
            } else {
                logger.warn(`Unknown Driver Action - Action: ${action}`);
                return { success: false, error: 'Unknown driver action' };
            }
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    async addJob(job: Job): Promise<void> {
        this.metrics.apiCalls.addJob++;
        this.metrics.jobsProcessed++;
        await this.jobProcessingService.addJob(job);
    }

    async addJobs(jobs: Job[]): Promise<void> {
        this.metrics.apiCalls.addJobs++;
        const result = await this.jobProcessingService.addJobs(jobs);
        this.metrics.jobsProcessed += result.added.length;
    }

    // Private methods
    private async handlePaymentCompleted(data: any): Promise<any> {
        const job: Job = {
            id: data.jobId,
            customerId: data.customerId,
            pickupLat: data.pickupLat,
            pickupLng: data.pickupLng,
            fare: data.fare,
            vehicleType: data.vehicleType,
            timestamp: Date.now()
        };

        logger.info(`Payment Completed - JobId: ${job.id}, Customer: ${job.customerId}, Fare: ${job.fare}`);
        await this.addJob(job);

        logger.info(`Driver Search Initiated - JobId: ${job.id}`);
        return {
            success: true,
            message: 'Driver search initiated',
            jobId: job.id,
            timestamp: job.timestamp
        };
    }

    private async handleGetStats(data: any): Promise<any> {
        logger.info(`Stats Request - Client: ${data.clientId || 'unknown'}`);
        const stats = this.getStats();
        return { success: true, stats };
    }

    private async handleHealthCheck(data: any): Promise<any> {
        logger.info('Health Check Request');

        try {
            const redisStart = Date.now();
            await redis.ping();
            const redisLatency = Date.now() - redisStart;

            const stats = this.getStats();

            return {
                success: true,
                status: 'healthy',
                redis: 'connected',
                redisLatency: `${redisLatency}ms`,
                ...stats
            };
        } catch (error: any) {
            this.metrics.errors++;
            logger.error(`Health Check Failed - Redis: disconnected, Error: ${error.message}`);
            return {
                success: false,
                status: 'unhealthy',
                redis: 'disconnected',
                error: error.message
            };
        }
    }

    private startJobProcessing(): void {
        logger.info(`Starting Job Processing Interval - Interval: ${this.PROCESSING_INTERVAL}ms`);

        this.processingInterval = setInterval(async () => {
            await this.processAllJobs();
        }, this.PROCESSING_INTERVAL);
    }

    private async processAllJobs(): Promise<void> {
        if (this.jobProcessingService.getActiveJobsCount() === 0) {
            return;
        }

        const startTime = Date.now();
        const jobs = this.jobProcessingService.getAllJobs();

        const processingPromises = jobs.map(job => this.processSingleJob(job));
        const results = await Promise.allSettled(processingPromises);

        const completedJobs: string[] = [];
        const failedJobs: string[] = [];

        results.forEach((result, index) => {
            const job = jobs[index];
            if (result.status === 'fulfilled' && result.value.completed) {
                completedJobs.push(job.id);
                this.jobProcessingService.removeJob(job.id);
            } else if (result.status === 'rejected') {
                failedJobs.push(job.id);
                this.jobProcessingService.removeJob(job.id);
                logger.error(`Job Processing Failed - JobId: ${job.id}, Error: ${result.reason}`);
            }
        });

        const processingTime = Date.now() - startTime;
        if (completedJobs.length > 0 || failedJobs.length > 0) {
            logger.info(`Job Processing Completed - Total: ${jobs.length}, Completed: ${completedJobs.length}, Failed: ${failedJobs.length}, Time: ${processingTime}ms`);
        }
    }

    // In JobOrchestratorService.ts - update the processSingleJob method
    private async processSingleJob(job: Job): Promise<{ completed: boolean }> {
        try {
            const matchedDrivers = await this.driverMatchingService.findBestDrivers(job, job.customerId);

            if (matchedDrivers.length > 0) {
                const result = await this.offerManagementService.sendOffers(job, matchedDrivers);
                this.metrics.offersSent += result.successful;
                this.metrics.driversMatched += matchedDrivers.length;

                logger.debug(`Job Matched - JobId: ${job.id}, Customer: ${job.customerId}, Drivers: ${matchedDrivers.length}`);
                return { completed: true };
            } else {
                logger.debug(`No Drivers Found - JobId: ${job.id}, Customer: ${job.customerId}`);
                return { completed: false };
            }
        } catch (error) {
            this.metrics.errors++;
            logger.error(`Job Processing Error - JobId: ${job.id}, Error: ${error}`);
            return { completed: true };
        }
    }

    getStats() {
        return {
            activeJobs: this.jobProcessingService.getActiveJobsCount(),
            cachedDrivers: this.driverLocationService.getDriverCacheSize(),
            processingRate: Math.round(this.jobProcessingService.getActiveJobsCount() * 10),
            cacheHitRate: this.driverLocationService.getDriverCacheSize() > 0 ? 0.95 : 0,
            metrics: this.metrics,
            timestamp: new Date().toISOString()
        };
    }
}

export const jobOrchestratorService = new JobOrchestratorService();