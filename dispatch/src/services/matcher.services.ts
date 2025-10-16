// import {redis} from '../infrastructure/redis';
// import {logger} from '../logger';
//
// interface Job {
//     id: string;
//     customerId: string;
//     pickupLat: number;
//     pickupLng: number;
//     fare: number;
//     vehicleType?: string;
//     timestamp: number;
// }
//
// interface Driver {
//     driverId: string;
//     lat: number;
//     lng: number;
//     score: number;
//     isFavorite: boolean;
//     isBusy: boolean;
//     isNew: boolean;
//     lastUpdate: number;
// }
//
// export class RealTimeMatcherService {
//     private driverCache = new Map<string, Driver>();
//     private activeJobs = new Map<string, Job>();
//     private processingInterval: NodeJS.Timeout | null = null;
//     private readonly MAX_CONCURRENT_JOBS = 1000;
//     private readonly PROCESSING_INTERVAL = 100;
//
//     // Metrics tracking
//     private metrics = {
//         rpcRequests: 0, jobsProcessed: 0, driversMatched: 0, offersSent: 0, errors: 0, apiCalls: {
//             handlePaymentCompleted: 0,
//             handleDriverResponse: 0,
//             handleGetStats: 0,
//             handleHealthCheck: 0,
//             addJobs: 0,
//             addJob: 0
//         }
//     };
//
//     start(): void {
//         this.startDriverCacheRefresh();
//         this.startJobProcessing();
//         logger.info(` Real-time Parallel
//         Matcher started successfully - Service: RealTimeMatcherService, Status: started, MaxConcurrentJobs: ${this.MAX_CONCURRENT_JOBS}, ProcessingInterval: ${this.PROCESSING_INTERVAL}ms`);
//     }
//
//     stop(): void {
//         if (this.processingInterval) {
//             clearInterval(this.processingInterval);
//         }
//
//         const finalMetrics = {
//             rpcRequests: this.metrics.rpcRequests,
//             jobsProcessed: this.metrics.jobsProcessed,
//             driversMatched: this.metrics.driversMatched,
//             offersSent: this.metrics.offersSent,
//             errors: this.metrics.errors
//         };
//
//         logger.info(` Real-time Matcher stopped - Service: RealTimeMatcherService, FinalMetrics: ${JSON.stringify(finalMetrics)}`);
//     }
//
//     /**
//      * Handle incoming Kafka RPC requests
//      */
//     async handleRPCRequest(data: any): Promise<any> {
//         const requestId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
//
//         logger.info(`RPC Request Received - Type: ${data.type}, RequestId: ${requestId}`);
//         this.metrics.rpcRequests++;
//
//         try {
//             logger.debug(` Processing RPC request - Type: ${data.type}, RequestId: ${requestId}`);
//
//             let result;
//             switch (data.type) {
//                 case 'payment.completed':
//                     this.metrics.apiCalls.handlePaymentCompleted++;
//                     result = await this.handlePaymentCompleted(data);
//                     break;
//
//                 case 'driver.response':
//                     this.metrics.apiCalls.handleDriverResponse++;
//                     result = await this.handleDriverResponse(data);
//                     break;
//
//                 case 'get.stats':
//                     this.metrics.apiCalls.handleGetStats++;
//                     result = await this.handleGetStats(data);
//                     break;
//
//                 case 'health.check':
//                     this.metrics.apiCalls.handleHealthCheck++;
//                     result = await this.handleHealthCheck(data);
//                     break;
//
//                 default:
//                     logger.warn(` Unknown RPC request type: ${data.type}`);
//                     return {success: false, error: 'Unknown request type'};
//             }
//
//             logger.info(` RPC Request Completed - Type: ${data.type}, RequestId: ${requestId}, Success: ${result.success}`);
//             return result;
//
//         } catch (error: any) {
//             this.metrics.errors++;
//             logger.error(` RPC Request Failed - Type: ${data.type}, RequestId: ${requestId}, Error: ${error.message}`);
//             return {success: false, error: error.message};
//         }
//     }
//
//     /**
//      * Handle driver responses from Kafka RPC
//      */
//     async handleDriverResponse(data: any): Promise<any> {
//         const {driverId, jobId, action, reason} = data;
//
//         logger.info(` Driver Response - Driver: ${driverId}, Job: ${jobId}, Action: ${action}, Reason: ${reason}`);
//
//         try {
//             if (action === 'accept') {
//                 await this.assignDriverToJob(jobId, driverId);
//                 logger.info(` Driver Accepted - Job: ${jobId}, Driver: ${driverId}`);
//                 return {success: true, message: 'Driver assigned to job'};
//
//             } else if (action === 'reject') {
//                 await this.handleDriverRejection(jobId, driverId, reason);
//                 logger.info(` Driver Rejected - Job: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);
//
//                 await this.findAlternativeDrivers(jobId);
//                 return {success: true, message: 'Searching for alternative drivers'};
//
//             } else {
//                 logger.warn(` Unknown Driver Action - Action: ${action}`);
//                 return {success: false, error: 'Unknown driver action'};
//             }
//         } catch (error: any) {
//             this.metrics.errors++;
//             logger.error(` Driver Response Error - Job: ${jobId}, Driver: ${driverId}, Error: ${error.message}`);
//             return {success: false, error: error.message};
//         }
//     }
//
//     /**
//      * Add multiple jobs simultaneously
//      */
//     async addJobs(jobs: Job[]): Promise<void> {
//         this.metrics.apiCalls.addJobs++;
//
//         logger.info(` Adding Multiple Jobs - Count: ${jobs.length}, CurrentActive: ${this.activeJobs.size}, Max: ${this.MAX_CONCURRENT_JOBS}`);
//
//         const addedJobs: string[] = [];
//         const skippedJobs: string[] = [];
//
//         for (const job of jobs) {
//             if (this.activeJobs.size >= this.MAX_CONCURRENT_JOBS) {
//                 logger.warn(` Max Concurrent Jobs Reached - Skipping Job: ${job.id}`);
//                 skippedJobs.push(job.id);
//                 continue;
//             }
//
//             this.activeJobs.set(job.id, job);
//             addedJobs.push(job.id);
//             this.metrics.jobsProcessed++;
//         }
//
//         logger.info(` Multiple Jobs Added - Added: ${addedJobs.length}, Skipped: ${skippedJobs.length}, TotalActive: ${this.activeJobs.size}`);
//     }
//
//     /**
//      * Add single job
//      */
//     async addJob(job: Job): Promise<void> {
//         this.metrics.apiCalls.addJob++;
//
//         if (this.activeJobs.size >= this.MAX_CONCURRENT_JOBS) {
//             logger.error(` Max Concurrent Jobs Reached - Cannot Add Job: ${job.id}`);
//             throw new Error('Max concurrent jobs reached');
//         }
//
//         this.activeJobs.set(job.id, job);
//         this.metrics.jobsProcessed++;
//
//         logger.debug(` Single Job Added - JobId: ${job.id}, Customer: ${job.customerId}, ActiveJobs: ${this.activeJobs.size}`);
//     }
//
//     // real time stats
//     getStats() {
//         const stats = {
//             activeJobs: this.activeJobs.size,
//             cachedDrivers: this.driverCache.size,
//             processingRate: Math.round(this.activeJobs.size * 10),
//             cacheHitRate: this.getCacheHitRate(),
//             metrics: this.metrics,
//             timestamp: new Date().toISOString()
//         };
//
//         logger.debug(`Service Stats - ActiveJobs: ${stats.activeJobs}, Drivers: ${stats.cachedDrivers}, Rate: ${stats.processingRate} jobs/s`);
//         return stats;
//     }
//
//     /**
//      * Handle payment completed events from Kafka RPC
//      */
//     private async handlePaymentCompleted(data: any): Promise<any> {
//         const job: Job = {
//             id: data.jobId,
//             customerId: data.customerId,
//             pickupLat: data.pickupLat,
//             pickupLng: data.pickupLng,
//             fare: data.fare,
//             vehicleType: data.vehicleType,
//             timestamp: Date.now()
//         };
//
//         logger.info(` Payment Completed - JobId: ${job.id}, Customer: ${job.customerId}, Fare: ${job.fare}`);
//         await this.addJob(job);
//
//         logger.info(` Driver Search Initiated - JobId: ${job.id}, ActiveJobs: ${this.activeJobs.size}`);
//         return {
//             success: true, message: 'Driver search initiated', jobId: job.id, timestamp: job.timestamp
//         };
//     }
//
//     /**
//      * Handle stats requests
//      */
//     private async handleGetStats(data: any): Promise<any> {
//         logger.info(` Stats Request - Client: ${data.clientId || 'unknown'}`);
//         const stats = this.getStats();
//         return {success: true, stats};
//     }
//
//     /**
//      * Handle health check requests
//      */
//     private async handleHealthCheck(data: any): Promise<any> {
//         logger.info(' Health Check Request');
//
//         try {
//             const redisStart = Date.now();
//             await redis.ping();
//             const redisLatency = Date.now() - redisStart;
//
//             const stats = this.getStats();
//
//             logger.debug(`${redisLatency}ms, ActiveJobs: ${this.activeJobs.size}, Drivers: ${this.driverCache.size}`);
//
//             return {
//                 success: true, status: 'healthy', redis: 'connected', redisLatency: `${redisLatency}ms`, ...stats
//             };
//         } catch (error: any) {
//             this.metrics.errors++;
//             logger.error(` Health Check Failed - Redis: disconnected, Error: ${error.message}`);
//             return {
//                 success: false, status: 'unhealthy', redis: 'disconnected', error: error.message
//             };
//         }
//     }
//
//     /**
//      * Core: Process ALL active jobs in parallel every 100ms
//      */
//     private startJobProcessing(): void {
//         logger.info(` Starting Job Processing Interval - Interval: ${this.PROCESSING_INTERVAL}ms`);
//
//         this.processingInterval = setInterval(async () => {
//             await this.processAllJobs();
//         }, this.PROCESSING_INTERVAL);
//     }
//
//     private async processAllJobs(): Promise<void> {
//         if (this.activeJobs.size === 0) {
//             logger.debug(' No Active Jobs to Process');
//             return;
//         }
//
//         const startTime = Date.now();
//         const jobs = Array.from(this.activeJobs.values());
//
//         logger.debug(` Starting Parallel Job Processing - Jobs: ${jobs.length}, Drivers: ${this.driverCache.size}`);
//
//         const processingPromises = jobs.map(job => this.processSingleJob(job));
//         const results = await Promise.allSettled(processingPromises);
//
//         const completedJobs: string[] = [];
//         const failedJobs: string[] = [];
//         const retryJobs: string[] = [];
//
//         results.forEach((result, index) => {
//             const job = jobs[index];
//
//             if (result.status === 'fulfilled') {
//                 if (result.value.completed) {
//                     completedJobs.push(job.id);
//                     this.activeJobs.delete(job.id);
//                 } else {
//                     retryJobs.push(job.id);
//                 }
//             } else {
//                 failedJobs.push(job.id);
//                 this.activeJobs.delete(job.id);
//                 logger.error(` Job Processing Failed - JobId: ${job.id}, Error: ${result.reason}`);
//             }
//         });
//
//         const processingTime = Date.now() - startTime;
//         const jobsPerSecond = (jobs.length / (processingTime / 1000)).toFixed(1);
//
//         logger.info(` Parallel Processing Completed - Total: ${jobs.length}, Completed: ${completedJobs.length}, Failed: ${failedJobs.length}, Retry: ${retryJobs.length}, Remaining: ${this.activeJobs.size}, Time: ${processingTime}ms, Rate: ${jobsPerSecond} jobs/s`);
//
//         if (processingTime > 50) {
//             logger.warn(` Job Processing Slow - Time: ${processingTime}ms, Jobs: ${jobs.length}, Expected: <50ms`);
//         }
//     }
//
//     /**
//      * Process single job with optimized matching
//      */
//     private async processSingleJob(job: Job): Promise<{ completed: boolean }> {
//         try {
//             const matchedDrivers = this.findBestDrivers(job);
//
//             if (matchedDrivers.length > 0) {
//                 await this.sendOffers(job, matchedDrivers);
//                 logger.debug(` Job Matched - JobId: ${job.id}, Customer: ${job.customerId}, Drivers: ${matchedDrivers.length}`);
//                 return {completed: true};
//             } else {
//                 logger.debug(` No Drivers Found - JobId: ${job.id}, Customer: ${job.customerId}`);
//                 return {completed: false};
//             }
//         } catch (error) {
//             this.metrics.errors++;
//             logger.error(` Job Processing Error - JobId: ${job.id}, Error: ${error instanceof Error ? error.message : error}`);
//             return {completed: true};
//         }
//     }
//
//     /**
//      * Core matching algorithm - finds best drivers for a job
//      */
//     private findBestDrivers(job: Job): string[] {
//         const startTime = Date.now();
//         const matches: Array<{ driverId: string; priority: number; distance: number }> = [];
//         const jobLocation = {lat: job.pickupLat, lng: job.pickupLng};
//         const now = Date.now();
//
//         logger.debug(` Starting Driver Matching - JobId: ${job.id}, Drivers: ${this.driverCache.size}`);
//
//         for (const [driverId, driver] of this.driverCache.entries()) {
//             if (now - driver.lastUpdate > 30000) continue;
//             if (driver.isBusy) continue;
//
//             const distance = this.calculateDistance(jobLocation.lat, jobLocation.lng, driver.lat, driver.lng);
//
//             const priority = this.calculateDriverPriority(driver, distance, job.customerId);
//
//             if (priority > 0) {
//                 matches.push({driverId, priority, distance});
//                 this.metrics.driversMatched++;
//             }
//
//             if (matches.length >= 20) break;
//         }
//
//         const sortedDrivers = matches
//             .sort((a, b) => {
//                 if (b.priority !== a.priority) return b.priority - a.priority;
//                 return a.distance - b.distance;
//             })
//             .slice(0, 10)
//             .map(m => m.driverId);
//
//         const matchingTime = Date.now() - startTime;
//         logger.debug(` Driver Matching Completed - JobId: ${job.id}, Matches: ${matches.length}, Selected: ${sortedDrivers.length}, Time: ${matchingTime}ms`);
//
//         return sortedDrivers;
//     }
//
//     /**
//      * Priority calculation - your business logic
//      */
//     private calculateDriverPriority(driver: Driver, distance: number, customerId: string): number {
//         let priority = 0;
//
//         if (driver.isFavorite && distance <= 3) {
//             priority = 1000 + driver.score;
//         } else if (driver.score >= 80 && distance <= 3) {
//             priority = 900 + driver.score;
//         } else if (driver.isNew && distance <= 3) {
//             priority = 800;
//         } else if (driver.score >= 60 && distance <= 3) {
//             priority = 700 + driver.score;
//         } else if (distance <= 3) {
//             priority = 600 - distance;
//         } else if (driver.isBusy && distance <= 5) {
//             priority = 500 - distance;
//         } else if (distance <= 15) {
//             priority = 400 - distance;
//         }
//
//         return priority;
//     }
//
//     /**
//      * Send offers to multiple drivers in parallel
//      */
//     private async sendOffers(job: Job, driverIds: string[]): Promise<void> {
//         logger.info(` Sending Offers - JobId: ${job.id}, Drivers: ${driverIds.length}`);
//
//         const offerPromises = driverIds.map(driverId => this.sendOfferToDriver(job, driverId));
//         const results = await Promise.allSettled(offerPromises);
//
//         const successful = results.filter(r => r.status === 'fulfilled').length;
//         const failed = results.filter(r => r.status === 'rejected').length;
//
//         this.metrics.offersSent += successful;
//         logger.info(` Offers Sent - JobId: ${job.id}, Total: ${driverIds.length}, Successful: ${successful}, Failed: ${failed}`);
//     }
//
//     private async sendOfferToDriver(job: Job, driverId: string): Promise<void> {
//         try {
//             const offerKey = `offer:${job.id}:${driverId}`;
//             const offerData = {
//                 jobId: job.id,
//                 driverId,
//                 customerId: job.customerId,
//                 pickupLat: job.pickupLat,
//                 pickupLng: job.pickupLng,
//                 fare: job.fare,
//                 status: 'pending',
//                 sentAt: new Date().toISOString(),
//                 expiresAt: Date.now() + 30000
//             };
//
//             await redis.setex(offerKey, 30, JSON.stringify(offerData));
//             logger.debug(` Offer Sent - JobId: ${job.id}, Driver: ${driverId}`);
//
//         } catch (error) {
//             this.metrics.errors++;
//             logger.error(` Offer Send Error - JobId: ${job.id}, Driver: ${driverId}, Error: ${error}`);
//         }
//     }
//
//     /**
//      * Assign driver to job and update Redis
//      */
//     private async assignDriverToJob(jobId: string, driverId: string): Promise<void> {
//         logger.info(` Assigning Driver - JobId: ${jobId}, Driver: ${driverId}`);
//
//         await redis.hset(`job:${jobId}`, {
//             assignedDriver: driverId, status: 'accepted', assignedAt: new Date().toISOString()
//         });
//
//         await redis.hset(`driver:${driverId}:profile`, 'isBusy', 'true');
//         await this.cancelOtherOffers(jobId, driverId);
//
//         logger.info(` Job Assigned - JobId: ${jobId}, Driver: ${driverId}`);
//     }
//
//     /**
//      * Handle driver rejection
//      */
//     private async handleDriverRejection(jobId: string, driverId: string, reason?: string): Promise<void> {
//         logger.info(` Handling Driver Rejection - JobId: ${jobId}, Driver: ${driverId}, Reason: ${reason}`);
//         await redis.del(`offer:${jobId}:${driverId}`);
//         await redis.srem(`driver:${driverId}:offers`, jobId);
//         logger.debug(` Driver Offer Removed - JobId: ${jobId}, Driver: ${driverId}`);
//     }
//
//     /**
//      * Find alternative drivers when original driver rejects
//      */
//     private async findAlternativeDrivers(jobId: string): Promise<void> {
//         logger.info(` Finding Alternative Drivers - JobId: ${jobId}`);
//
//         const jobData = await redis.hgetall(`job:${jobId}`);
//         if (!jobData.pickupLat || !jobData.pickupLng) {
//             logger.warn(` Job Location Not Found - JobId: ${jobId}`);
//             return;
//         }
//
//         const job: Job = {
//             id: jobId,
//             customerId: jobData.customerId,
//             pickupLat: parseFloat(jobData.pickupLat),
//             pickupLng: parseFloat(jobData.pickupLng),
//             fare: parseFloat(jobData.fare),
//             timestamp: Date.now()
//         };
//
//         await this.addJob(job);
//         logger.info(` Job Queued for Re-matching - JobId: ${jobId}`);
//     }
//
//     /**
//      * Cancel all other offers for a job except the accepted one
//      */
//
//
//     private async cancelOtherOffers(jobId: string, acceptedDriverId: string): Promise<void> {
//         logger.debug(` Cancelling Other Offers - JobId: ${jobId}, AcceptedDriver: ${acceptedDriverId}`);
//
//         const offerKeys = await redis.keys(`offer:${jobId}:*`);
//         const cancelPromises = offerKeys.map(async (key) => {
//             const driverId = key.split(':')[2];
//             if (driverId !== acceptedDriverId) {
//                 await redis.del(key);
//                 await redis.srem(`driver:${driverId}:offers`, jobId);
//             }
//         });
//
//         await Promise.allSettled(cancelPromises);
//         logger.debug(` Other Offers Cancelled - JobId: ${jobId}, Cancelled: ${offerKeys.length - 1}`);
//     }
//
//     /**
//      * Driver cache management - refreshed every 2 seconds
//      */
//     private async startDriverCacheRefresh(): Promise<void> {
//         logger.info(' Starting Driver Cache Refresh - Interval: 2s');
//         await this.refreshDriverCache();
//         setInterval(async () => {
//             await this.refreshDriverCache();
//         }, 2000);
//     }
//
//     private async refreshDriverCache(): Promise<void> {
//         const startTime = Date.now();
//
//         try {
//             logger.debug(` Refreshing Driver Cache - Current: ${this.driverCache.size}`);
//             const keys = await redis.keys('driver:*:location');
//             const newCache = new Map<string, Driver>();
//
//             const driverPromises = keys.map(async (key) => {
//                 const driverId = key.split(':')[1];
//                 try {
//                     const [location, profile] = await Promise.all([redis.hgetall(key), redis.hgetall(`driver:${driverId}:profile`)]);
//
//                     if (location?.lat && location?.lng) {
//                         const driver: Driver = {
//                             driverId,
//                             lat: parseFloat(location.lat),
//                             lng: parseFloat(location.lng),
//                             score: parseFloat(profile?.score || "0"),
//                             isFavorite: profile?.isFavorite === "true",
//                             isBusy: profile?.isBusy === "true",
//                             isNew: this.isNewDriver(profile?.approvedDate || ""),
//                             lastUpdate: parseInt(location.ts || "0")
//                         };
//                         newCache.set(driverId, driver);
//                     }
//                 } catch (error) {
//                     logger.error(` Driver Data Error - Driver: ${driverId}, Error: ${error}`);
//                 }
//             });
//
//             await Promise.allSettled(driverPromises);
//             this.driverCache = newCache;
//
//             const refreshTime = Date.now() - startTime;
//             logger.debug(` Driver Cache Refreshed - Drivers: ${newCache.size}, Time: ${refreshTime}ms`);
//
//         } catch (error) {
//             this.metrics.errors++;
//             logger.error(` Cache Refresh Error - Error: ${error}`);
//         }
//     }
//
//     /**
//      * Haversine distance calculation
//      */
//     private calculateDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
//         const R = 6371;
//         const dLat = (lat2 - lat1) * Math.PI / 180;
//         const dLng = (lng2 - lng1) * Math.PI / 180;
//         const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
//         return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
//     }
//
//     private isNewDriver(approvedDate: string): boolean {
//         if (!approvedDate) return false;
//         const approved = new Date(approvedDate);
//         return (Date.now() - approved.getTime()) <= (30 * 24 * 60 * 60 * 1000);
//     }
//
//     private getCacheHitRate(): number {
//         return this.driverCache.size > 0 ? 0.95 : 0;
//     }
//
//
// }
//
// export const realTimeMatcherService = new RealTimeMatcherService();