import { logger } from '../logger';
import { Job } from '../types';

export class JobProcessingService {
    private activeJobs = new Map<string, Job>();
    private readonly MAX_CONCURRENT_JOBS = 1000;

    async addJob(job: Job): Promise<void> {
        if (this.activeJobs.size >= this.MAX_CONCURRENT_JOBS) {

            logger.error(`Max concurrent jobs reached - cannot add job: ${job.id}`);
            throw new Error('Max concurrent jobs reached');
        }

        this.activeJobs.set(job.id, job);

        logger.debug(
            `Job Added - JobId: ${job.id}, Customer: ${job.customerId}, ActiveJobs: ${this.activeJobs.size}`
        );
    }

    async addJobs(jobs: Job[]): Promise<{ added: string[]; skipped: string[] }> {
        const addedJobs: string[] = [];
        const skippedJobs: string[] = [];

        for (const job of jobs) {
            if (this.activeJobs.size >= this.MAX_CONCURRENT_JOBS) {

                logger.warn(`Max Concurrent Jobs Reached - Skipping Job: ${job.id}`);
                skippedJobs.push(job.id);
                continue;
            }

            this.activeJobs.set(job.id, job);
            addedJobs.push(job.id);
        }

        logger.info(
            `Multiple Jobs Added - Added: ${addedJobs.length}, Skipped: ${skippedJobs.length}, TotalActive: ${this.activeJobs.size}`
        );
        return { added: addedJobs, skipped: skippedJobs };
    }

    getAllJobs(): Job[] {
        return Array.from(this.activeJobs.values());
    }

    removeJob(jobId: string): boolean {
        const removed = this.activeJobs.delete(jobId);
        if (removed) {

            logger.debug(`Job Removed - JobId: ${jobId}, Remaining: ${this.activeJobs.size}`);
        }
        return removed;
    }

    getJob(jobId: string): Job | null {
        return this.activeJobs.get(jobId) || null;
    }

    getActiveJobsCount(): number {
        return this.activeJobs.size;
    }

    hasJob(jobId: string): boolean {
        return this.activeJobs.has(jobId);
    }

    dumpJobs(): void {
        // 🔥 FIX: Correct syntax
        logger.info(`Active Jobs (${this.activeJobs.size}): ${Array.from(this.activeJobs.keys()).join(', ')}`);
    }
}

export const jobProcessingService = new JobProcessingService();