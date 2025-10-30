import { logger } from '../logger';
import { Job } from '../types';

export class JobProcessingService {
    private activeJobs = new Map<string, Job>();

    async addJob(job: Job): Promise<void> {
        this.activeJobs.set(job.id, job);

        logger.debug(
            `Job Added - JobId: ${job.id}, Customer: ${job.customerId}, ActiveJobs: ${this.activeJobs.size}`
        );
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

}

export const jobProcessingService = new JobProcessingService();