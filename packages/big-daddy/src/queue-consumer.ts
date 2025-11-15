/**
 * Queue Consumer Worker for Async Operations
 *
 * This worker processes jobs from the vitess-index-jobs queue:
 * - IndexBuildJob: Build a new virtual index from existing data
 * - IndexMaintenanceJob: Maintain indexes after UPDATE/DELETE operations
 * - ReshardTableJob: Reshard a table from source to target shards
 * - ReshardingChangeLog: Change log entries (batched and processed during resharding replay)
 */

import { withLogTags } from 'workers-tagged-logger';
import { logger } from './logger';
import type { IndexJob, ReshardingChangeLog, MessageBatch } from './engine/queue/types';
import { processBuildIndexJob, processIndexMaintenanceJob, processReshardTableJob } from './engine/async-jobs';

/**
 * Queue message handler
 * Receives batches of up to 10 messages and processes them
 */
export async function queueHandler(batch: MessageBatch<IndexJob>, env: Env, batchCorrelationId?: string): Promise<void> {
	return withLogTags({ source: 'QueueConsumer' }, async () => {
		const cid = batchCorrelationId || crypto.randomUUID();
		logger.setTags({
			correlationId: cid,
			requestId: cid,
			component: 'QueueConsumer',
			operation: 'queueHandler',
		});

		logger.info('Processing queue batch', {
			batchSize: batch.messages.length,
			queue: batch.queue,
		});

		// Process messages in parallel where possible
		const results = await Promise.allSettled(
			batch.messages.map(async (message) => {
				try {
					const jobCorrelationId = message.body.correlation_id || cid;
					logger.setTags({
						correlationId: jobCorrelationId,
						requestId: jobCorrelationId,
						jobId: message.id,
						jobType: message.body.type,
					});

					logger.info('Processing queue message', {
						jobId: message.id,
						jobType: message.body.type,
						attempts: message.attempts,
					});

					await processIndexJob(message.body, env, jobCorrelationId);

					logger.info('Successfully processed job', {
						jobId: message.id,
						jobType: message.body.type,
						status: 'success',
					});
				} catch (error) {
					logger.error('Failed to process job', {
						jobId: message.id,
						jobType: message.body.type,
						error: error instanceof Error ? error.message : String(error),
						status: 'failure',
					});
					// Throwing will cause the message to be retried
					throw error;
				}
			}),
		);

		// Log summary
		const successful = results.filter((r) => r.status === 'fulfilled').length;
		const failed = results.filter((r) => r.status === 'rejected').length;

		logger.info('Batch processing complete', {
			batchSize: batch.messages.length,
			successful,
			failed,
			status: failed > 0 ? 'partial' : 'success',
		});
	});
}

/**
 * Process a single job - dispatches to appropriate handler
 */
async function processIndexJob(job: IndexJob, env: Env, correlationId?: string): Promise<void> {
	switch (job.type) {
		case 'build_index':
			await processBuildIndexJob(job, env, correlationId);
			break;
		case 'maintain_index':
			await processIndexMaintenanceJob(job, env, correlationId);
			break;
		case 'reshard_table':
			await processReshardTableJob(job, env, correlationId);
			break;
		case 'resharding_change_log':
			// Change log entries are batched and processed during Phase 3C
			// They don't need individual processing - they're fetched from queue during replay
			logger.debug('Change log entry queued for later replay', {
				reshardingId: (job as ReshardingChangeLog).resharding_id,
			});
			break;
		default:
			throw new Error(`Unknown job type: ${(job as any).type}`);
	}
}
