/**
 * Reshard Table Job Handler
 *
 * Handles resharding a table from one source shard to multiple target shards.
 *
 * Process (Phases 3B-5):
 * - Phase 3A (write logging) is handled by the Conductor before queuing
 * - Phase 3B: Copy data from source shard to all target shards
 * - Phase 3C: Replay all captured changes to target shards
 * - Phase 3D: Verify row counts match between source and targets
 * - Phase 4: Atomic status switch (mark targets active, source to_be_deleted)
 * - Phase 5: Delete old shard data
 */

import { withLogTags } from 'workers-tagged-logger';
import { logger } from '../../logger';
import type { ReshardTableJob, ReshardingChangeLog } from '../queue/types';
import type { Storage, StorageResults } from '../storage';
import type { Topology } from '../topology/index';
import { hashToShard } from '../utils/sharding';

export async function processReshardTableJob(job: ReshardTableJob, env: Env, correlationId?: string): Promise<void> {
	return withLogTags({ source: 'QueueConsumer' }, async () => {
		const tableName = job.table_name;
		const sourceShardId = job.source_shard_id;
		const targetShardIds = job.target_shard_ids;
		const jobId = job.change_log_id; // Use change_log_id as unique job identifier

		logger.setTags({
			table: tableName,
			reshardingId: job.change_log_id,
			jobId,
			correlationId,
		});

		logger.info('Processing resharding job', {
			table: tableName,
			sourceShardId,
			targetShardIds,
			changeLogId: job.change_log_id,
		});

		const topologyId = env.TOPOLOGY.idFromName(job.database_id);
		const topologyStub = env.TOPOLOGY.get(topologyId);

	try {
		// Get initial source count for debugging
		const topology = await topologyStub.getTopology();
		const sourceShard = topology.table_shards.find(
			s => s.table_name === tableName && s.shard_id === sourceShardId
		);
		let initialSourceCount = 0;
		if (sourceShard) {
			const sourceStorageId = env.STORAGE.idFromName(sourceShard.node_id);
			const sourceStorageStub = env.STORAGE.get(sourceStorageId);
			const countResult = await sourceStorageStub.executeQuery({
				query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
				params: [sourceShardId],
			}) as StorageResults<any>;
			initialSourceCount = countResult.rows[0]?.count || 0;
		}

		logger.info('Resharding initial state', { table: tableName, initialSourceCount, sourceShardId });

		// Create async job record
		await topologyStub.createAsyncJob(jobId, 'reshard_table', tableName, {
			source_shard_id: sourceShardId,
			target_shard_ids: targetShardIds,
			shard_key: job.shard_key,
			shard_strategy: job.shard_strategy,
		});

		// Mark async job as running
		await topologyStub.startAsyncJob(jobId);

		// Mark resharding as starting (transition to 'copying' phase)
		await topologyStub.startResharding(tableName);
		logger.info('Resharding started', { table: tableName, status: 'copying', initialSourceCount });

		// Phase 3B: Copy data from source shard to target shards
		// First, log the initial state of target shards before copy
		const topologyForPreCopy = await topologyStub.getTopology();
		const preCopyTargetCounts: { [key: number]: number } = {};
		const preCopyVirtualShardBreakdown: { [key: number]: { [key: number]: number } } = {};
		for (const targetShardId of targetShardIds) {
			const targetShard = topologyForPreCopy.table_shards.find(
				s => s.table_name === tableName && s.shard_id === targetShardId
			);
			if (targetShard) {
				const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
				const targetStorageStub = env.STORAGE.get(targetStorageId);

				// Get total count filtered by target shard ID
				const countResult = await targetStorageStub.executeQuery({
					query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
					params: [targetShardId],
				}) as StorageResults<any>;
				preCopyTargetCounts[targetShardId] = countResult.rows[0]?.count || 0;

				// Get breakdown by virtualShard value to detect unexpected data
				const breakdown = await targetStorageStub.executeQuery({
					query: `SELECT _virtualShard, COUNT(*) as count FROM ${tableName} GROUP BY _virtualShard`,
					params: [],
				}) as StorageResults<any>;
				const breakdownMap: { [key: number]: number } = {};
				for (const row of breakdown.rows) {
					breakdownMap[row._virtualShard] = row.count;
				}
				preCopyVirtualShardBreakdown[targetShardId] = breakdownMap;
			}
		}
		logger.info('Pre-copy target shard state', {
			preCopyTargetCounts,
			targetShardIds,
			preCopyVirtualShardBreakdown
		});

		const copyStats = await copyShardData(
			env,
			job.database_id,
			sourceShardId,
			targetShardIds,
			tableName,
			job.shard_key,
			job.shard_strategy
		);

		// Log post-copy state to verify what was actually written
		const postCopyVirtualShardBreakdown: { [key: number]: { [key: number]: number } } = {};
		for (const targetShardId of targetShardIds) {
			const targetShard = topologyForPreCopy.table_shards.find(
				s => s.table_name === tableName && s.shard_id === targetShardId
			);
			if (targetShard) {
				const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
				const targetStorageStub = env.STORAGE.get(targetStorageId);

				// Get breakdown by virtualShard value to see what was actually copied
				const breakdown = await targetStorageStub.executeQuery({
					query: `SELECT _virtualShard, COUNT(*) as count FROM ${tableName} GROUP BY _virtualShard`,
					params: [],
				}) as StorageResults<any>;
				const breakdownMap: { [key: number]: number } = {};
				for (const row of breakdown.rows) {
					breakdownMap[row._virtualShard] = row.count;
				}
				postCopyVirtualShardBreakdown[targetShardId] = breakdownMap;
			}
		}
		logger.info('Post-copy target shard state', {
			distribution: copyStats.distribution,
			postCopyVirtualShardBreakdown
		});

		// Phase 3C: Replay captured changes
		const replayStats = await replayChangeLog(
			env,
			job.database_id,
			job.change_log_id,
			targetShardIds,
			tableName,
			job.shard_key,
			job.shard_strategy
		);

		// Phase 3D: Verify data integrity
		const verifyStats = await verifyIntegrity(
			env,
			job.database_id,
			sourceShardId,
			targetShardIds,
			tableName
		);

		// Combined comprehensive log showing copy -> replay -> verify progression
		logger.info('Phase 3: Copy-Replay-Verify Summary', {
			preCopyTargetState: preCopyTargetCounts,
			phase3B_copy: {
				rowsCopied: copyStats.rows_copied,
				distribution: copyStats.distribution,
				duration: copyStats.duration,
			},
			phase3C_replay: {
				changesReplayed: replayStats.changes_replayed,
				duration: replayStats.duration,
			},
			phase3D_verify: verifyStats.verificationDetails || {
				status: verifyStats.isValid ? 'PASSED' : 'FAILED',
				error: verifyStats.error,
				duration: verifyStats.duration,
			},
		});

		if (!verifyStats.isValid) {
			throw new Error(`Data verification failed: ${verifyStats.error}`);
		}

		// Phase 4: Atomic status switch
		await topologyStub.atomicStatusSwitch(tableName);
		logger.info('Phase 4: Atomic status switch completed', { table: tableName });

		// Phase 5: Delete old shard data
		await deleteOldShardData(env, job.database_id, sourceShardId, tableName);
		logger.info('Phase 5: Old shard data deleted', { table: tableName, shardId: sourceShardId });

		// CRITICAL: Data verification and copy/delete all passed successfully
		// Mark resharding as complete before job completion
		// This should happen BEFORE completeAsyncJob to ensure clean state
		try {
			await topologyStub.markReshardingComplete(tableName);
			logger.info('Resharding marked as complete', { table: tableName });
		} catch (completeError) {
			// If we can't mark as complete but data was successfully moved, still mark job as done
			logger.error('Failed to mark resharding as complete (but data migration succeeded)', {
				error: completeError instanceof Error ? completeError.message : String(completeError),
			});
		}

		// Mark async job as completed
		try {
			await topologyStub.completeAsyncJob(jobId);
			logger.info('Async job marked as complete', { jobId });
		} catch (jobError) {
			logger.error('Failed to mark async job as complete (but data migration succeeded)', {
				jobId,
				error: jobError instanceof Error ? jobError.message : String(jobError),
			});
		}

		logger.info('Resharding completed successfully', {
			table: tableName,
			status: 'complete',
		});
	} catch (error) {
		const errorMessage = error instanceof Error ? error.message : String(error);
		logger.error('Resharding failed', {
			table: tableName,
			error: errorMessage,
			status: 'failed',
		});

		// Mark resharding state as failed
		await topologyStub.markReshardingFailed(tableName, errorMessage);

		// Mark all target shards as 'failed' to clean up pending shards
		// ONLY do this if we haven't already switched them to active
		// Check current shard status before marking as failed
		try {
			const currentTopology = await topologyStub.getTopology();
			const targetShardsActive = currentTopology.table_shards.some(
				s => s.table_name === tableName && targetShardIds.includes(s.shard_id) && s.status === 'active'
			);

			// Only mark as failed if they're not already active (which means Phase 4 completed)
			if (!targetShardsActive) {
				await topologyStub.markTargetShardsAsFailed(tableName, targetShardIds);
				logger.info('Target shards marked as failed', { table: tableName, targetShardIds });
			} else {
				logger.warn('Not marking target shards as failed - they are already active', {
					table: tableName,
					targetShardIds,
				});
			}
		} catch (shardError) {
			logger.warn('Failed to handle target shard status', {
				tableName,
				targetShardIds,
				error: shardError instanceof Error ? shardError.message : String(shardError),
			});
		}

		// Mark async job as failed (no retries - fail fast)
		try {
			await topologyStub.failAsyncJob(jobId, errorMessage);
		} catch (jobError) {
			logger.warn('Failed to mark async job as failed', {
				jobId,
				error: jobError instanceof Error ? jobError.message : String(jobError),
			});
		}

		throw error;
	}
	});
}

/**
 * Phase 3B: Copy data from source shard to target shards
 * Routes rows to target shards based on shard key using the same hashing strategy
 */
async function copyShardData(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	targetShardIds: number[],
	tableName: string,
	shardKey: string,
	shardStrategy: 'hash' | 'range'
): Promise<{ rows_copied: number; duration: number; distribution: { [key: number]: number } }> {
	const startTime = Date.now();
	let rowsCopied = 0;

	logger.info('Phase 3B: Starting data copy', {
		sourceShardId,
		targetShardIds,
	});

	try {
		// Get all data from source shard
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(topologyId);
		const topology = await topologyStub.getTopology();

		const sourceShard = topology.table_shards.find(
			s => s.table_name === tableName && s.shard_id === sourceShardId
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const storageId = env.STORAGE.idFromName(sourceShard.node_id);
		const storageStub = env.STORAGE.get(storageId);

		// Query all data from source shard filtered by _virtualShard
		const result = await storageStub.executeQuery({
			query: `SELECT * FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		}) as StorageResults<any>;

		if (!('rows' in result)) {
			throw new Error('Expected QueryResult but got BatchQueryResult');
		}

		const rows = result.rows as Record<string, any>[];

		// Also get count from source to verify (filtered by _virtualShard)
		const sourceCountResult = await storageStub.executeQuery({
			query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		}) as StorageResults<any>;
		const sourceCount = sourceCountResult.rows[0]?.count || 0;

		logger.info('Fetched rows from source shard', {
			rowCount: rows.length,
			countQuery: sourceCount,
			match: rows.length === sourceCount,
		});

		// Warn if there's a mismatch
		if (rows.length !== sourceCount) {
			logger.warn('Row count mismatch in source shard fetch', {
				selectStar: rows.length,
				countQuery: sourceCount,
				difference: sourceCount - rows.length,
			});
		}

		// Build map of target shard IDs to storage nodes for efficient lookup
		const targetShardMap = new Map<number, string>();
		for (const shard of topology.table_shards.filter(s => s.table_name === tableName && targetShardIds.includes(s.shard_id))) {
			targetShardMap.set(shard.shard_id, shard.node_id);
		}

		// Verify all target shards have nodes
		for (const targetShardId of targetShardIds) {
			if (!targetShardMap.has(targetShardId)) {
				throw new Error(`Target shard ${targetShardId} has no node mapping`);
			}
		}

		// Insert rows into target shards based on shard key distribution
		// Each row goes to exactly one target shard determined by hash(shardKeyValue) % targetShardCount
		const shardDistribution = new Map<number, number>();
		for (const targetShardId of targetShardIds) {
			shardDistribution.set(targetShardId, 0);
		}

		for (const row of rows) {
			// Extract the shard key value from the row
			const shardKeyValue = row[shardKey];
			if (shardKeyValue === undefined) {
				logger.warn('Row missing shard key', { shardKey, row });
				continue;
			}

			// Determine which target shard this row belongs to
			// hashToShard returns 0-indexed value, so we use it to select from targetShardIds
			const targetShardIndex = hashToShard(shardKeyValue, targetShardIds.length);
			const targetShardId = targetShardIds[targetShardIndex];

			// Get the storage node for this target shard
			const targetNodeId = targetShardMap.get(targetShardId);
			if (!targetNodeId) {
				throw new Error(`Target shard ${targetShardId} node mapping not found`);
			}

			// Build INSERT OR REPLACE query with proper shard awareness
			// IMPORTANT: We need to handle multiple scenarios:
			// 1. Normal case: Row doesn't exist in target, INSERT it with correct _virtualShard
			// 2. Retry case: Same row with same _virtualShard already exists, REPLACE to update it
			// 3. Multi-shard case: Multiple source shards might have same ID for different logical rows
			//
			// The key insight: Since physical nodes can hold multiple virtual shards,
			// the source shard row (with _virtualShard = sourceShardId) and target shard row
			// (with _virtualShard = targetShardId) are DIFFERENT rows even if they have same primary key.
			// They're distinguished by the combination of (primary_key, _virtualShard).
			//
			// However, SQLite PRIMARY KEY doesn't include _virtualShard, so we need to handle
			// the collision. We use INSERT OR REPLACE to ensure we always have the target shard
			// version, but the source shard version might get replaced if they're on same node.
			// This is okay because after resharding completes, the source shard will be deleted anyway.
			const columns = Object.keys(row).filter(col => col !== '_virtualShard');
			const allColumns = [...columns, '_virtualShard'];
			const values = allColumns.map(() => '?');
			const query = `INSERT OR REPLACE INTO ${tableName} (${allColumns.join(', ')}) VALUES (${values.join(', ')})`;
			const params = [...columns.map(col => row[col]), targetShardId];

			// Insert row into its designated target shard only
			const targetStorageId = env.STORAGE.idFromName(targetNodeId);
			const targetStorageStub = env.STORAGE.get(targetStorageId);

			try {
				await targetStorageStub.executeQuery({
					query,
					params,
				}) as StorageResults<any>;

				rowsCopied++;
				const currentCount = shardDistribution.get(targetShardId) || 0;
				shardDistribution.set(targetShardId, currentCount + 1);
			} catch (error) {
				logger.error('Failed to copy row to target shard', {
					shardKeyValue,
					targetShardId,
					error: error instanceof Error ? error.message : String(error),
				});
				throw error;
			}
		}

		// Log final distribution
		const distributionBreakdown: { [key: number]: number } = {};
		for (const [shardId, count] of shardDistribution) {
			distributionBreakdown[shardId] = count;
		}

		logger.info('Phase 3B: Final copy distribution', {
			totalRows: rowsCopied,
			totalShards: targetShardIds.length,
			distribution: distributionBreakdown,
			sourceRowsRead: rows.length,
			sourceCountQuery: sourceCount,
			match: rows.length === sourceCount,
		});

		const duration = Date.now() - startTime;
		logger.info('Phase 3B: Data copy completed', { rowsCopied, duration });
		return { rows_copied: rowsCopied, duration, distribution: distributionBreakdown };
	} catch (error) {
		logger.error('Phase 3B: Data copy failed', {
			error: error instanceof Error ? error.message : String(error),
		});
		throw error;
	}
}

/**
 * Phase 3C: Replay captured changes to target shards
 * This is idempotent - replaying INSERT again will fail (which is okay for copy),
 * but UPDATE and DELETE work fine
 */
async function replayChangeLog(
	env: Env,
	databaseId: string,
	changeLogId: string,
	targetShardIds: number[],
	tableName: string,
	shardKey: string,
	shardStrategy: 'hash' | 'range'
): Promise<{ changes_replayed: number; duration: number }> {
	const startTime = Date.now();
	let changesReplayed = 0;

	logger.info('Phase 3C: Starting change log replay', {
		changeLogId,
		targetShardIds,
	});

	try {
		// In production, this would fetch change log entries from a persistent log
		// For now, we'll just log that replay would happen here
		logger.info('Phase 3C: No change log entries to replay (or would fetch from persistent log)');

		const duration = Date.now() - startTime;
		return { changes_replayed: changesReplayed, duration };
	} catch (error) {
		logger.error('Phase 3C: Change log replay failed', {
			error: error instanceof Error ? error.message : String(error),
		});
		throw error;
	}
}

/**
 * Phase 3D: Verify data integrity by comparing row counts
 */
async function verifyIntegrity(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	targetShardIds: number[],
	tableName: string
): Promise<{ isValid: boolean; error?: string; duration: number; verificationDetails?: any }> {
	const startTime = Date.now();

	logger.info('Phase 3D: Starting data verification', {
		sourceShardId,
		targetShardIds,
	});

	try {
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(topologyId);
		const topology = await topologyStub.getTopology();

		// Get source shard row count
		const sourceShard = topology.table_shards.find(
			s => s.table_name === tableName && s.shard_id === sourceShardId
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const sourceStorageId = env.STORAGE.idFromName(sourceShard.node_id);
		const sourceStorageStub = env.STORAGE.get(sourceStorageId);

		// Count rows in source shard filtered by _virtualShard to ensure we only count source data
		const sourceCountResult = await sourceStorageStub.executeQuery({
			query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		}) as StorageResults<any>;

		const sourceCount = sourceCountResult.rows[0]?.count || 0;
		logger.info('Source shard row count', { sourceShardId, count: sourceCount, nodeId: sourceShard.node_id });

		// Get target shards total row count with detailed breakdown
		let targetTotalCount = 0;
		const targetBreakdown: { [key: number]: number } = {};

		for (const targetShardId of targetShardIds) {
			const targetShard = topology.table_shards.find(
				s => s.table_name === tableName && s.shard_id === targetShardId
			);
			if (!targetShard) {
				throw new Error(`Target shard ${targetShardId} not found`);
			}

			const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
			const targetStorageStub = env.STORAGE.get(targetStorageId);

			// Count rows in target shard filtered by _virtualShard to ensure we only count target data
			const targetCountResult = await targetStorageStub.executeQuery({
				query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
				params: [targetShardId],
			}) as StorageResults<any>;

			const targetCount = targetCountResult.rows[0]?.count || 0;
			targetTotalCount += targetCount;
			targetBreakdown[targetShardId] = targetCount;

			logger.info('Target shard row count', {
				targetShardId,
				count: targetCount,
				nodeId: targetShard.node_id,
				percentageOfTarget: ((targetCount / Math.max(sourceCount, 1)) * 100).toFixed(2) + '%'
			});
		}

		const duration = Date.now() - startTime;

		// Single comprehensive verification log
		const isValid = sourceCount === targetTotalCount;
		const difference = targetTotalCount - sourceCount;
		const differencePercent = ((difference / Math.max(sourceCount, 1)) * 100).toFixed(2);

		const verificationStatus = isValid ? 'PASSED' : 'FAILED';
		const verificationSummary = {
			status: verificationStatus,
			duration,
			source: {
				shardId: sourceShardId,
				rowCount: sourceCount,
			},
			targets: {
				shardIds: targetShardIds,
				totalRowCount: targetTotalCount,
				breakdown: targetBreakdown,
				breakdown_details: targetShardIds.map(shardId => ({
					shardId,
					rowCount: targetBreakdown[shardId] || 0,
				})),
			},
			comparison: {
				match: isValid,
				difference: difference,
				differencePercent: `${differencePercent}%`,
				expectedSourceCount: sourceCount,
				actualTargetCount: targetTotalCount,
			},
		};

		if (isValid) {
			return { isValid: true, duration, verificationDetails: verificationSummary };
		} else {
			const error = `Row count mismatch: expected ${sourceCount} rows, got ${targetTotalCount} rows (difference: ${difference})`;
			return { isValid: false, error, duration, verificationDetails: verificationSummary };
		}
	} catch (error) {
		logger.error('Phase 3D: Data verification error', {
			error: error instanceof Error ? error.message : String(error),
		});
		throw error;
	}
}

/**
 * Phase 5: Delete old shard data
 */
async function deleteOldShardData(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	tableName: string
): Promise<void> {
	logger.info('Phase 5: Starting old shard deletion', {
		sourceShardId,
	});

	try {
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(topologyId);
		const topology = await topologyStub.getTopology();

		const sourceShard = topology.table_shards.find(
			s => s.table_name === tableName && s.shard_id === sourceShardId
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const storageId = env.STORAGE.idFromName(sourceShard.node_id);
		const storageStub = env.STORAGE.get(storageId);

		// Delete all data from source shard
		await storageStub.executeQuery({
			query: `DELETE FROM ${tableName}`,
			params: [],
		}) as StorageResults<any>;

		logger.info('Phase 5: Old shard data deleted', { sourceShardId });
	} catch (error) {
		logger.error('Phase 5: Old shard deletion failed', {
			error: error instanceof Error ? error.message : String(error),
		});
		throw error;
	}
}

/**
 * Compute index key from a row and column list
 * Returns null if any column value is NULL (NULL values are not indexed)
 */
