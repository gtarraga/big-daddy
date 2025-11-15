/**
 * Maintain Index Job Handler
 *
 * Maintains virtual indexes after UPDATE/DELETE operations.
 *
 * Process:
 * 1. For each affected shard, read current data
 * 2. Compute what index entries should exist based on current state
 * 3. Compare with topology's current state
 * 4. Apply changes in a single batched call
 */

import { logger } from '../../logger';
import type { IndexMaintenanceJob } from '../queue/types';

/**
 * Compute the index key value for a row
 * Returns null if any column value is NULL (don't index NULL values)
 */
function computeIndexKey(row: Record<string, any>, columns: string[]): string | null {
	const values = [];
	for (const col of columns) {
		const val = row[col];
		if (val === null || val === undefined) {
			return null; // Don't index NULL values
		}
		values.push(val);
	}
	return columns.length === 1 ? String(values[0]) : JSON.stringify(values);
}

export async function processIndexMaintenanceJob(job: IndexMaintenanceJob, env: Env, correlationId?: string): Promise<void> {
	logger.setTags({
		table: job.table_name,
	});

	logger.info('Maintaining indexes after write operation', {
		operation: job.operation,
		affectedIndexes: job.affected_indexes.length,
		shardCount: job.shard_ids.length,
	});
	console.log(`Maintaining indexes for ${job.operation} on ${job.table_name}, shards: ${job.shard_ids.join(',')}`);

	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(job.database_id));
	const topology = await topologyStub.getTopology();

	// Get index definitions
	const indexes = topology.virtual_indexes.filter(
		(idx) => job.affected_indexes.includes(idx.index_name) && idx.status === 'ready'
	);

	if (indexes.length === 0) {
		console.log('No ready indexes to maintain');
		return;
	}

	// Collect all index changes across all shards
	const allChanges: Array<{
		operation: 'add' | 'remove';
		index_name: string;
		key_value: string;
		shard_id: number;
	}> = [];

	// Process each affected shard
	for (const shardId of job.shard_ids) {
		const shard = topology.table_shards.find((s) => s.table_name === job.table_name && s.shard_id === shardId);
		if (!shard) {
			console.warn(`Shard ${shardId} not found for table ${job.table_name}`);
			continue;
		}

		const storageStub = env.STORAGE.get(env.STORAGE.idFromName(shard.node_id));

		// For each index, rebuild entries for this shard
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns);

			// Read current state from shard
			const columnList = indexColumns.join(', ');
			const query = `SELECT ${columnList} FROM ${job.table_name}`;

			try {
				const result = await storageStub.executeQuery({
					query,
					params: [],
					queryType: 'SELECT',
				});

				// Type guard: single query always returns QueryResult
				if (!('rows' in result)) {
					throw new Error('Expected QueryResult but got BatchQueryResult');
				}

				// Extract rows with proper typing to avoid deep instantiation with Disposable types
				const resultRows = (result as any).rows as Record<string, any>[];

				// Build set of values that SHOULD include this shard
				const shouldExist = new Set<string>();
				for (const row of resultRows) {
					const keyValue = computeIndexKey(row, indexColumns);
					if (keyValue) {
						shouldExist.add(keyValue);
					}
				}

				// Get current state from Topology
				const currentEntries = await topologyStub.getIndexEntriesForShard(index.index_name, shardId);
				const currentlyExists = new Set(currentEntries.map((e) => e.key_value));

				// Compute changes
				// Add shard to entries where it should exist but doesn't
				for (const keyValue of shouldExist) {
					if (!currentlyExists.has(keyValue)) {
						allChanges.push({
							operation: 'add',
							index_name: index.index_name,
							key_value: keyValue,
							shard_id: shardId,
						});
					}
				}

				// Remove shard from entries where it shouldn't exist but does
				for (const keyValue of currentlyExists) {
					if (!shouldExist.has(keyValue)) {
						allChanges.push({
							operation: 'remove',
							index_name: index.index_name,
							key_value: keyValue,
							shard_id: shardId,
						});
					}
				}
			} catch (error) {
				console.error(`Error querying shard ${shardId}:`, error);
				throw error;
			}
		}
	}

	// Apply all changes in a single Topology call
	if (allChanges.length > 0) {
		await topologyStub.batchMaintainIndexes(allChanges);
		console.log(`Applied ${allChanges.length} index changes for ${job.table_name}`);
	} else {
		console.log(`No index changes needed for ${job.table_name}`);
	}
}
