/**
 * Build Index Job Handler
 *
 * Builds a virtual index from existing data across all shards.
 *
 * Process:
 * 1. Get all shards for this table from topology
 * 2. For each distinct value in the indexed column(s):
 *    - Query all shards to find which contain that value
 *    - Create index entry mapping value → shard_ids
 * 3. Update index status to 'ready' or 'failed'
 */

import { withLogTags } from 'workers-tagged-logger';
import { logger } from '../../logger';
import type { IndexBuildJob } from '../queue/types';

export async function processBuildIndexJob(job: IndexBuildJob, env: Env, correlationId?: string): Promise<void> {
	const columnList = job.columns.join(', ');
	logger.setTags({
		table: job.table_name,
		indexName: job.index_name,
	});

	logger.info('Building virtual index', {
		indexName: job.index_name,
		table: job.table_name,
		columns: columnList,
	});

	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(topologyId);

	try {
		// 1. Get all shards for this table from topology
		const topology = await topologyStub.getTopology();
		const tableShards = topology.table_shards.filter((s) => s.table_name === job.table_name);

		if (tableShards.length === 0) {
			logger.error('No shards found for table', { table: job.table_name });
			throw new Error(`No shards found for table '${job.table_name}'`);
		}

		logger.info('Found shards for table', {
			shardCount: tableShards.length,
		});

		// 2. Collect all distinct values from all shards
		// Map: composite key value → Set<shard_id>
		const valueToShards = new Map<string, Set<number>>();

		for (const shard of tableShards) {
			try {
				// Get storage stub for this shard
				const storageId = env.STORAGE.idFromName(shard.node_id);
				const storageStub = env.STORAGE.get(storageId);

				// Query for all distinct combinations of indexed columns
				const result = await storageStub.executeQuery({
					query: `SELECT DISTINCT ${columnList} FROM ${job.table_name}`,
					params: [],
					queryType: 'SELECT',
				});

				// Type guard: single query always returns QueryResult
				if (!('rows' in result)) {
					throw new Error('Expected QueryResult but got BatchQueryResult');
				}

				// Extract rows with proper typing to avoid deep instantiation with Disposable types
				const resultRows = (result as any).rows as Record<string, any>[];

				// For each distinct combination, add this shard to its shard set
				for (const row of resultRows) {
					// Build composite key from all indexed columns
					// For single column: just the value
					// For multiple columns: JSON array of values
					let keyValue: string;

					if (job.columns.length === 1) {
						const value: any = row[job.columns[0]!];
						// Skip NULL values
						if (value === null || value === undefined) {
							continue;
						}
						keyValue = String(value);
					} else {
						// Composite index - build key from all column values
						const values = job.columns.map(col => row[col]);
						// Skip if any value is NULL
						if (values.some(v => v === null || v === undefined)) {
							continue;
						}
						// Store as JSON array
						keyValue = JSON.stringify(values);
					}

					if (!valueToShards.has(keyValue)) {
						valueToShards.set(keyValue, new Set());
					}

					valueToShards.get(keyValue)!.add(shard.shard_id);
				}

				console.log(`Processed shard ${shard.shard_id}, found values for index`);
			} catch (error) {
				console.error(`Error querying shard ${shard.shard_id}:`, error);
				throw new Error(`Failed to query shard ${shard.shard_id}: ${error instanceof Error ? error.message : String(error)}`);
			}
		}

		logger.info('Collected distinct values from shards', {
			distinctValues: valueToShards.size,
		});

		// 3. Create index entries in batch
		const entries = Array.from(valueToShards.entries()).map(([keyValue, shardIdSet]) => ({
			keyValue,
			shardIds: Array.from(shardIdSet).sort((a, b) => a - b), // Sort for consistency
		}));

		if (entries.length > 0) {
			const result = await topologyStub.batchUpsertIndexEntries(job.index_name, entries);
			logger.info('Created index entries', {
				entryCount: result.count,
			});
		}

		// 4. Update index status to 'ready'
		await topologyStub.updateIndexStatus(job.index_name, 'ready');

		logger.info('Index build complete', {
			indexName: job.index_name,
			uniqueValues: entries.length,
			status: 'ready',
		});
	} catch (error) {
		logger.error('Failed to build index', {
			indexName: job.index_name,
			error: error instanceof Error ? error.message : String(error),
		});

		// Update index status to 'failed'
		const errorMessage = error instanceof Error ? error.message : String(error);
		await topologyStub.updateIndexStatus(job.index_name, 'failed', errorMessage);

		throw error;
	}
}
