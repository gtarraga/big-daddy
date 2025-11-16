import type { SelectStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import { extractTableName as extractTableNameFromAST } from '../../utils/ast-utils';
import type { QueryResult, ShardStats, QueryHandlerContext, ShardInfo } from '../types';
import { mergeResultsSimple } from '../utils';

/**
 * Execute a SELECT query on the appropriate shards
 *
 * This handler:
 * 1. Gets the query plan (which shards to query) from topology
 * 2. Executes the query on all target shards in parallel
 * 3. Merges results from all shards
 * 4. Adds cache statistics
 */
export async function handleSelect(
	statement: SelectStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, storage, topology, cache, correlationId } = context;
	const tableName = extractTableNameFromAST(statement);

	if (!tableName) {
		throw new Error('Could not determine table name from SELECT query');
	}

	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	// This determines which shards to query
	const { planData, cacheHit } = await getCachedQueryPlanData(
		context,
		tableName,
		statement,
		params,
	);

	logger.info('Query plan determined for SELECT', {
		cacheHit,
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Execute query on all target shards in parallel
	const { results, shardStats } = await executeOnShards(context, shardsToQuery, query, params);

	// STEP 3: Merge results from all shards
	const result = mergeResultsSimple(results, true);

	// STEP 4: Add cache statistics
	const stats = cache.getStats();
	result.cacheStats = {
		cacheHit,
		totalHits: stats.hits,
		totalMisses: stats.misses,
		cacheSize: stats.size,
	};

	// Add shard statistics
	result.shardStats = shardStats;

	logger.info('SELECT query completed', {
		shardsQueried: shardsToQuery.length,
		rowCount: result.rows.length,
	});

	return result;
}

/**
 * Get query plan data with caching
 *
 * This method checks the cache first before calling the Topology DO.
 * For cacheable queries (SELECT with WHERE clause), this can eliminate
 * the Topology DO call entirely.
 *
 * @returns Query plan data and whether it was a cache hit
 */
async function getCachedQueryPlanData(
	context: QueryHandlerContext,
	tableName: string,
	statement: any,
	params: any[],
): Promise<{ planData: any; cacheHit: boolean }> {
	const { topology, databaseId, cache, correlationId } = context;

	// Try to build a cache key for this query
	const cacheKey = cache.buildQueryPlanCacheKey(tableName, statement, params);

	// Check cache if we have a valid cache key
	if (cacheKey) {
		const cached = cache.getQueryPlanData(cacheKey);
		if (cached) {
			return { planData: cached, cacheHit: true };
		}
	}

	// Cache miss - fetch from Topology DO
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);
	const planData = await topologyStub.getQueryPlanData(tableName, statement, params, correlationId);

	// Store in cache if cacheable
	if (cacheKey) {
		const ttl = cache.getTTLForQuery(statement);
		cache.setQueryPlanData(cacheKey, planData, ttl);
	}

	return { planData, cacheHit: false };
}

/**
 * Execute query on all target shards in parallel with batching
 *
 * This method:
 * 1. Processes shards in batches of 7 to avoid overwhelming the system
 * 2. Injects _virtualShard filter for shard-specific isolation
 * 3. Tracks execution statistics for each shard
 * 4. Returns merged results and shard performance data
 */
async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	query: string,
	params: any[],
): Promise<{ results: QueryResult[]; shardStats: ShardStats[] }> {
	const { storage } = context;
	const BATCH_SIZE = 7;
	const allResults: QueryResult[] = [];
	const allShardStats: ShardStats[] = [];

	// Import utils here to avoid circular dependencies
	const { injectVirtualShardFilter } = await import('../utils');

	// Process shards in batches of 7
	for (let i = 0; i < shardsToQuery.length; i += BATCH_SIZE) {
		const batch = shardsToQuery.slice(i, i + BATCH_SIZE);

		const batchResults = await Promise.all(
			batch.map(async (shard) => {
				const shardStartTime = Date.now();
				// Get the storage stub using the node_id from table_shards mapping
				const storageId = storage.idFromName(shard.node_id);
				const storageStub = storage.get(storageId);

				try {
					// Inject _virtualShard filter into the query for this specific shard
					// This is critical because during resharding, a physical storage node can have
					// data from multiple virtual shards, so we need to filter at the SQL level
					const { modifiedQuery, modifiedParams } = injectVirtualShardFilter(
						query,
						params,
						shard.shard_id,
						shard.table_name,
						'SELECT',
					);

					// Execute the query
					const rawResult = (await storageStub.executeQuery({
						query: modifiedQuery,
						params: modifiedParams,
						queryType: 'SELECT',
					})) as any;

					const shardDuration = Date.now() - shardStartTime;

					// Track shard stats
					const shardStat: ShardStats = {
						shardId: shard.shard_id,
						nodeId: shard.node_id,
						rowsReturned: rawResult.rows.length,
						duration: shardDuration,
					};

					allShardStats.push(shardStat);

					return {
						rows: rawResult.rows,
						rowsAffected: rawResult.rowsAffected ?? 0,
					};
				} catch (error) {
					logger.error('Shard query failed', {
						shardId: shard.shard_id,
						nodeId: shard.node_id,
						error: error instanceof Error ? error.message : String(error),
						duration: Date.now() - shardStartTime,
					});
					throw error;
				}
			}),
		);

		allResults.push(...batchResults);
	}

	return { results: allResults, shardStats: allShardStats };
}
