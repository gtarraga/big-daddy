import type { InsertStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceEventJob } from '../../queue/types';
import { mergeResultsSimple } from '../utils';
import { executeOnShards, logWriteIfResharding, invalidateCacheForWrite, getCachedQueryPlanData } from '../utils/write';
import { injectVirtualShardFilter } from '../utils/helpers';

/**
 * Build a key value for an indexed column(s) from row data
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
function buildIndexKeyValue(rowData: Record<string, any>, columns: string[]): string | null {
	if (columns.length === 1) {
		const value = rowData[columns[0]!];
		if (value === null || value === undefined) {
			return null;
		}
		return String(value);
	} else {
		// Composite index - build key from all column values
		const values = columns.map((col) => rowData[col]);
		if (values.some((v) => v === null || v === undefined)) {
			return null;
		}
		return JSON.stringify(values);
	}
}

/**
 * Extract inserted rows and generate index maintenance events
 *
 * For each shard that received inserts:
 * 1. Extract column values from statement and params
 * 2. Deduplicate by (index_name, key_value, shard_id)
 * 3. Generate 'add' events
 */
function generateInsertIndexEvents(
	statement: InsertStatement,
	params: any[],
	shardsToQuery: ShardInfo[],
	virtualIndexes: Array<{ index_name: string; columns: string }>,
	shardMapping: Map<string, number[]>, // Maps row index to shard IDs it goes to
): IndexMaintenanceEventJob['events'] {
	// Use a Set to deduplicate: key is "index_name:key_value:shard_id"
	const dedupedMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'add';
		}
	>();

	// Extract column names and values from INSERT statement
	const columns = statement.columns;
	if (!columns || columns.length === 0) {
		return []; // No columns specified, can't extract values
	}

	// Extract values from the VALUES clause
	const values = statement.values;
	if (!values || values.length === 0) {
		return []; // No values to insert
	}

	// For each inserted row
	values.forEach((valueList, rowIndex) => {
		// Build a row object with column names mapped to values
		const rowData: Record<string, any> = {};
		valueList.forEach((value, colIndex) => {
			const colIdent = columns[colIndex];
			if (colIdent) {
				// Extract column name from Identifier
				const colName = typeof colIdent === 'string' ? colIdent : (colIdent as any).name;
				// Value is either a Literal or Placeholder
				if (typeof value === 'object' && value !== null) {
					if ('type' in value && value.type === 'Placeholder') {
						// It's a placeholder - get value from params
						const paramIndex = (value as any).parameterIndex;
						rowData[colName] = params[paramIndex] ?? null;
					} else if ('type' in value && value.type === 'Literal') {
						// It's a literal value
						rowData[colName] = (value as any).value;
					}
				} else {
					// Direct value (shouldn't happen with parsed AST, but handle it)
					rowData[colName] = value;
				}
			}
		});

		// For each index, extract key values
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];
			const keyValue = buildIndexKeyValue(rowData, indexColumns);

			if (keyValue === null) {
				// Skip NULL values - they're not indexed
				continue;
			}

			// Get shard IDs this row goes to
			const shardIds = shardMapping.get(String(rowIndex)) || [];
			for (const shardId of shardIds) {
				const dedupKey = `${index.index_name}:${keyValue}:${shardId}`;
				if (!dedupedMap.has(dedupKey)) {
					dedupedMap.set(dedupKey, {
						index_name: index.index_name,
						key_value: keyValue,
						shard_id: shardId,
						operation: 'add',
					});
				}
			}
		}
	});

	return Array.from(dedupedMap.values());
}

/**
 * Handle INSERT query
 *
 * This handler:
 * 1. Gets the query plan (which shards to insert to) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Executes the query on all target shards in parallel
 * 4. Enqueues async index maintenance for any indexes on this table
 * 5. Invalidates relevant cache entries
 * 6. Merges and returns results
 */
export async function handleInsert(
	statement: InsertStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, indexQueue, correlationId } = context;
	const tableName = statement.table.name;

	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(context, tableName, statement, params);

	logger.info('Query plan determined for INSERT', {
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Execute query on all target shards in parallel
	const { results, shardStats } = await executeOnShards(context, shardsToQuery, statement, params);

	logger.info('Shard execution completed for INSERT', {
		shardsQueried: shardsToQuery.length,
	});

	// STEP 4: Index maintenance - generate and queue index events if indexes exist
	if (planData.virtualIndexes.length > 0) {
		// Map all rows to all target shards (simplified: we don't know exact per-row shard distribution yet)
		// In practice, each row will hit one shard, but for event generation we map to all shardsToQuery
		const shardMapping = new Map<string, number[]>();
		const shardIds = shardsToQuery.map((s: ShardInfo) => s.shard_id);
		if (statement.values) {
			statement.values.forEach((_, rowIndex) => {
				shardMapping.set(String(rowIndex), shardIds);
			});
		}

		// Generate deduplicated index events
		const events = generateInsertIndexEvents(
			statement,
			params,
			shardsToQuery,
			planData.virtualIndexes,
			shardMapping,
		);

		logger.info('Generated index maintenance events for INSERT', {
			eventCount: events.length,
		});

		// Queue the index maintenance events if any
		if (events.length > 0 && indexQueue) {
			const job: IndexMaintenanceEventJob = {
				type: 'maintain_index_events',
				database_id: databaseId,
				table_name: tableName,
				events,
				created_at: new Date().toISOString(),
				correlation_id: correlationId,
			};

			await indexQueue.send(job);

			logger.info('Index maintenance events queued for INSERT', {
				eventCount: events.length,
			});
		}
	}

	// STEP 5: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

	// STEP 6: Merge results from all shards
	const result = mergeResultsSimple(results, false);

	// Add shard statistics
	result.shardStats = shardStats;

	logger.info('INSERT query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
