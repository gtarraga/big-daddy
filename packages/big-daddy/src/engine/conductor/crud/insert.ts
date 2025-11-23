import type { InsertStatement } from '@databases/sqlite-ast';
import { generate } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo, SqlParam } from '../types';
import { mergeResultsSimple } from '../utils';
import { hashToShardId } from '../utils/helpers';
import {
	executeOnShards,
	logWriteIfResharding,
	invalidateCacheForWrite,
	getCachedQueryPlanData,
} from '../utils/write';
import { prepareIndexMaintenanceQueries, dispatchIndexSyncingFromQueryResults } from '../utils/index-maintenance';

/**
 * Group INSERT rows by target shard and create per-shard INSERT statements
 *
 * For each row in the INSERT:
 * 1. Extract the shard key value
 * 2. Hash it to determine the target shard
 * 3. Group rows by shard
 * 4. Create a separate INSERT statement for each shard with only its rows
 * 5. Remap parameter indices for each shard's statement
 *
 * @param statement The original INSERT statement
 * @param shardKeyColumn The name of the shard key column
 * @param numShards Total number of shards
 * @param params Query parameters
 * @returns Map of shardId -> { statement: INSERT statement, params: filtered params }
 */
function groupInsertByShards(
	statement: InsertStatement,
	shardKeyColumn: string,
	numShards: number,
	params: SqlParam[],
): Map<number, { statement: InsertStatement; params: SqlParam[] }> {
	// Map of shardId -> { rows, paramIndices }
	const shardGroups = new Map<number, { rows: typeof statement.values; paramIndices: number[] }>();

	const columns = statement.columns;
	if (!columns || columns.length === 0) {
		logger.warn('No columns in INSERT statement');
		return new Map();
	}

	const values = statement.values;
	if (!values || values.length === 0) {
		logger.warn('No values in INSERT statement');
		return new Map();
	}

	// Find the index of the shard key column
	const shardKeyIndex = columns.findIndex((col) => {
		const colName = typeof col === 'string' ? col : (col as any).name;
		return colName === shardKeyColumn;
	});


	if (shardKeyIndex === -1) {
		logger.warn('Shard key column not found in INSERT, may not distribute optimally', {
			shardKey: shardKeyColumn,
		});
		// If shard key not in INSERT, can't determine target shard
		return new Map();
	}

	// Group each row by target shard, tracking which parameters belong to this row
	const numParamsPerRow = columns.length; // Each column gets one parameter
	values.forEach((valueList, rowIndex) => {
		// Extract the shard key value from this row
		const shardKeyExpr = valueList[shardKeyIndex];
		let shardKeyValue: any;

		if (!shardKeyExpr) {
			logger.warn('Missing shard key value in INSERT row', { rowIndex });
			return;
		}

		if (typeof shardKeyExpr === 'object' && shardKeyExpr !== null) {
			if ('type' in shardKeyExpr && shardKeyExpr.type === 'Placeholder') {
				shardKeyValue = params[(shardKeyExpr as any).parameterIndex];
			} else if ('type' in shardKeyExpr && shardKeyExpr.type === 'Literal') {
				shardKeyValue = (shardKeyExpr as any).value;
			}
		} else {
			shardKeyValue = shardKeyExpr;
		}

		// Hash the shard key value to determine target shard
		const shardId = hashToShardId(shardKeyValue, numShards);

		// Track parameters for this row
		const rowParamIndices = valueList
			.map((expr) => {
				if (typeof expr === 'object' && expr !== null && 'type' in expr && expr.type === 'Placeholder') {
					return (expr as any).parameterIndex;
				}
				return -1; // Not a placeholder, skip
			})
			.filter((idx) => idx !== -1);


		// Group this row and its parameters by shard
		if (!shardGroups.has(shardId)) {
			shardGroups.set(shardId, { rows: [], paramIndices: [] });
		}
		shardGroups.get(shardId)!.rows.push(valueList);
		shardGroups.get(shardId)!.paramIndices.push(...rowParamIndices);
	});


	// Create INSERT statements and filtered parameter arrays for each shard
	const result = new Map<number, { statement: InsertStatement; params: SqlParam[] }>();
	for (const [shardId, group] of shardGroups) {
		// Extract only the parameters needed for this shard's rows
		const shardParams = group.paramIndices.map((idx) => params[idx]);

		// Remap the placeholder indices in the rows to the new parameter array
		// paramCounter tracks across all rows in this shard, not per row
		let paramCounter = 0;
		const remappedRows = group.rows.map((valueList) => {
			return valueList.map((expr) => {
				if (typeof expr === 'object' && expr !== null && 'type' in expr && expr.type === 'Placeholder') {
					// Remap the placeholder index
					const remappedExpr = { ...(expr as any), parameterIndex: paramCounter };
					paramCounter++;
					return remappedExpr;
				}
				return expr;
			});
		});

		result.set(shardId, {
			statement: {
				...statement,
				values: remappedRows,
			},
			params: shardParams,
		});
	}

	return result;
}

/**
 * Extract inserted rows from INSERT statement for index maintenance
 *
 * Builds a row map indexed by shard ID for index maintenance operations.
 */
function extractInsertedRows(
	statement: InsertStatement,
	params: any[],
	shardsToQuery: ShardInfo[],
): Map<number, Record<string, any>[]> {
	const newRows = new Map<number, Record<string, any>[]>();

	// Initialize empty arrays for each shard
	for (const shard of shardsToQuery) {
		newRows.set(shard.shard_id, []);
	}

	const columns = statement.columns;
	if (!columns || columns.length === 0) {
		return newRows;
	}

	const values = statement.values;
	if (!values || values.length === 0) {
		return newRows;
	}

	// For each inserted row
	values.forEach((valueList) => {
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

		// Map this row to all target shards (will be filtered by per-shard statements)
		for (const shard of shardsToQuery) {
			const shardRows = newRows.get(shard.shard_id) || [];
			shardRows.push(rowData);
			newRows.set(shard.shard_id, shardRows);
		}
	});

	return newRows;
}

/**
 * Handle INSERT query
 *
 * This handler:
 * 1. Gets the query plan and table metadata from topology
 * 2. Groups INSERT rows by target shard based on shard key hashing
 * 3. Logs the write if resharding is in progress
 * 4. Prepares and executes per-shard INSERT queries
 * 5. Dispatches index maintenance events if needed
 * 6. Invalidates relevant cache entries
 * 7. Merges and returns results
 */
export async function handleInsert(
	statement: InsertStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { topology, databaseId, storage } = context;
	const tableName = statement.table.name;
	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(context, tableName, statement, params);

	logger.info('Query plan determined for INSERT', {
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
		shardKey: planData.shardKey,
	});

	const allShards = planData.shardsToQuery;

	// STEP 2: Group INSERT rows by target shard based on shard key
	const perShardStatements = groupInsertByShards(statement, planData.shardKey, allShards.length, params);


	// If we couldn't group rows (shard key not in INSERT), execute on all shards
	const shardsToQuery = perShardStatements.size === 0 ? allShards : allShards.filter(s => perShardStatements.has(s.shard_id));

	logger.info('INSERT rows grouped by shard', {
		shardsWithRows: perShardStatements.size,
		totalShards: allShards.length,
		shardsToQueryLength: shardsToQuery.length,
	});

	// STEP 3: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 4: Execute per-shard INSERTs
	let execResult;
	if (perShardStatements.size > 0) {

		// Execute only the shards that have rows
		const resultsPerShard = new Map<number, QueryResult>();
		const shardStatsPerShard: any[] = [];

		for (const [shardId, { statement: shardStatement, params: shardParams }] of perShardStatements) {
			const shard = allShards.find(s => s.shard_id === shardId);
			if (!shard) {
					continue;
			}


			// For per-shard execution, use the filtered and remapped params for this shard
			const queries = prepareIndexMaintenanceQueries(
				planData.virtualIndexes.length > 0,
				shardStatement,
				undefined,
				shardParams,
			);


			const result = await executeOnShards(context, [shard], queries);
			const shardResult = (result.results as QueryResult[])[0] || { rows: [], rowsAffected: 0 };
			resultsPerShard.set(shardId, shardResult);
			if (result.shardStats) {
				shardStatsPerShard.push(...result.shardStats);
			}
		}

		execResult = {
			results: Array.from(resultsPerShard.values()),
			shardStats: shardStatsPerShard,
		};
	} else {
		// Fallback: execute on all shards if we can't determine distribution
		logger.warn('Could not determine shard distribution, executing on all shards', { tableName });
		const queries = prepareIndexMaintenanceQueries(
			planData.virtualIndexes.length > 0,
			statement,
			undefined,
			params,
		);
		execResult = await executeOnShards(context, shardsToQuery, queries);
	}

	logger.info('Shard execution completed for INSERT', {
		shardsQueried: shardsToQuery.length,
	});

	// STEP 5: Dispatch index maintenance if needed
	if (planData.virtualIndexes.length > 0) {
		// Extract inserted rows from statement
		const newRows = extractInsertedRows(statement, params, shardsToQuery);

		// Dispatch index maintenance with extracted rows
		await dispatchIndexSyncingFromQueryResults(
			'INSERT',
			execResult.results as QueryResult[][],
			tableName,
			shardsToQuery,
			planData.virtualIndexes,
			context,
			() => ({ newRows }),
		);
	}

	// STEP 6: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

	// STEP 7: Merge results from all shards
	const results = execResult.results as QueryResult[];
	const result = mergeResultsSimple(results, statement);

	// Add shard statistics
	result.shardStats = execResult.shardStats;

	logger.info('INSERT query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
