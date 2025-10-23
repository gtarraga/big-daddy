import { parse } from '@databases/sqlite-ast';
import type {
	Statement,
	SelectStatement,
	InsertStatement,
	UpdateStatement,
	DeleteStatement,
	CreateTableStatement,
	CreateIndexStatement,
} from '@databases/sqlite-ast';
import type { DurableObjectStub } from 'cloudflare:workers';
import type { Storage, QueryResult as StorageQueryResult, QueryType } from '../Storage/Storage';
import type { Topology, TableMetadata } from '../Topology/Topology';
import { hashToShard } from './utils/sharding';
import { extractTableName, extractWhereClause, extractValueFromExpression, getQueryType, buildQuery } from './utils/ast-utils';
import { extractTableMetadata } from './utils/schema-utils';
import type { IndexJob } from '../Queue/types';

/**
 * Result from a SQL query execution
 */
export interface QueryResult {
	rows: Record<string, any>[];
	rowsAffected?: number;
}

/**
 * Conductor - Routes SQL queries to the appropriate storage shards
 *
 * The Conductor sits between the client and the distributed storage layer,
 * parsing queries, determining which shards to target, and coordinating
 * execution across multiple storage nodes.
 */
export class ConductorClient {
	constructor(
		private databaseId: string,
		private storage: DurableObjectNamespace<Storage>,
		private topology: DurableObjectNamespace<Topology>,
		private indexQueue?: Queue,
	) {}

	/**
	 * Execute a SQL query using tagged template literals
	 *
	 * @example
	 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;
	 * await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;
	 */
	sql = async (strings: TemplateStringsArray, ...values: any[]): Promise<QueryResult> => {
		// STEP 1: Parse - Build and parse the SQL query
		const { query, params } = buildQuery(strings, values);
		const statement = parse(query);

		// Handle CREATE TABLE statements (special case - affects all nodes)
		if (statement.type === 'CreateTableStatement') {
			return await this.handleCreateTable(statement as CreateTableStatement, query);
		}

		// Handle CREATE INDEX statements (special case - creates virtual index)
		if (statement.type === 'CreateIndexStatement') {
			return await this.handleCreateIndex(statement as CreateIndexStatement);
		}

		// STEP 2: Route - Determine which shards to target
		const tableName = extractTableName(statement);
		if (!tableName) {
			throw new Error('Could not determine table name from query');
		}

		const { tableMetadata, tableShards } = await this.getTableTopologyInfo(tableName);
		const shardsToQuery = await this.determineShardTargets(statement, tableMetadata, tableShards, params, tableName);

		// STEP 2.5: Fetch old values for UPDATE/DELETE before executing (needed for index maintenance)
		const oldValuesForIndexMaintenance = await this.fetchOldValuesForIndexMaintenance(statement, tableName, shardsToQuery, query, params);

		// STEP 3: Execute - Run query on all target shards in parallel
		const queryType = getQueryType(statement);
		const results = await this.executeOnShards(shardsToQuery, query, params, queryType);

		// STEP 3.5: Index Maintenance - Update virtual indexes if this was a write operation
		await this.maintainIndexes(statement, tableName, shardsToQuery, params, results, oldValuesForIndexMaintenance);

		// STEP 4: Merge - Combine results from all shards
		return this.mergeResults(results, statement);
	};

	/**
	 * Handle CREATE TABLE statement execution
	 */
	private async handleCreateTable(statement: CreateTableStatement, query: string): Promise<QueryResult> {
		const tableName = statement.table.name;

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Check if table already exists in topology
		const existingTable = topologyData.tables.find((t) => t.table_name === tableName);

		// If IF NOT EXISTS is specified and table exists, skip
		if (statement.ifNotExists && existingTable) {
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		// If table exists and IF NOT EXISTS was not specified, throw error
		if (existingTable) {
			throw new Error(`Table '${tableName}' already exists in topology`);
		}

		// Extract metadata from the CREATE TABLE statement
		const metadata = extractTableMetadata(statement);

		// Add table to topology
		await topologyStub.updateTopology({
			tables: {
				add: [metadata],
			},
		});

		// Execute CREATE TABLE on all storage nodes in parallel (use actual node IDs from topology)
		await Promise.all(
			topologyData.storage_nodes.map(async (node) => {
				const storageId = this.storage.idFromName(node.node_id);
				const storageStub = this.storage.get(storageId);

				await storageStub.executeQuery({
					query,
					params: [],
					queryType: 'CREATE',
				});
			}),
		);

		return {
			rows: [],
			rowsAffected: 0,
		};
	}

	/**
	 * Handle CREATE INDEX statement execution
	 *
	 * This method:
	 * 1. Validates the index and table
	 * 2. Creates SQLite indexes on all storage shards (for query performance)
	 * 3. Creates the virtual index definition in Topology with status 'building'
	 * 4. Enqueues an IndexBuildJob to the queue for async processing (builds virtual index metadata)
	 * 5. Returns immediately to the client (non-blocking)
	 */
	private async handleCreateIndex(statement: CreateIndexStatement): Promise<QueryResult> {
		const indexName = statement.name.name;
		const tableName = statement.table.name;

		// Extract column names from the CREATE INDEX statement
		const columns = statement.columns.map((col) => col.name);

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);

		// Get all shards for this table to create SQLite indexes
		const { tableShards } = await this.getTableTopologyInfo(tableName);

		// Build the CREATE INDEX SQL statement
		const uniqueClause = statement.unique ? 'UNIQUE ' : '';
		const ifNotExistsClause = statement.ifNotExists ? 'IF NOT EXISTS ' : '';
		const columnList = columns.join(', ');
		const createIndexSQL = `CREATE ${uniqueClause}INDEX ${ifNotExistsClause}${indexName} ON ${tableName}(${columnList})`;

		// Create SQLite indexes on all storage shards in parallel
		// This ensures queries on individual shards are fast (reduces rows scanned)
		await Promise.all(
			tableShards.map(async (shard) => {
				const storageId = this.storage.idFromName(shard.node_id);
				const storageStub = this.storage.get(storageId);

				try {
					await storageStub.executeQuery({
						query: createIndexSQL,
						params: [],
						queryType: 'CREATE',
					});
				} catch (error) {
					// If IF NOT EXISTS is specified, ignore "already exists" errors
					if (statement.ifNotExists && (error as Error).message.includes('already exists')) {
						return;
					}
					throw error;
				}
			}),
		);

		// Create the virtual index in Topology with 'building' status
		// Virtual index provides shard-level routing optimization
		const indexType = statement.unique ? 'unique' : 'hash';
		const result = await topologyStub.createVirtualIndex(indexName, tableName, columns, indexType);

		if (!result.success) {
			// If IF NOT EXISTS is specified and index already exists, return success
			if (statement.ifNotExists && result.error?.includes('already exists')) {
				return {
					rows: [],
					rowsAffected: 0,
				};
			}

			throw new Error(result.error || 'Failed to create index');
		}

		// Enqueue index build job for background processing
		// This populates the virtual index metadata with existing data
		await this.enqueueIndexJob({
			type: 'build_index',
			database_id: this.databaseId,
			table_name: tableName,
			columns: columns,
			index_name: indexName,
			created_at: new Date().toISOString(),
		});

		return {
			rows: [],
			rowsAffected: 0,
		};
	}

	/**
	 * Fetch topology information for a table
	 */
	private async getTableTopologyInfo(tableName: string): Promise<{
		tableMetadata: TableMetadata;
		tableShards: Array<{ table_name: string; shard_id: number; node_id: string }>;
	}> {
		// Get topology information
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find the table metadata
		const tableMetadata = topologyData.tables.find((t) => t.table_name === tableName);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		// Get table shards for this table
		const tableShards = topologyData.table_shards.filter((s) => s.table_name === tableName);

		if (tableShards.length === 0) {
			throw new Error(`No shards found for table '${tableName}'`);
		}

		return { tableMetadata, tableShards };
	}

	/**
	 * Determine which shards should be targeted for a query
	 *
	 * This method handles routing logic for all statement types:
	 * - INSERT: Routes to a single shard based on shard key value
	 * - SELECT/UPDATE/DELETE: Analyzes WHERE clause to determine if we can route to a single shard
	 *   or need to query all shards. Checks virtual indexes if shard key isn't used.
	 */
	private async determineShardTargets(
		statement: Statement,
		tableMetadata: TableMetadata,
		tableShards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
		tableName: string,
	): Promise<Array<{ table_name: string; shard_id: number; node_id: string }>> {
		// Handle INSERT statements - route to specific shard based on shard key value
		if (statement.type === 'InsertStatement') {
			const shardId = this.getShardIdForInsert(statement as InsertStatement, tableMetadata, tableShards.length, params);
			const shard = tableShards.find((s) => s.shard_id === shardId);
			if (!shard) {
				throw new Error(`Shard ${shardId} not found for table '${tableName}'`);
			}
			return [shard];
		}

		// Handle SELECT/UPDATE/DELETE statements - check WHERE clause
		const whereClause = extractWhereClause(statement);

		if (whereClause) {
			// WHERE clause exists - check if it filters on shard key
			const shardId = this.getShardIdFromWhere(whereClause, tableMetadata, tableShards.length, params);
			if (shardId !== null) {
				// WHERE clause filters on shard key with equality - route to specific shard
				const shard = tableShards.find((s) => s.shard_id === shardId);
				if (!shard) {
					throw new Error(`Shard ${shardId} not found for table '${tableName}'`);
				}
				return [shard];
			}

			// Shard key not used - check if we can use a virtual index
			const indexedShards = await this.getShardsFromIndexedWhere(whereClause, tableName, tableShards, params);
			if (indexedShards !== null) {
				return indexedShards;
			}
		}

		// No WHERE clause, or WHERE clause doesn't filter on shard key or indexed column - query all shards
		return tableShards;
	}

	/**
	 * Execute a query on multiple shards in parallel, with batching
	 *
	 * Cloudflare has a limit of 6 subrequests in parallel (we use 7 to be safe with the limit).
	 * This method batches shard queries into groups of 7 to respect this constraint.
	 */
	private async executeOnShards(
		shardsToQuery: Array<{ table_name: string; shard_id: number; node_id: string }>,
		query: string,
		params: any[],
		queryType: QueryType,
	): Promise<QueryResult[]> {
		const BATCH_SIZE = 7;
		const allResults: QueryResult[] = [];

		// Process shards in batches of 7
		for (let i = 0; i < shardsToQuery.length; i += BATCH_SIZE) {
			const batch = shardsToQuery.slice(i, i + BATCH_SIZE);

			const batchResults = await Promise.all(
				batch.map(async (shard) => {
					// Get the storage stub using the node_id from table_shards mapping
					const storageId = this.storage.idFromName(shard.node_id);
					const storageStub = this.storage.get(storageId);

					// Execute the query (single query always returns StorageQueryResult)
					const rawResult = await storageStub.executeQuery({
						query,
						params,
						queryType,
					});

					// Convert StorageQueryResult to QueryResult
					const result = rawResult as unknown as StorageQueryResult;
					return {
						rows: result.rows,
						rowsAffected: result.rowsAffected,
					};
				}),
			);

			allResults.push(...batchResults);
		}

		return allResults;
	}

	/**
	 * Merge results from multiple shards
	 */
	private mergeResults(results: QueryResult[], statement: Statement): QueryResult {
		if (results.length === 1) {
			return results[0];
		}

		const isSelect = statement.type === 'SelectStatement';
		return this.mergeResultsSimple(results, isSelect);
	}


	/**
	 * Calculate which shard an INSERT should go to based on the shard key value
	 */
	private getShardIdForInsert(statement: InsertStatement, tableMetadata: TableMetadata, numShards: number, params: any[]): number {
		// Find the index of the shard key column
		const shardKeyIndex = statement.columns?.findIndex((col) => col.name === tableMetadata.shard_key);

		if (shardKeyIndex === undefined || shardKeyIndex === -1) {
			throw new Error(`Shard key '${tableMetadata.shard_key}' not found in INSERT columns`);
		}

		// Get the shard key value from the first row of values
		// For now, only support single-row inserts
		if (statement.values.length === 0) {
			throw new Error('INSERT statement has no values');
		}

		const shardKeyExpression = statement.values[0][shardKeyIndex];

		// Extract the actual value from the Expression
		const value = extractValueFromExpression(shardKeyExpression, params);

		if (value === null || value === undefined) {
			throw new Error('Could not resolve shard key value from parameters');
		}

		// Hash the value to determine the shard
		return hashToShard(value, numShards);
	}


	/**
	 * Extract shard ID from WHERE clause if it filters on the shard key with equality
	 * Returns null if the WHERE clause doesn't provide a specific shard key value
	 */
	private getShardIdFromWhere(where: any, tableMetadata: TableMetadata, numShards: number, params: any[]): number | null {
		// For MVP, we only support simple equality comparisons: WHERE shard_key = value
		// We check if the WHERE clause is a binary operation with '=' operator
		if (where.type === 'BinaryOperation' && where.operator === '=') {
			// Check if one side is the shard key column
			const leftIsShardKey = where.left.type === 'ColumnReference' && where.left.name === tableMetadata.shard_key;
			const rightIsShardKey = where.right.type === 'ColumnReference' && where.right.name === tableMetadata.shard_key;

			let value: any = null;

			if (leftIsShardKey) {
				// Shard key is on the left, value is on the right
				value = extractValueFromExpression(where.right, params);
			} else if (rightIsShardKey) {
				// Shard key is on the right, value is on the left
				value = extractValueFromExpression(where.left, params);
			}

			if (value !== null) {
				return hashToShard(value, numShards);
			}
		}

		// WHERE clause doesn't provide a specific shard key value
		// This includes cases like:
		// - Complex conditions (AND/OR)
		// - Range queries (>, <, >=, <=)
		// - Non-equality operators
		// - Filters on non-shard-key columns
		return null;
	}

	/**
	 * Check if WHERE clause can use a virtual index to reduce shard fan-out
	 * Returns array of shards to query, or null if no index can be used
	 */
	private async getShardsFromIndexedWhere(
		where: any,
		tableName: string,
		tableShards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
	): Promise<Array<{ table_name: string; shard_id: number; node_id: string }> | null> {
		// Get topology to check for indexes
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Try to match composite index from AND conditions
		// Example: WHERE col1 = val1 AND col2 = val2
		if (where.type === 'BinaryOperation' && where.operator === 'AND') {
			const columnValues = this.extractColumnValuesFromAnd(where, params);

			if (columnValues.size > 0) {
				// Find a composite index that matches these columns (leftmost prefix)
				const index = topologyData.virtual_indexes.find((idx) => {
					if (idx.table_name !== tableName || idx.status !== 'ready') {
						return false;
					}
					const indexColumns = JSON.parse(idx.columns);

					// Check if we have values for a leftmost prefix of this index
					// For index (a, b, c), we can use: (a), (a,b), or (a,b,c)
					// But NOT (b), (c), (b,c), etc.
					if (indexColumns.length === 1) {
						// Single column index
						return columnValues.has(indexColumns[0]);
					}

					// Multi-column index - check leftmost prefix
					let matchCount = 0;
					for (let i = 0; i < indexColumns.length; i++) {
						if (columnValues.has(indexColumns[i])) {
							matchCount++;
						} else {
							// Leftmost prefix broken
							break;
						}
					}

					// We can use this index if we matched at least the first column
					return matchCount > 0 && matchCount === Math.min(columnValues.size, indexColumns.length);
				});

				if (index) {
					const indexColumns = JSON.parse(index.columns);

					// Build the key value
					let keyValue: string;
					if (indexColumns.length === 1) {
						const value = columnValues.get(indexColumns[0])!;
						if (value === null || value === undefined) {
							return null;
						}
						keyValue = String(value);
					} else {
						// Multi-column index - build composite key
						const values: any[] = [];
						for (const col of indexColumns) {
							const value = columnValues.get(col);
							if (value === undefined) {
								// Don't have value for this column - can't use full index
								break;
							}
							if (value === null) {
								return null; // NULL values aren't indexed
							}
							values.push(value);
						}

						if (values.length === 0) {
							return null;
						}

						keyValue = values.length === 1 ? String(values[0]) : JSON.stringify(values);
					}

					// Lookup which shards contain this value
					const shardIds = await topologyStub.getIndexedShards(index.index_name, keyValue);

					if (shardIds === null || shardIds.length === 0) {
						return [];
					}

					const targetShards = tableShards.filter((s) => shardIds.includes(s.shard_id));
					return targetShards;
				}
			}
		}

		// Try to match single equality condition
		// Example: WHERE col = val
		if (where.type === 'BinaryOperation' && where.operator === '=') {
			// Check if one side is a column reference
			let columnName: string | null = null;
			let valueExpression: any | null = null;

			if (where.left.type === 'ColumnReference') {
				columnName = where.left.name;
				valueExpression = where.right;
			} else if (where.right.type === 'ColumnReference') {
				columnName = where.right.name;
				valueExpression = where.left;
			}

			if (!columnName || !valueExpression) {
				return null;
			}

			// Find a ready index for this column (can be single or composite with this as leftmost)
			const index = topologyData.virtual_indexes.find((idx) => {
				if (idx.table_name !== tableName || idx.status !== 'ready') {
					return false;
				}
				const columns = JSON.parse(idx.columns);
				// Match single-column indexes or composite indexes with this as the first column
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			// Extract the value being searched for
			const value = extractValueFromExpression(valueExpression, params);
			if (value === null || value === undefined) {
				return null;
			}

			// Convert value to string (same as how we store single-column indexes)
			const keyValue = String(value);

			// Lookup which shards contain this value
			const shardIds = await topologyStub.getIndexedShards(index.index_name, keyValue);

			if (shardIds === null || shardIds.length === 0) {
				return [];
			}

			const targetShards = tableShards.filter((s) => shardIds.includes(s.shard_id));
			return targetShards;
		}

		// Try to match IN query
		// Example: WHERE col IN (val1, val2, val3)
		if (where.type === 'InOperation') {
			const columnName = where.left.type === 'ColumnReference' ? where.left.name : null;

			if (!columnName) {
				return null;
			}

			// Find a ready single-column index for this column
			const index = topologyData.virtual_indexes.find((idx) => {
				if (idx.table_name !== tableName || idx.status !== 'ready') {
					return false;
				}
				const columns = JSON.parse(idx.columns);
				// Only match single-column indexes for IN queries
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			// Extract all values from the IN list
			const values: any[] = [];
			if (where.right.type === 'List') {
				for (const item of where.right.items) {
					const value = extractValueFromExpression(item, params);
					if (value !== null && value !== undefined) {
						values.push(value);
					}
				}
			}

			if (values.length === 0) {
				return null;
			}

			// Look up shards for each value and collect unique shard IDs
			const allShardIds = new Set<number>();

			for (const value of values) {
				const keyValue = String(value);
				const shardIds = await topologyStub.getIndexedShards(index.index_name, keyValue);

				if (shardIds && shardIds.length > 0) {
					shardIds.forEach(id => allShardIds.add(id));
				}
			}

			if (allShardIds.size === 0) {
				// None of the values exist in the index
				return [];
			}

			// Map shard IDs to actual shard objects
			const targetShards = tableShards.filter((s) => allShardIds.has(s.shard_id));
			return targetShards;
		}

		return null;
	}

	/**
	 * Extract column-value pairs from AND conditions
	 * Example: WHERE col1 = val1 AND col2 = val2 => Map { col1 => val1, col2 => val2 }
	 */
	private extractColumnValuesFromAnd(where: any, params: any[]): Map<string, any> {
		const columnValues = new Map<string, any>();

		// Recursively extract from AND tree
		const extract = (node: any) => {
			if (node.type === 'BinaryOperation') {
				if (node.operator === 'AND') {
					// Recurse into both sides
					extract(node.left);
					extract(node.right);
				} else if (node.operator === '=') {
					// Extract column = value
					let columnName: string | null = null;
					let valueExpression: any | null = null;

					if (node.left.type === 'ColumnReference') {
						columnName = node.left.name;
						valueExpression = node.right;
					} else if (node.right.type === 'ColumnReference') {
						columnName = node.right.name;
						valueExpression = node.left;
					}

					if (columnName && valueExpression) {
						const value = extractValueFromExpression(valueExpression, params);
						columnValues.set(columnName, value);
					}
				}
			}
		};

		extract(where);
		return columnValues;
	}

	/**
	 * Fetch old values for indexed columns before UPDATE/DELETE operations
	 * This is needed to maintain indexes correctly
	 */
	private async fetchOldValuesForIndexMaintenance(
		statement: Statement,
		tableName: string,
		shards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		query: string,
		params: any[],
	): Promise<Map<number, any[]> | null> {
		// Only fetch for UPDATE and DELETE
		if (statement.type !== 'UpdateStatement' && statement.type !== 'DeleteStatement') {
			return null;
		}

		// Get topology to check if there are any indexes
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find all ready indexes for this table
		const indexes = topologyData.virtual_indexes.filter((idx) => idx.table_name === tableName && idx.status === 'ready');

		if (indexes.length === 0) {
			// No indexes to maintain - skip fetching old values
			return null;
		}

		// Build SELECT query to fetch old values of all columns (we'll filter to indexed ones later)
		const whereClause = extractWhereClause(statement);

		if (!whereClause) {
			// No WHERE clause - would affect all rows, skip for now
			// TODO: Handle this case if needed
			return null;
		}

		// Fetch old values from each shard BEFORE the UPDATE/DELETE
		// We use SELECT * to get all columns, which avoids parameter binding issues
		const oldValuesByShard = new Map<number, any[]>();

		for (const shard of shards) {
			const storageId = this.storage.idFromName(shard.node_id);
			const storageStub = this.storage.get(storageId);

			try {
				// Build a simple WHERE clause for fetching
				// For UPDATE: Parse the SET clause to skip those parameters
				// For DELETE: Use all parameters
				let whereParams = params;

				if (statement.type === 'UpdateStatement') {
					// Skip the SET clause parameters - they come first in the params array
					const setCount = (statement as UpdateStatement).set.length;
					whereParams = params.slice(setCount);
				}

				// We'll use the original query to get the WHERE clause portion
				const whereIndex = query.toUpperCase().indexOf('WHERE');
				if (whereIndex === -1) {
					continue;
				}

				const whereClauseStr = query.substring(whereIndex + 6); // Skip 'WHERE '

				// SELECT * to get all columns (easier than figuring out which columns we need)
				const selectQuery = `SELECT * FROM ${tableName} WHERE ${whereClauseStr}`;

				const result = await storageStub.executeQuery({
					query: selectQuery,
					params: whereParams,
					queryType: 'SELECT',
				});

				oldValuesByShard.set(shard.shard_id, result.rows);
			} catch (error) {
				console.error(`Failed to fetch old values from shard ${shard.shard_id}:`, error);
				// Continue with other shards
			}
		}

		return oldValuesByShard;
	}

	/**
	 * Maintain virtual indexes after write operations (INSERT/UPDATE/DELETE)
	 * This runs synchronously to ensure indexes stay up-to-date
	 */
	private async maintainIndexes(
		statement: Statement,
		tableName: string,
		shardsAffected: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
		results: QueryResult[],
		oldValues: Map<number, any[]> | null,
	): Promise<void> {
		// Only maintain indexes for write operations
		if (statement.type !== 'InsertStatement' && statement.type !== 'UpdateStatement' && statement.type !== 'DeleteStatement') {
			return;
		}

		// Get topology to check for indexes on this table
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find all ready indexes for this table
		const indexes = topologyData.virtual_indexes.filter((idx) => idx.table_name === tableName && idx.status === 'ready');

		if (indexes.length === 0) {
			// No indexes to maintain
			return;
		}

		// Handle each statement type
		if (statement.type === 'InsertStatement') {
			await this.maintainIndexesForInsert(statement as InsertStatement, indexes, shardsAffected[0], params, topologyStub);
		} else if (statement.type === 'UpdateStatement') {
			await this.maintainIndexesForUpdate(statement as UpdateStatement, indexes, shardsAffected, params, topologyStub, tableName, oldValues);
		} else if (statement.type === 'DeleteStatement') {
			await this.maintainIndexesForDelete(statement as DeleteStatement, indexes, shardsAffected, params, topologyStub, tableName, oldValues);
		}
	}

	/**
	 * Maintain indexes after INSERT operation
	 */
	private async maintainIndexesForInsert(
		statement: InsertStatement,
		indexes: Array<{ index_name: string; columns: string }>,
		shard: { table_name: string; shard_id: number; node_id: string },
		params: any[],
		topologyStub: DurableObjectStub<Topology>,
	): Promise<void> {
		// Extract values from the INSERT statement
		if (!statement.columns || statement.values.length === 0) {
			return;
		}

		const row = statement.values[0]; // Only handle single-row inserts for now

		// For each index, extract the value(s) and update the index entry
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns);

			// Build composite key from all indexed columns
			const values: any[] = [];
			let hasNull = false;

			for (const colName of indexColumns) {
				const columnIndex = statement.columns.findIndex((col) => col.name === colName);
				if (columnIndex === -1) {
					// This column is not in the INSERT - skip this index
					hasNull = true;
					break;
				}

				const valueExpression = row[columnIndex];
				const value = extractValueFromExpression(valueExpression, params);

				if (value === null || value === undefined) {
					// NULL values are not indexed
					hasNull = true;
					break;
				}

				values.push(value);
			}

			if (hasNull || values.length !== indexColumns.length) {
				continue;
			}

			// Build the key value
			const keyValue = indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);

			// Add this shard to the index entry for this value
			await topologyStub.addShardToIndexEntry(index.index_name, keyValue, shard.shard_id);
		}
	}

	/**
	 * Maintain indexes after UPDATE operation
	 */
	private async maintainIndexesForUpdate(
		statement: UpdateStatement,
		indexes: Array<{ index_name: string; columns: string }>,
		shards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
		topologyStub: DurableObjectStub<Topology>,
		tableName: string,
		oldValuesByShard: Map<number, any[]> | null,
	): Promise<void> {
		if (!oldValuesByShard) {
			// No old values fetched (no indexes or no WHERE clause)
			return;
		}

		// Extract new values from the UPDATE statement
		const newValues = new Map<string, any>();
		for (const assignment of statement.set) {
			const columnName = assignment.column.name;
			const value = extractValueFromExpression(assignment.value, params);
			newValues.set(columnName, value);
		}

		// For each affected shard, update indexes using pre-fetched old values
		for (const shard of shards) {
			const oldRows = oldValuesByShard.get(shard.shard_id);
			if (!oldRows || oldRows.length === 0) {
				continue;
			}

			// For each row that was updated, update the indexes
			for (const oldRow of oldRows) {
				for (const index of indexes) {
					const indexColumns = JSON.parse(index.columns);

					// Build old composite key
					const oldValues: any[] = [];
					let oldHasNull = false;
					for (const colName of indexColumns) {
						const val = oldRow[colName];
						if (val === null || val === undefined) {
							oldHasNull = true;
							break;
						}
						oldValues.push(val);
					}

					// Build new composite key
					const newVals: any[] = [];
					let newHasNull = false;
					let anyChanged = false;
					for (const colName of indexColumns) {
						const newVal = newValues.has(colName) ? newValues.get(colName) : oldRow[colName];
						if (newVal === null || newVal === undefined) {
							newHasNull = true;
							break;
						}
						newVals.push(newVal);
						// Check if this column changed
						if (newValues.has(colName) && String(newVal) !== String(oldRow[colName])) {
							anyChanged = true;
						}
					}

					// Only update if something changed
					if (!anyChanged) {
						continue;
					}

					// Build key values
					const oldKeyValue = indexColumns.length === 1 ? String(oldValues[0]) : JSON.stringify(oldValues);
					const newKeyValue = indexColumns.length === 1 ? String(newVals[0]) : JSON.stringify(newVals);

					// Remove old entry if it had no NULLs
					if (!oldHasNull) {
						await topologyStub.removeShardFromIndexEntry(index.index_name, oldKeyValue, shard.shard_id);
					}

					// Add new entry if it has no NULLs
					if (!newHasNull) {
						await topologyStub.addShardToIndexEntry(index.index_name, newKeyValue, shard.shard_id);
					}
				}
			}
		}
	}

	/**
	 * Maintain indexes after DELETE operation
	 */
	private async maintainIndexesForDelete(
		statement: DeleteStatement,
		indexes: Array<{ index_name: string; columns: string }>,
		shards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
		topologyStub: DurableObjectStub<Topology>,
		tableName: string,
		oldValuesByShard: Map<number, any[]> | null,
	): Promise<void> {
		if (!oldValuesByShard) {
			// No old values fetched (no indexes or no WHERE clause)
			return;
		}

		// For each affected shard, remove entries from indexes using pre-fetched old values
		for (const shard of shards) {
			const oldRows = oldValuesByShard.get(shard.shard_id);
			if (!oldRows || oldRows.length === 0) {
				continue;
			}

			// For each row that was deleted, remove from indexes
			for (const oldRow of oldRows) {
				for (const index of indexes) {
					const indexColumns = JSON.parse(index.columns);

					// Build composite key from old values
					const values: any[] = [];
					let hasNull = false;
					for (const colName of indexColumns) {
						const val = oldRow[colName];
						if (val === null || val === undefined) {
							hasNull = true;
							break;
						}
						values.push(val);
					}

					// Skip if has NULL
					if (hasNull) {
						continue;
					}

					// Build key value
					const keyValue = indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);

					// Remove from index
					await topologyStub.removeShardFromIndexEntry(index.index_name, keyValue, shard.shard_id);
				}
			}
		}
	}

	/**
	 * Enqueue an index job to the queue
	 */
	private async enqueueIndexJob(job: IndexJob): Promise<void> {
		if (!this.indexQueue) {
			console.warn('INDEX_QUEUE not available, skipping index job:', job.type);
			return;
		}

		try {
			await this.indexQueue.send(job);
			console.log(`Enqueued ${job.type} job for ${job.table_name}`);
		} catch (error) {
			console.error(`Failed to enqueue index job:`, error);
			// Don't throw - index operations should not block queries
		}
	}

	/**
	 * Merge results from multiple shards (simple version)
	 */
	private mergeResultsSimple(results: QueryResult[], isSelect: boolean): QueryResult {
		if (isSelect) {
			// Merge rows from all shards
			const mergedRows = results.flatMap((r) => r.rows);
			return {
				rows: mergedRows,
				rowsAffected: mergedRows.length,
			};
		} else {
			// For INSERT/UPDATE/DELETE, sum the rowsAffected
			const totalAffected = results.reduce((sum, r) => sum + (r.rowsAffected || 0), 0);
			return {
				rows: [],
				rowsAffected: totalAffected,
			};
		}
	}
}

/**
 * Create a Conductor client for a specific database
 *
 * @param databaseId - Unique identifier for the database
 * @param env - Worker environment with Durable Object bindings
 * @returns A Conductor client with sql method for executing queries
 *
 * @example
 * const conductor = createConductor('my-database', env);
 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${123}`;
 */
export function createConductor(databaseId: string, env: Env): ConductorClient {
	return new ConductorClient(databaseId, env.STORAGE, env.TOPOLOGY, env.INDEX_QUEUE);
}
