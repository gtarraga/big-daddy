import { parse } from '@databases/sqlite-ast';
import type { Statement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement } from '@databases/sqlite-ast';
import type { Storage } from '../Storage/Storage';
import type { Topology } from '../Topology/Topology';

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
	) {}

	/**
	 * Execute a SQL query using tagged template literals
	 *
	 * @example
	 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;
	 */
	sql = async (strings: TemplateStringsArray, ...values: any[]): Promise<QueryResult> => {
		// Build the SQL query and extract parameters
		const { query, params } = this.buildQuery(strings, values);

		// Extract table name using simple regex for MVP
		// This handles: SELECT/INSERT/UPDATE/DELETE ... FROM/INTO table_name
		const tableName = this.extractTableNameSimple(query);

		if (!tableName) {
			throw new Error('Could not determine table name from query');
		}

		// Get topology information
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find the table metadata
		const tableMetadata = topologyData.tables.find((t) => t.table_name === tableName);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		// For MVP: query all shards for SELECT, first shard for everything else
		const isSelect = /^\s*SELECT/i.test(query);
		const shards = topologyData.table_shards.filter((s) => s.table_name === tableName);
		const shardIds = isSelect ? shards.map((s) => s.shard_id) : shards.length > 0 ? [shards[0].shard_id] : [];

		// Execute query on appropriate shard(s)
		const results = await Promise.all(
			shardIds.map(async (shardId) => {
				// Find the node for this shard
				const shard = topologyData.table_shards.find((s) => s.table_name === tableName && s.shard_id === shardId);
				if (!shard) {
					throw new Error(`Shard ${shardId} not found for table '${tableName}'`);
				}

				// Get the storage stub
				const storageId = this.storage.idFromName(shard.node_id);
				const storageStub = this.storage.get(storageId);

				// Execute the query
				return await storageStub.executeQuery(query, params);
			})
		);

		// Merge results if multiple shards are involved
		if (results.length === 1) {
			return results[0];
		} else {
			return this.mergeResultsSimple(results, isSelect);
		}
	};

	/**
	 * Build a parameterized query from template literal parts
	 */
	private buildQuery(strings: TemplateStringsArray, values: any[]): { query: string; params: any[] } {
		let query = '';
		const params: any[] = [];

		for (let i = 0; i < strings.length; i++) {
			query += strings[i];

			if (i < values.length) {
				// Add parameter placeholder
				query += '?';
				params.push(values[i]);
			}
		}

		return { query, params };
	}

	/**
	 * Extract table name using simple regex (MVP approach)
	 * Handles SELECT FROM, INSERT INTO, UPDATE, DELETE FROM
	 */
	private extractTableNameSimple(query: string): string | null {
		// Try to match FROM clause (SELECT, DELETE)
		let match = query.match(/\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
		if (match) return match[1];

		// Try to match INTO clause (INSERT)
		match = query.match(/\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
		if (match) return match[1];

		// Try to match UPDATE clause
		match = query.match(/\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
		if (match) return match[1];

		return null;
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
	return new ConductorClient(databaseId, env.STORAGE, env.TOPOLOGY);
}
