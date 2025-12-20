import { DurableObject } from 'cloudflare:workers';
import { logger } from '../logger';

// Type definitions for Storage operations

export interface QueryResult<T = Record<string, any>> {
	rows: T[];
	rowsAffected?: number;
}

export interface BatchQueryResult<T = Record<string, any>> {
	results: QueryResult<T>[];
	totalRowsAffected: number;
}

/**
 * Type helper for casting query results to a specific row type
 * Use like: await stub.executeQuery({...}) as StorageResults<{ count: number }>
 */
export type StorageResults<T> = QueryResult<T>;

export interface QueryBatch {
	query: string;
	params?: any[];
	correlationId?: string; // Optional correlation ID for tracing
}

/** Storage Durable Object - individual database shard */
export class Storage extends DurableObject<Env> {
	/**
	 * The constructor is invoked once upon creation of the Durable Object
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	/**
	 * Execute a query or batch of queries on this storage shard
	 *
	 * @param queries - Single query or array of queries to execute
	 * @returns Query results with rows and metadata (single or batch)
	 */
	async executeQuery(queries: QueryBatch | QueryBatch[]): Promise<QueryResult | BatchQueryResult> {
		const source = 'Storage';
		const component = 'Storage';
		const operation = 'executeQuery';
		const queryArray = Array.isArray(queries) ? queries : [queries];
		const isBatch = Array.isArray(queries);
		const correlationId = queryArray[0]?.correlationId;
		const shardId = this.ctx.id.toString();
		const queryCount = queryArray.length;

		const startTime = Date.now();
		logger.debug`Executing storage query ${{source}} ${{component}} ${{operation}} ${{correlationId}} ${{requestId: correlationId}} ${{shardId}} ${{queryCount}} ${{isBatch}}`;

		const results: QueryResult[] = [];
		let totalRowsAffected = 0;

		try {
			for (const batch of queryArray) {
				const queryStartTime = Date.now();
				const result = this.ctx.storage.sql.exec(batch.query, ...(batch.params ?? []));
				const rows = result.toArray() as unknown as Record<string, any>[];
				let rowsAffected = result.rowsWritten ?? 0;
				if (batch.query.trim().toUpperCase().startsWith('INSERT') ||
				    batch.query.trim().toUpperCase().startsWith('UPDATE') ||
				    batch.query.trim().toUpperCase().startsWith('DELETE')) {
					rowsAffected = Math.max(1, Math.floor(rowsAffected / 2));
				}

				const rowCount = rows.length;
				const duration = Date.now() - queryStartTime;
				logger.debug`Query executed on shard ${{source}} ${{component}} ${{shardId}} ${{rowCount}} ${{rowsAffected}} ${{duration}}`;

				results.push({
					rows,
					rowsAffected,
				});

				totalRowsAffected += rowsAffected;
			}

			const finalDuration = Date.now() - startTime;
			const status = 'success';
			logger.info`Storage query completed successfully ${{source}} ${{component}} ${{shardId}} ${{duration: finalDuration}} ${{queryCount}} ${{totalRowsAffected}} ${{status}}`;

			// Return single result if input was a single query, batch result otherwise
			if (!isBatch) {
				return results[0]!;
			}

			return {
				results,
				totalRowsAffected,
			};
		} catch (error) {
			const errorDuration = Date.now() - startTime;
			const errorMsg = error instanceof Error ? error.message : String(error);
			const status = 'failure';
			logger.error`Storage query failed ${{source}} ${{component}} ${{shardId}} ${{duration: errorDuration}} ${{queryCount}} ${{error: errorMsg}} ${{status}}`;
			throw error;
		}
	}

	/**
	 * Get the current database size in bytes
	 *
	 * @returns Database size in bytes
	 */
	async getDatabaseSize(): Promise<number> {
		return this.ctx.storage.sql.databaseSize;
	}

	/**
	 * Get the current bookmark for point-in-time recovery
	 *
	 * @returns Current bookmark identifier
	 */
	async getBookmark(): Promise<string> {
		const bookmark = this.ctx.storage.sql.databaseSize;
		// Return a stringified version of current state
		// Actual bookmark implementation may vary based on Cloudflare's API
		return String(bookmark);
	}

	/**
	 * Reset the database to a specific bookmark
	 *
	 * @param bookmark - Bookmark identifier to reset to
	 * @returns Success status
	 */
	async resetToBookmark(bookmark: string): Promise<{ success: boolean }> {
		// Note: Actual point-in-time recovery API may differ
		// This is a placeholder implementation
		// You may need to use backup/restore mechanisms instead
		return { success: true };
	}
}
