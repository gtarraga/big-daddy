/**
 * Vitess for Durable Objects - Distributed SQL Database on Cloudflare
 *
 * This worker provides an RPC interface for executing SQL queries across
 * a sharded database cluster using Durable Objects for storage and coordination.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your Durable Object in action
 * - Run `npm run deploy` to publish your application
 *
 * Learn more at https://developers.cloudflare.com/durable-objects
 */

import { WorkerEntrypoint } from 'cloudflare:workers';
import { createConductor } from './Conductor/Conductor';
import { queueHandler } from './queue-consumer';
import type { QueryResult } from './Conductor/Conductor';
import type { MessageBatch, IndexJob } from './Queue/types';

// Export Durable Objects
export { Storage } from './Storage/Storage';
export { Topology } from './Topology/Topology';

// Export types
export type { QueryResult } from './Conductor/Conductor';

/**
 * Vitess Worker - RPC-enabled SQL query interface
 *
 * This worker exposes the Conductor's SQL interface via RPC, allowing
 * other workers to execute queries via service bindings.
 *
 * @example Service Binding Usage (from another worker):
 * ```typescript
 * const result = await env.VITESS.sql(['SELECT * FROM users WHERE id = ', ''], [123]);
 * ```
 *
 * @example HTTP API Usage:
 * ```typescript
 * POST /sql
 * {
 *   "database": "my-database",
 *   "query": "SELECT * FROM users WHERE id = ?",
 *   "params": [123]
 * }
 * ```
 */
export default class VitessWorker extends WorkerEntrypoint<Env> {
	/**
	 * RPC method: Execute a SQL query
	 *
	 * This method is callable via service bindings from other workers:
	 * const result = await env.VITESS.sql(strings, values, databaseId);
	 *
	 * @param strings - Template string array (from tagged template literal)
	 * @param values - Parameter values
	 * @param databaseId - Database identifier (defaults to 'default')
	 * @returns Query result with rows and metadata
	 */
	async sql(
		strings: TemplateStringsArray | string[],
		values: any[],
		databaseId: string = 'default'
	): Promise<QueryResult> {
		const conductor = createConductor(databaseId, this.env);

		// Convert to TemplateStringsArray for the conductor
		const templateStrings = strings as any as TemplateStringsArray;
		return await conductor.sql(templateStrings, ...values);
	}

	/**
	 * HTTP fetch handler for REST API access
	 *
	 * Supports POST /sql for executing queries via HTTP
	 */
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		// Handle SQL query endpoint
		if (url.pathname === '/sql' && request.method === 'POST') {
			try {
				const body = await request.json<{
					database?: string;
					query: string;
					params?: any[];
				}>();

				const { database = 'default', query, params = [] } = body;

				// Split query into template strings array and params
				// For now, we'll treat the query string as a single template
				const strings = [query] as any as TemplateStringsArray;
				const result = await this.sql(strings, params, database);

				return new Response(JSON.stringify(result, null, 2), {
					headers: { 'Content-Type': 'application/json' },
				});
			} catch (error) {
				return new Response(
					JSON.stringify({
						error: error instanceof Error ? error.message : String(error),
					}),
					{
						status: 400,
						headers: { 'Content-Type': 'application/json' },
					}
				);
			}
		}

		// Handle health check
		if (url.pathname === '/health') {
			return new Response(JSON.stringify({ status: 'ok' }), {
				headers: { 'Content-Type': 'application/json' },
			});
		}

		// Default response
		return new Response(
			JSON.stringify({
				message: 'Vitess for Durable Objects',
				endpoints: {
					'/sql': 'POST - Execute SQL query',
					'/health': 'GET - Health check',
				},
			}),
			{
				headers: { 'Content-Type': 'application/json' },
			}
		);
	}

	/**
	 * Queue handler for processing virtual index jobs
	 */
	async queue(batch: MessageBatch<unknown>): Promise<void> {
		return queueHandler(batch as MessageBatch<IndexJob>, this.env);
	}
}
