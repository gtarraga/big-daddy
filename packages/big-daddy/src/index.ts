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
import { withLogTags } from 'workers-tagged-logger';
import { logger } from './logger';
import { createConductor } from './engine/conductor';
import { queueHandler } from './queue-consumer';
import type { QueryResult } from './engine/conductor';
import type { MessageBatch, IndexJob } from './engine/queue/types';
import { Topology } from './engine/topology';
import { Storage } from './engine/storage';
// Export Durable Objects
export { Storage, Topology };

// Export conductor creation functions
export { createConductor } from './engine/conductor';

// Export types
export type { QueryResult, ConductorAPI } from './engine/conductor';
export type { CacheStats } from './engine/utils/topology-cache';

/**
 * Configuration options for createConnection
 */
export interface ConnectionConfig {
	/** Number of storage nodes to create for this database */
	nodes: number;
	/** Optional correlation ID for request tracking */
	correlationId?: string;
}

/**
 * SQL tagged template literal function
 */
export interface SqlFunction {
	(strings: TemplateStringsArray, ...values: any[]): Promise<QueryResult>;
}

/**
 * Environment interface with required Big Daddy bindings
 */
export interface BigDaddyEnv {
	STORAGE: DurableObjectNamespace<Storage>;
	TOPOLOGY: DurableObjectNamespace<Topology>;
	INDEX_QUEUE: Queue;
}

/**
 * Create a connection to a database with automatic topology initialization
 *
 * This function:
 * 1. Checks if topology exists in the cache
 * 2. Checks if topology exists in the Topology DO
 * 3. Creates a new topology if it doesn't exist
 * 4. Returns a sql tagged template literal function ready to use
 *
 * @param databaseId - Unique identifier for the database
 * @param config - Configuration options (nodes, correlationId)
 * @param env - Worker environment with Durable Object bindings
 * @returns SQL tagged template literal function
 *
 * @example
 * ```typescript
 * const sql = await createConnection('my-database', { nodes: 10 }, env);
 * const result = await sql`SELECT * FROM users WHERE id = ${123}`;
 * ```
 */
export async function createConnection(databaseId: string, config: ConnectionConfig, env: BigDaddyEnv): Promise<SqlFunction> {
	return withLogTags({ source: 'createConnection' }, async () => {
		const cid = config.correlationId || crypto.randomUUID();

		logger.setTags({
			correlationId: cid,
			requestId: cid,
			databaseId,
			component: 'createConnection',
		});

		logger.debug('Creating connection', {
			nodes: config.nodes,
		});

		// Get the Topology DO stub
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(topologyId);

		// Try to check if topology exists by calling getTopology
		// If it throws an error about topology not being created, we'll create it
		try {
			const topologyData = await topologyStub.getTopology();

			logger.debug('Topology already exists', {
				nodes: topologyData.storage_nodes.length,
			});
		} catch (error) {
			// Check if the error is about topology not being created
			if (error instanceof Error && error.message.includes('Topology not created')) {
				logger.info('Topology does not exist, creating', {
					nodes: config.nodes,
				});

				// Create topology with the specified number of nodes
				const result = await topologyStub.create(config.nodes);

				if (!result.success) {
					throw new Error(`Failed to create topology: ${result.error}`);
				}

				logger.info('Topology created successfully', {
					nodes: config.nodes,
				});
			} else {
				// Re-throw if it's a different error
				throw error;
			}
		}

		// TODO support passing correlationID here
		const client = createConductor(databaseId, cid, env);

		// Return the sql function bound with the correlation ID
		const sql: SqlFunction = async (strings: TemplateStringsArray, ...values: any[]) => {
			return client.sql(strings, cid, ...values);
		};

		return sql;
	});
}

/**
 * Big Daddy Worker - RPC-enabled distributed SQL database
 *
 * This worker exposes the createConnection API via RPC, allowing
 * other workers to create database connections via service bindings.
 *
 * @example Service Binding Usage (from another worker):
 * ```typescript
 * const sql = await env.BIG_DADDY.createConnection('my-database', { nodes: 8 });
 * const result = await sql`SELECT * FROM users WHERE id = ${123}`;
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
export default class BigDaddy extends WorkerEntrypoint<Env> {
	/**
	 * RPC method: Create a connection to a database
	 *
	 * This method is callable via service bindings from other workers:
	 * const sql = await env.BIG_DADDY.createConnection('my-database', { nodes: 8 });
	 *
	 * @param databaseId - Unique identifier for the database
	 * @param config - Configuration options (nodes, correlationId)
	 * @returns SQL tagged template literal function
	 */
	async createConnection(databaseId: string, config: ConnectionConfig): Promise<SqlFunction> {
		return createConnection(databaseId, config, this.env);
	}

	/**
	 * HTTP fetch handler for REST API access
	 *
	 * Supports POST /sql for executing queries via HTTP
	 */
	override async fetch(request: Request): Promise<Response> {
		return withLogTags({ source: 'BigDaddy' }, async () => {
			const url = new URL(request.url);

			// Generate or extract correlation ID from request headers
			const correlationId = request.headers.get('x-correlation-id') || request.headers.get('cf-ray') || crypto.randomUUID();

			logger.setTags({
				correlationId,
				requestId: correlationId,
				component: 'BigDaddy',
				operation: 'fetch',
			});

			logger.debug('HTTP request received', {
				method: request.method,
				path: url.pathname,
			});

			// Handle SQL query endpoint
			if (url.pathname === '/sql' && request.method === 'POST') {
				try {
					const body = await request.json<{
						database?: string;
						query: string;
						params?: any[];
					}>();

					const { database = 'default', query, params = [] } = body;

					// Create connection and execute query
					const sql = await this.createConnection(database, { nodes: 8, correlationId });

					// Parse query to build template strings
					const strings = [query] as any as TemplateStringsArray;
					const conductor = createConductor(database, this.env);
					const result = await conductor.sql(strings, ...params);

					return new Response(JSON.stringify(result, null, 2), {
						headers: { 'Content-Type': 'application/json' },
					});
				} catch (error) {
					logger.error('SQL query request failed', {
						error: error instanceof Error ? error.message : String(error),
						status: 'failure',
					});
					return new Response(
						JSON.stringify({
							error: error instanceof Error ? error.message : String(error),
						}),
						{
							status: 400,
							headers: { 'Content-Type': 'application/json' },
						},
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
					message: 'Big Daddy - Distributed SQL on Cloudflare',
					endpoints: {
						'/sql': 'POST - Execute SQL query',
						'/health': 'GET - Health check',
					},
				}),
				{
					headers: { 'Content-Type': 'application/json' },
				},
			);
		});
	}

	/**
	 * Queue handler for processing virtual index jobs
	 */
	override async queue(batch: MessageBatch<unknown>): Promise<void> {
		return withLogTags({ source: 'BigDaddy' }, async () => {
			const correlationId = crypto.randomUUID();
			logger.setTags({
				correlationId,
				requestId: correlationId,
				component: 'BigDaddy',
				operation: 'queue',
			});

			logger.info('Processing queue batch', {
				batchSize: batch.messages.length,
			});

			try {
				await queueHandler(batch as MessageBatch<IndexJob>, this.env, correlationId);
				logger.info('Queue batch processed successfully', {
					batchSize: batch.messages.length,
					status: 'success',
				});
			} catch (error) {
				logger.error('Queue batch processing failed', {
					batchSize: batch.messages.length,
					status: 'failure',
					error: error instanceof Error ? error.message : String(error),
				});
				throw error;
			}
		});
	}
}
