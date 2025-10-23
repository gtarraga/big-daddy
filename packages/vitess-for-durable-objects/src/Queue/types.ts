/**
 * Queue job type definitions for virtual index operations
 */

/**
 * Job to build a virtual index from existing data
 *
 * Note: Index maintenance (INSERT/UPDATE/DELETE) is handled synchronously
 * in the Conductor, not via queue. Only initial index building uses the queue.
 */
export interface IndexBuildJob {
	type: 'build_index';
	database_id: string;
	table_name: string;
	columns: string[]; // Array of column names for composite indexes
	index_name: string;
	created_at: string;
}

/**
 * Union type of all possible index jobs
 * Currently only IndexBuildJob - index updates are synchronous
 */
export type IndexJob = IndexBuildJob;

/**
 * Queue message batch from Cloudflare
 */
export interface MessageBatch<T = IndexJob> {
	queue: string;
	messages: Array<Message<T>>;
}

/**
 * Individual queue message
 */
export interface Message<T = IndexJob> {
	id: string;
	timestamp: Date;
	body: T;
	attempts: number;
}
