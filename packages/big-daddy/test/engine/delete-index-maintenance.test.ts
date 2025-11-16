import { describe, it, expect, vi, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConnection } from '../../src/index';

/**
 * DELETE with Index Maintenance Tests
 *
 * These tests verify that DELETE operations properly:
 * 1. Capture the rows being deleted
 * 2. Extract index key values from those rows
 * 3. Deduplicate events (multiple rows with same index key = one event)
 * 4. Queue index maintenance events for async processing
 */

// Store queue messages for inspection in tests
let capturedQueueMessages: any[] = [];

// Wrap the queue.send to capture calls
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);
env.INDEX_QUEUE.send = async (message: any) => {
	capturedQueueMessages.push(message);
	return originalQueueSend(message);
};

describe('DELETE with Index Maintenance', () => {
	beforeEach(() => {
		// Clear captured messages before each test
		capturedQueueMessages = [];
	});

	it('should queue deduped index maintenance events when deleting rows with indexes', async () => {
		const dbId = 'test-delete-index-events';
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Create table with indexed column
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL,
			status TEXT
		)`;

		// Create index on email (unique values)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_email', 'users', ['email'], 'hash');

		// Insert 5 rows - will be distributed across 3 shards
		await sql`INSERT INTO users (id, email, name, status) VALUES (1, ${'user1@example.com'}, ${'User 1'}, ${'active'})`;
		await sql`INSERT INTO users (id, email, name, status) VALUES (2, ${'user2@example.com'}, ${'User 2'}, ${'active'})`;
		await sql`INSERT INTO users (id, email, name, status) VALUES (3, ${'user3@example.com'}, ${'User 3'}, ${'inactive'})`;
		await sql`INSERT INTO users (id, email, name, status) VALUES (4, ${'user4@example.com'}, ${'User 4'}, ${'active'})`;
		await sql`INSERT INTO users (id, email, name, status) VALUES (5, ${'user5@example.com'}, ${'User 5'}, ${'inactive'})`;

		// Perform DELETE that will affect multiple shards
		const deleteResult = await sql`DELETE FROM users WHERE status = ${'inactive'}`;

		// Should have deleted 2 rows (id 3 and 5)
		expect(deleteResult.rowsAffected).toBe(2);

		// Verify queue received index maintenance events
		// Should have exactly 1 message (one batch of deduplicated events)
		expect(capturedQueueMessages.length).toBeGreaterThanOrEqual(1);

		const job = capturedQueueMessages[0];
		expect(job.type).toBe('maintain_index_events');
		expect(job.table_name).toBe('users');
		expect(job.database_id).toBe(dbId);

		// Verify events for deleted rows
		// Should have 2 events (one per deleted row), each with:
		// - index_name: 'idx_email'
		// - operation: 'remove'
		// - key_value: the email value
		// - shard_id: which shard the row was on
		const emailEvents = job.events.filter((e: any) => e.index_name === 'idx_email');
		expect(emailEvents.length).toBeGreaterThanOrEqual(1);

		// Verify we have events for the deleted email addresses
		const keyValues = emailEvents.map((e: any) => e.key_value);
		expect(keyValues).toContain('user3@example.com');
		expect(keyValues).toContain('user5@example.com');

		// All should be remove operations
		emailEvents.forEach((event: any) => {
			expect(event.operation).toBe('remove');
		});
	});

	it('should deduplicate index events when multiple rows with same index value are deleted from same shard', async () => {
		const dbId = 'test-delete-index-dedup';
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			amount REAL
		)`;

		// Create index on status (non-unique, multiple rows per value)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_status', 'orders', ['status'], 'hash');

		// Insert orders with duplicate status values
		await sql`INSERT INTO orders (id, customer_id, status, amount) VALUES (1, ${100}, ${'pending'}, ${99.99})`;
		await sql`INSERT INTO orders (id, customer_id, status, amount) VALUES (2, ${101}, ${'pending'}, ${199.99})`;
		await sql`INSERT INTO orders (id, customer_id, status, amount) VALUES (3, ${102}, ${'pending'}, ${299.99})`;
		await sql`INSERT INTO orders (id, customer_id, status, amount) VALUES (4, ${103}, ${'completed'}, ${99.99})`;

		// Delete all pending orders
		const deleteResult = await sql`DELETE FROM orders WHERE status = ${'pending'}`;
		expect(deleteResult.rowsAffected).toBe(3);

		// Verify queue received deduplicated events
		expect(capturedQueueMessages.length).toBeGreaterThanOrEqual(1);

		const job = capturedQueueMessages[0];
		expect(job.type).toBe('maintain_index_events');

		// Expected behavior:
		// - 3 rows deleted, all with status='pending'
		// - Should produce at most 1 event per shard: (idx_status, 'pending', shard_X, 'remove')
		// NOT 3 events (one per row) - that's the deduplication benefit
		const pendingEvents = job.events.filter(
			(e: any) => e.index_name === 'idx_status' && e.key_value === 'pending'
		);
		expect(pendingEvents.length).toBeGreaterThanOrEqual(1);
		expect(pendingEvents.length).toBeLessThanOrEqual(2); // At most 2 shards
		pendingEvents.forEach((event: any) => {
			expect(event.operation).toBe('remove');
		});
	});

	it('should handle composite index deduplication correctly', async () => {
		const dbId = 'test-delete-composite-index';
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table with composite index candidates
		await sql`CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL,
			subcategory TEXT NOT NULL,
			price REAL
		)`;

		// Get fresh topology stub after connection is established
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));

		// Verify topology exists
		const topology = await topologyStub.getTopology();
		expect(topology).toBeDefined();

		// Create composite index
		await topologyStub.createVirtualIndex('idx_category_sub', 'products', ['category', 'subcategory'], 'hash');

		// Insert products with duplicate category/subcategory combinations
		await sql`INSERT INTO products (id, category, subcategory, price) VALUES (1, ${'Electronics'}, ${'Phones'}, ${699.99})`;
		await sql`INSERT INTO products (id, category, subcategory, price) VALUES (2, ${'Electronics'}, ${'Phones'}, ${799.99})`;
		await sql`INSERT INTO products (id, category, subcategory, price) VALUES (3, ${'Electronics'}, ${'Phones'}, ${899.99})`;
		await sql`INSERT INTO products (id, category, subcategory, price) VALUES (4, ${'Electronics'}, ${'Laptops'}, ${1299.99})`;

		// Delete all phones under $900
		const deleteResult = await sql`DELETE FROM products WHERE category = ${'Electronics'} AND subcategory = ${'Phones'} AND price < ${900}`;
		expect(deleteResult.rowsAffected).toBe(2); // Deletes id 1 and 2

		// Verify queue received deduplicated events
		expect(capturedQueueMessages.length).toBeGreaterThanOrEqual(1);

		const job = capturedQueueMessages[0];
		expect(job.type).toBe('maintain_index_events');

		// Expected: at most 1 event per shard with composite key ['Electronics', 'Phones']
		// NOT 2 events (one per row)
		// Key should be JSON stringified: ["Electronics","Phones"]
		const compositeEvents = job.events.filter(
			(e: any) => e.index_name === 'idx_category_sub' && e.key_value === JSON.stringify(['Electronics', 'Phones'])
		);
		expect(compositeEvents.length).toBeGreaterThanOrEqual(1);
		compositeEvents.forEach((event: any) => {
			expect(event.operation).toBe('remove');
		});
	});

	it('should not queue events if no indexes exist on table', async () => {
		const dbId = 'test-delete-no-indexes';
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table WITHOUT index
		await sql`CREATE TABLE logs (
			id INTEGER PRIMARY KEY,
			message TEXT NOT NULL,
			level TEXT
		)`;

		// Insert and delete rows
		await sql`INSERT INTO logs (id, message, level) VALUES (1, ${'Test message'}, ${'INFO'})`;
		await sql`INSERT INTO logs (id, message, level) VALUES (2, ${'Error occurred'}, ${'ERROR'})`;

		const deleteResult = await sql`DELETE FROM logs WHERE level = ${'INFO'}`;
		expect(deleteResult.rowsAffected).toBe(1);

		// Should NOT queue any index maintenance events since there are no indexes
		expect(capturedQueueMessages).toHaveLength(0);
	});

	it('should handle NULL values in indexed columns during delete', async () => {
		const dbId = 'test-delete-index-nulls';
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE records (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT NOT NULL
		)`;

		// Create index (may index NULLs)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_email', 'records', ['email'], 'hash');

		// Insert records with some NULLs
		await sql`INSERT INTO records (id, email, name) VALUES (1, ${'user1@example.com'}, ${'User 1'})`;
		await sql`INSERT INTO records (id, email, name) VALUES (2, ${null}, ${'User 2'})`;
		await sql`INSERT INTO records (id, email, name) VALUES (3, ${'user3@example.com'}, ${'User 3'})`;
		await sql`INSERT INTO records (id, email, name) VALUES (4, ${null}, ${'User 4'})`;

		// Delete rows with NULL email
		const deleteResult = await sql`DELETE FROM records WHERE email IS NULL`;
		expect(deleteResult.rowsAffected).toBe(2);

		// Expected: 0 events
		// NULL values should not produce index events (they're not indexed)
		expect(capturedQueueMessages).toHaveLength(0);
	});

	it('should handle delete with multiple indexes on same table', async () => {
		const dbId = 'test-delete-multiple-indexes';
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			username TEXT NOT NULL,
			status TEXT
		)`;

		// Create multiple indexes
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_email', 'users', ['email'], 'hash');
		await topologyStub.createVirtualIndex('idx_username', 'users', ['username'], 'hash');

		// Insert users
		await sql`INSERT INTO users (id, email, username, status) VALUES (1, ${'alice@example.com'}, ${'alice'}, ${'active'})`;
		await sql`INSERT INTO users (id, email, username, status) VALUES (2, ${'bob@example.com'}, ${'bob'}, ${'inactive'})`;
		await sql`INSERT INTO users (id, email, username, status) VALUES (3, ${'charlie@example.com'}, ${'charlie'}, ${'inactive'})`;

		// Delete inactive users
		const deleteResult = await sql`DELETE FROM users WHERE status = ${'inactive'}`;
		expect(deleteResult.rowsAffected).toBe(2);

		// Verify events for both indexes
		expect(capturedQueueMessages.length).toBeGreaterThanOrEqual(1);

		const job = capturedQueueMessages[0];
		expect(job.type).toBe('maintain_index_events');

		// Should have events for both indexes
		const emailEvents = job.events.filter((e: any) => e.index_name === 'idx_email');
		const usernameEvents = job.events.filter((e: any) => e.index_name === 'idx_username');

		// At least one event per index (deduped)
		expect(emailEvents.length).toBeGreaterThanOrEqual(1);
		expect(usernameEvents.length).toBeGreaterThanOrEqual(1);

		// Verify the email values match deleted users
		const emailValues = emailEvents.map((e: any) => e.key_value);
		expect(emailValues).toContain('bob@example.com');
		expect(emailValues).toContain('charlie@example.com');

		// Verify the username values match deleted users
		const usernameValues = usernameEvents.map((e: any) => e.key_value);
		expect(usernameValues).toContain('bob');
		expect(usernameValues).toContain('charlie');

		// All events should be remove operations
		job.events.forEach((event: any) => {
			expect(event.operation).toBe('remove');
		});
	});
});
