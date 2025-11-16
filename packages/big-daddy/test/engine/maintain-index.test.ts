import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConductor } from '../../src/index';
import { queueHandler } from '../../src/queue-consumer';
import type { IndexMaintenanceJob, MessageBatch } from '../../src/engine/queue/types';

/**
 * Maintain Index Tests
 *
 * These tests verify the maintain-index.ts functionality by:
 * 1. Setting up a table with an index
 * 2. Performing UPDATE/DELETE operations
 * 3. Manually triggering maintenance jobs
 * 4. Verifying the index is updated correctly
 */

async function initializeTopology(dbId: string, numNodes: number) {
	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
	await topologyStub.create(numNodes);
}

async function setupDatabaseWithIndex(dbId: string) {
	const conductor = createConductor(dbId, crypto.randomUUID(), env);

	// Create table
	await conductor.sql`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		category TEXT NOT NULL,
		price REAL NOT NULL
	)`;

	// Insert initial data
	for (let i = 1; i <= 5; i++) {
		await conductor.sql`INSERT INTO products (id, name, category, price) VALUES (
			${i},
			${'Product ' + i},
			${i <= 2 ? 'Electronics' : 'Clothing'},
			${i * 10.0}
		)`;
	}

	// Create an index
	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
	await topologyStub.createVirtualIndex('idx_category', 'products', ['category'], 'hash');

	// Build initial index
	const buildJobResult = await conductor.sql`SELECT DISTINCT category FROM products`;
	const entries = (buildJobResult as any).rows.map((row: any) => ({
		keyValue: row.category,
		shardIds: [0],
	}));

	if (entries.length > 0) {
		await topologyStub.batchUpsertIndexEntries('idx_category', entries);
		await topologyStub.updateIndexStatus('idx_category', 'ready');
	}

	return conductor;
}

async function triggerMaintenanceJob(job: IndexMaintenanceJob): Promise<void> {
	const batch: MessageBatch<IndexMaintenanceJob> = {
		queue: 'vitess-index-jobs',
		messages: [
			{
				id: `test-${Date.now()}-${Math.random()}`,
				timestamp: new Date(),
				body: job,
				attempts: 1,
			},
		],
	};

	await queueHandler(batch as any, env);
}

describe('Maintain Index', () => {
	it('should maintain index after UPDATE operation', async () => {
		const dbId = 'test-maintain-update';
		await initializeTopology(dbId, 1);

		const conductor = await setupDatabaseWithIndex(dbId);
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));

		// Update a product's category
		await conductor.sql`UPDATE products SET category = ${'Home'} WHERE id = ${1}`;

		// Trigger maintenance
		const maintenanceJob: IndexMaintenanceJob = {
			type: 'maintain_index',
			database_id: dbId,
			table_name: 'products',
			operation: 'UPDATE',
			shard_ids: [0],
			affected_indexes: ['idx_category'],
			updated_columns: ['category'],
			created_at: new Date().toISOString(),
		};

		await triggerMaintenanceJob(maintenanceJob);

		// Verify index was updated
		const topology = await topologyStub.getTopology();
		const entries = topology.virtual_index_entries;

		// Should have Electronics (4 items), Clothing (1 item), Home (1 item)
		const categories = entries.map((e) => e.key_value).sort();
		expect(categories).toContain('Clothing');
		expect(categories).toContain('Electronics');
		expect(categories).toContain('Home');
	});

	it('should maintain index after DELETE operation', async () => {
		const dbId = 'test-maintain-delete';
		await initializeTopology(dbId, 1);

		const conductor = await setupDatabaseWithIndex(dbId);
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));

		// Delete a product
		await conductor.sql`DELETE FROM products WHERE id = ${1}`;

		// Trigger maintenance
		const maintenanceJob: IndexMaintenanceJob = {
			type: 'maintain_index',
			database_id: dbId,
			table_name: 'products',
			operation: 'DELETE',
			shard_ids: [0],
			affected_indexes: ['idx_category'],
			created_at: new Date().toISOString(),
		};

		await triggerMaintenanceJob(maintenanceJob);

		// Verify index was maintained
		const topology = await topologyStub.getTopology();
		const entries = topology.virtual_index_entries;

		const categories = entries.map((e) => e.key_value).sort();
		expect(categories).toContain('Clothing');
		expect(categories).toContain('Electronics');
	});

	it('should maintain multiple indexes', async () => {
		const dbId = 'test-maintain-multiple-indexes';
		await initializeTopology(dbId, 1);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		// Create table
		await conductor.sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			amount REAL NOT NULL
		)`;

		// Insert data
		for (let i = 1; i <= 5; i++) {
			await conductor.sql`INSERT INTO orders (id, customer_id, status, amount) VALUES (
				${i},
				${100 + i},
				${i <= 3 ? 'completed' : 'pending'},
				${i * 50.0}
			)`;
		}

		// Create two indexes
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_status', 'orders', ['status'], 'hash');
		await topologyStub.createVirtualIndex('idx_customer', 'orders', ['customer_id'], 'hash');

		// Build indexes
		const statusQuery = await conductor.sql`SELECT DISTINCT status FROM orders`;
		const statusEntries = (statusQuery as any).rows.map((row: any) => ({
			keyValue: String(row.status),
			shardIds: [0],
		}));
		await topologyStub.batchUpsertIndexEntries('idx_status', statusEntries);
		await topologyStub.updateIndexStatus('idx_status', 'ready');

		const customerQuery = await conductor.sql`SELECT DISTINCT customer_id FROM orders`;
		const customerEntries = (customerQuery as any).rows.map((row: any) => ({
			keyValue: String(row.customer_id),
			shardIds: [0],
		}));
		await topologyStub.batchUpsertIndexEntries('idx_customer', customerEntries);
		await topologyStub.updateIndexStatus('idx_customer', 'ready');

		// Update an order's status
		await conductor.sql`UPDATE orders SET status = ${'cancelled'} WHERE id = ${1}`;

		// Trigger maintenance on both indexes
		const maintenanceJob: IndexMaintenanceJob = {
			type: 'maintain_index',
			database_id: dbId,
			table_name: 'orders',
			operation: 'UPDATE',
			shard_ids: [0],
			affected_indexes: ['idx_status', 'idx_customer'],
			updated_columns: ['status'],
			created_at: new Date().toISOString(),
		};

		await triggerMaintenanceJob(maintenanceJob);

		// Verify both indexes were maintained
		const topology = await topologyStub.getTopology();
		const entries = topology.virtual_index_entries;

		const statuses = entries
			.filter((e) => e.index_name === 'idx_status')
			.map((e) => e.key_value)
			.sort();
		expect(statuses).toContain('cancelled');
		expect(statuses).toContain('completed');
		expect(statuses).toContain('pending');

		const customers = entries
			.filter((e) => e.index_name === 'idx_customer')
			.map((e) => e.key_value)
			.sort();
		expect(customers.length).toBe(5);
	});

	it('should handle NULL values correctly during maintenance', async () => {
		const dbId = 'test-maintain-nulls';
		await initializeTopology(dbId, 1);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		// Create table
		await conductor.sql`CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			tags TEXT
		)`;

		// Insert data with some NULLs
		await conductor.sql`INSERT INTO items (id, name, tags) VALUES (${1}, ${'Item 1'}, ${'important'})`;
		await conductor.sql`INSERT INTO items (id, name, tags) VALUES (${2}, ${'Item 2'}, ${null})`;
		await conductor.sql`INSERT INTO items (id, name, tags) VALUES (${3}, ${'Item 3'}, ${'archive'})`;

		// Create index on tags
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_tags', 'items', ['tags'], 'hash');

		// Build initial index (only non-NULL values)
		const tagsQuery = await conductor.sql`SELECT DISTINCT tags FROM items WHERE tags IS NOT NULL`;
		const tagsEntries = (tagsQuery as any).rows.map((row: any) => ({
			keyValue: String(row.tags),
			shardIds: [0],
		}));
		await topologyStub.batchUpsertIndexEntries('idx_tags', tagsEntries);
		await topologyStub.updateIndexStatus('idx_tags', 'ready');

		// Update item 2 to have a tag
		await conductor.sql`UPDATE items SET tags = ${'important'} WHERE id = ${2}`;

		// Trigger maintenance
		const maintenanceJob: IndexMaintenanceJob = {
			type: 'maintain_index',
			database_id: dbId,
			table_name: 'items',
			operation: 'UPDATE',
			shard_ids: [0],
			affected_indexes: ['idx_tags'],
			updated_columns: ['tags'],
			created_at: new Date().toISOString(),
		};

		await triggerMaintenanceJob(maintenanceJob);

		// Verify index was updated correctly (only non-NULL values)
		const topology = await topologyStub.getTopology();
		const entries = topology.virtual_index_entries;

		const tags = entries.map((e) => e.key_value).sort();
		expect(tags).toContain('important');
		expect(tags).toContain('archive');
		expect(tags.length).toBe(2); // No NULL entries
	});

	it('should remove shard from index when value is deleted', async () => {
		const dbId = 'test-maintain-remove-shard';
		await initializeTopology(dbId, 1);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		// Create table
		await conductor.sql`CREATE TABLE tags (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT NOT NULL
		)`;

		// Insert data
		await conductor.sql`INSERT INTO tags (id, name, type) VALUES (${1}, ${'urgent'}, ${'priority'})`;
		await conductor.sql`INSERT INTO tags (id, name, type) VALUES (${2}, ${'low'}, ${'priority'})`;
		await conductor.sql`INSERT INTO tags (id, name, type) VALUES (${3}, ${'red'}, ${'color'})`;

		// Create and build index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_type', 'tags', ['type'], 'hash');

		const typeQuery = await conductor.sql`SELECT DISTINCT type FROM tags`;
		const typeEntries = (typeQuery as any).rows.map((row: any) => ({
			keyValue: String(row.type),
			shardIds: [0],
		}));
		await topologyStub.batchUpsertIndexEntries('idx_type', typeEntries);
		await topologyStub.updateIndexStatus('idx_type', 'ready');

		let topology = await topologyStub.getTopology();
		expect(topology.virtual_index_entries).toHaveLength(2); // color, priority

		// Delete all 'color' entries
		await conductor.sql`DELETE FROM tags WHERE type = ${'color'}`;

		// Trigger maintenance
		const maintenanceJob: IndexMaintenanceJob = {
			type: 'maintain_index',
			database_id: dbId,
			table_name: 'tags',
			operation: 'DELETE',
			shard_ids: [0],
			affected_indexes: ['idx_type'],
			created_at: new Date().toISOString(),
		};

		await triggerMaintenanceJob(maintenanceJob);

		// Verify 'color' entry was removed
		topology = await topologyStub.getTopology();
		const types = topology.virtual_index_entries.map((e) => e.key_value);
		expect(types).toContain('priority');
		expect(types).not.toContain('color');
	});
});
