import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';

describe('Topology Durable Object', () => {
	describe('create', () => {
		it('should create a topology with specified number of nodes', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-1');
			const stub = env.TOPOLOGY.get(id);

			// Create topology with 3 nodes
			const result = await stub.create(3);
			expect(result.success).toBe(true);

			// Verify nodes were created
			const topology = await stub.getTopology();
			expect(topology.storage_nodes).toHaveLength(3);
			expect(topology.storage_nodes[0].node_id).toBe('node-0');
			expect(topology.storage_nodes[1].node_id).toBe('node-1');
			expect(topology.storage_nodes[2].node_id).toBe('node-2');

			// All nodes should be active
			topology.storage_nodes.forEach((node) => {
				expect(node.status).toBe('active');
				expect(node.capacity_used).toBe(0);
				expect(node.error).toBeNull();
			});
		});

		it('should throw if create is called twice', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-2');
			const stub = env.TOPOLOGY.get(id);

			// First create should succeed
			const result1 = await stub.create(2);
			expect(result1.success).toBe(true);

			// Second create should throw
			await expect(stub.create(2)).rejects.toThrow('Topology already created');
		});

		it('should error if numNodes is less than 1', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-3');
			const stub = env.TOPOLOGY.get(id);

			const result = await stub.create(0);
			expect(result.success).toBe(false);
			expect(result.error).toContain('Number of nodes must be at least 1');
		});
	});

	describe('getTopology', () => {
		it('should throw if topology not created', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-get-1');
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to get topology before creation
			await expect(stub.getTopology()).rejects.toThrow('Topology not created');
		});

		it('should return topology after creation', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-get-2');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			const topology = await stub.getTopology();
			expect(topology).toHaveProperty('storage_nodes');
			expect(topology).toHaveProperty('tables');
			expect(topology).toHaveProperty('table_shards');
			expect(topology.storage_nodes).toHaveLength(2);
			expect(topology.tables).toEqual([]);
			expect(topology.table_shards).toEqual([]);
		});
	});

	describe('updateTopology', () => {
		it('should throw if topology not created', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-1');
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to update topology before creation
			await expect(
				stub.updateTopology({
					tables: {
						add: [
							{
								table_name: 'test',
								primary_key: 'id',
								primary_key_type: 'INTEGER',
								shard_strategy: 'hash',
								shard_key: 'id',
								num_shards: 1,
								block_size: 500,
							},
						],
					},
				})
			).rejects.toThrow('Topology not created');
		});

		it('should add tables', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-2');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(1);
			expect(topology.tables[0].table_name).toBe('users');
		});

		it('should add table shards', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-3');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table first
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'products',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Add shards
			await stub.updateTopology({
				table_shards: {
					add: [
						{
							table_name: 'products',
							shard_id: 0,
							node_id: 'node-0',
							range_start: null,
							range_end: null,
						},
						{
							table_name: 'products',
							shard_id: 1,
							node_id: 'node-1',
							range_start: null,
							range_end: null,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.table_shards).toHaveLength(2);
		});

		it('should update table configuration', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-4');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'items',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Update table
			await stub.updateTopology({
				tables: {
					update: [
						{
							table_name: 'items',
							block_size: 1000,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables[0].block_size).toBe(1000);
		});

		it('should remove tables', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-5');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'temp',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Remove table
			await stub.updateTopology({
				tables: {
					remove: ['temp'],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(0);
		});
	});
});
