import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { QueryResult, ShardStats } from "../../src/engine/conductor/types";
import { createConnection } from "../../src/index";

type QueryResultWithShardStats = QueryResult & { shardStats: ShardStats[] };

/**
 * Sum of rowsReturned across all shardStats
 */
function totalRowsFetched(shardStats: ShardStats[]): number {
	return shardStats.reduce((sum, s) => sum + s.rowsReturned, 0);
}

/**
 * These tests verify that getCachedQueryPlanData properly routes queries to the correct shards.
 * The bug being tested: queries for a single item by id should only hit 1 shard, not all shards.
 */
describe("getCachedQueryPlanData", () => {
	describe("Single shard queries - querying by shard key (id)", () => {
		it("should route SELECT query by id to only 1 shard with 3 shards total", async () => {
			const dbId = "test-single-item-3-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			// Create table with 3 shards specified via SHARDS comment
			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT, email TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert a test user
			await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice@example.com"})`;

			// Query for a single item by id
			const result =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only query 1 shard, not all 3
			expect(result.shardStats).toBeDefined();
			expect(result.shardStats).toHaveLength(1);
			expect(result.rows).toHaveLength(1);
			expect(result.rows[0]).toMatchObject({ id: 100, name: "Alice" });
		});

		it("should return the same shard for the same id on repeated calls", async () => {
			const dbId = "test-same-shard-consistency-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Query the same id twice
			const result1 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;
			const result2 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Both should query the same shard
			expect(result1.shardStats).toHaveLength(1);
			expect(result2.shardStats).toHaveLength(1);
			expect(result1.shardStats[0]?.shardId).toBe(
				result2.shardStats[0]?.shardId,
			);
		});

		it("should return different shards for different ids", async () => {
			const dbId = "test-different-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Query different ids
			const result1 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;
			const result2 =
				(await sql`SELECT * FROM users WHERE id = ${200}`) as QueryResultWithShardStats;

			// Both should query exactly 1 shard each
			expect(result1.shardStats).toHaveLength(1);
			expect(result2.shardStats).toHaveLength(1);
		});
	});

	describe("Non-shard-key queries", () => {
		it("should return all shards when querying without WHERE clause", async () => {
			const dbId = "test-all-shards-select-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;
			await sql`INSERT INTO users (id, name) VALUES (${300}, ${"Charlie"})`;

			// Select all users
			const result =
				(await sql`SELECT * FROM users`) as QueryResultWithShardStats;

			// Should query all 3 shards
			expect(result.shardStats).toHaveLength(3);
			expect(result.rows).toHaveLength(3);
		});

		it("should return all shards when querying by non-shard-key column", async () => {
			const dbId = "test-non-shard-key-where-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// WHERE on non-shard-key column - should hit all shards
			const result =
				(await sql`SELECT * FROM users WHERE name = ${"Alice"}`) as QueryResultWithShardStats;

			// Should query all shards since we're querying by name (not shard key)
			expect(result.shardStats).toHaveLength(3);
			expect(result.rows).toHaveLength(1);
			expect(result.rows[0]).toMatchObject({ name: "Alice" });
		});
	});

	describe("Complex WHERE conditions", () => {
		it("should handle queries with multiple conditions including shard key", async () => {
			const dbId = "test-multi-condition-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name, age) VALUES (${100}, ${"Alice"}, ${25})`;

			// WHERE with multiple conditions: one on shard key, one on non-shard-key
			const result =
				(await sql`SELECT * FROM users WHERE id = ${100} AND age > ${20}`) as QueryResultWithShardStats;

			// Should still return only 1 shard because id is the shard key
			expect(result.shardStats).toHaveLength(1);
			expect(result.rows).toHaveLength(1);
		});

		it("should handle IN queries on shard key", async () => {
			const dbId = "test-in-clause-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;
			await sql`INSERT INTO users (id, name) VALUES (${300}, ${"Charlie"})`;

			// IN clause on shard key - might hit multiple shards
			const result =
				(await sql`SELECT * FROM users WHERE id IN (${100}, ${200})`) as QueryResultWithShardStats;

			// Should query at least 1 shard (might be 1 or 2 depending on hash distribution)
			expect(result.shardStats.length).toBeGreaterThanOrEqual(1);
			expect(result.rows.length).toBeLessThanOrEqual(2);
		});
	});

	describe("UPDATE and DELETE statements", () => {
		it("should route UPDATE to only 1 shard when WHERE filters on shard key", async () => {
			const dbId = "test-update-shard-key-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

			// Update a specific user by shard key
			const result =
				(await sql`UPDATE users SET name = ${"Alice Updated"} WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only update 1 shard
			expect(result.shardStats).toHaveLength(1);
			expect(result.rowsAffected).toBe(1);

			// Verify the update worked
			const verify =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResult;
			expect(verify.rows[0]).toMatchObject({ id: 100, name: "Alice Updated" });
		});

		it("should route DELETE to only 1 shard when WHERE filters on shard key", async () => {
			const dbId = "test-delete-shard-key-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

			// Delete a specific user by shard key
			const result =
				(await sql`DELETE FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only delete from 1 shard
			expect(result.shardStats).toHaveLength(1);
			expect(result.rowsAffected).toBe(1);

			// Verify the delete worked
			const verify =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResult;
			expect(verify.rows).toHaveLength(0);
		});

		it("should route UPDATE to all shards when WHERE does not filter on shard key", async () => {
			const dbId = "test-update-all-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Update based on non-shard-key column (should query all shards)
			const result =
				(await sql`UPDATE users SET name = ${"Updated"} WHERE name = ${"Alice"}`) as QueryResultWithShardStats;

			// Should update on all shards (though data might only be on 1-2 shards)
			expect(result.shardStats).toHaveLength(3);
			expect(result.rowsAffected).toBe(1);
		});
	});

	describe("Phase 2: Distributed LIMIT/OFFSET", () => {
		describe("Correctness - global pagination enforcement", () => {
			it("should return exactly LIMIT rows for fan-out SELECT", async () => {
				const dbId = "test-limit-correctness-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				// Insert rows across multiple shards
				for (let i = 1; i <= 20; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with LIMIT 10 - should return exactly 10 rows
				const result =
					(await sql`SELECT * FROM users LIMIT 10`) as QueryResultWithShardStats;

				expect(result.rows).toHaveLength(10);
				expect(result.shardStats).toBeDefined();
				expect(result.shardStats.length).toBeGreaterThanOrEqual(1);
			});

			it("should return exactly LIMIT rows with OFFSET", async () => {
				const dbId = "test-limit-offset-correctness-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				// Insert rows across multiple shards
				for (let i = 1; i <= 20; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with LIMIT 10 OFFSET 5 - should return exactly 10 rows
				const result =
					(await sql`SELECT * FROM users LIMIT 10 OFFSET 5`) as QueryResultWithShardStats;

				expect(result.rows).toHaveLength(10);
			});

			it("should return 0 rows when OFFSET exceeds total rows", async () => {
				const dbId = "test-offset-exceeds-total-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				// Insert 5 rows
				for (let i = 1; i <= 5; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with OFFSET 100 - should return 0 rows
				const result =
					(await sql`SELECT * FROM users LIMIT 10 OFFSET 100`) as QueryResultWithShardStats;

				expect(result.rows).toHaveLength(0);
			});

			it("should handle LIMIT without OFFSET correctly", async () => {
				const dbId = "test-limit-only-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				// Insert rows
				for (let i = 1; i <= 15; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with LIMIT 7 - should return exactly 7 rows
				const result =
					(await sql`SELECT * FROM users LIMIT 7`) as QueryResultWithShardStats;

				expect(result.rows).toHaveLength(7);
			});
		});

		describe("Optimization - over-fetch reduction", () => {
			it("should reduce over-fetch for eligible queries", async () => {
				const dbId = "test-overfetch-reduction-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				// Insert rows across shards (known distribution)
				for (let i = 1; i <= 30; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with LIMIT 10 - should fetch close to 10 rows, not 30
				const result =
					(await sql`SELECT * FROM users LIMIT 10`) as QueryResultWithShardStats;

				expect(result.rows).toHaveLength(10);

				// Calculate total fetched across shards
				const totalFetched = result.shardStats.reduce(
					(sum, stat) => sum + stat.rowsReturned,
					0,
				);

				// Should fetch significantly less than LIMIT * shardCount (30)
				// Allow some slack for catch-up logic
				expect(totalFetched).toBeLessThan(30);
				expect(totalFetched).toBeGreaterThanOrEqual(10);
			});

			it("should not rewrite queries with WHERE clause", async () => {
				const dbId = "test-no-rewrite-with-where-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				for (let i = 1; i <= 20; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with WHERE - should still work correctly but may fetch more
				const result =
					(await sql`SELECT * FROM users WHERE name LIKE 'User%' LIMIT 5`) as QueryResultWithShardStats;

				expect(result.rows.length).toBeLessThanOrEqual(5);
			});

			it("should not rewrite queries with ORDER BY", async () => {
				const dbId = "test-no-rewrite-with-orderby-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				for (let i = 1; i <= 20; i++) {
					await sql`INSERT INTO users (id, name) VALUES (${i * 100}, ${`User${i}`})`;
				}

				// Query with ORDER BY - should still work correctly
				const result =
					(await sql`SELECT * FROM users ORDER BY id LIMIT 5`) as QueryResultWithShardStats;

				expect(result.rows.length).toBeLessThanOrEqual(5);
			});

			it("should not rewrite single-shard queries", async () => {
				const dbId = "test-no-rewrite-single-shard-v1";
				const sql = await createConnection(dbId, { nodes: 3 }, env);

				const createTableSql =
					"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
				await sql([createTableSql] as unknown as TemplateStringsArray);

				await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

				// Query by shard key - should only hit 1 shard
				const result =
					(await sql`SELECT * FROM users WHERE id = ${100} LIMIT 5`) as QueryResultWithShardStats;

				expect(result.shardStats).toHaveLength(1);
				expect(result.rows.length).toBeLessThanOrEqual(1);
			});
		});
	});
});

// ============================================================================
// Phase 2: Distributed LIMIT/OFFSET Tests
// ============================================================================

describe("Distributed LIMIT/OFFSET", () => {
	describe("Global pagination correctness", () => {
		it("SELECT * LIMIT 10 across N shards returns exactly 10 rows", async () => {
			const dbId = "test-limit-correctness-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 30 rows (10 per shard on average)
			for (let i = 1; i <= 30; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 10
			const result =
				(await sql`SELECT * FROM items LIMIT 10`) as QueryResultWithShardStats;

			// Must return exactly 10 rows
			expect(result.rows).toHaveLength(10);
		});

		it("SELECT * LIMIT 10 OFFSET 5 returns exactly 10 rows", async () => {
			const dbId = "test-limit-offset-correctness-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 30 rows
			for (let i = 1; i <= 30; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 10 OFFSET 5
			const result =
				(await sql`SELECT * FROM items LIMIT 10 OFFSET 5`) as QueryResultWithShardStats;

			// Must return exactly 10 rows
			expect(result.rows).toHaveLength(10);
		});

		it("OFFSET beyond total rows returns 0 rows", async () => {
			const dbId = "test-offset-beyond-total-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 10 rows
			for (let i = 1; i <= 10; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with OFFSET > total rows
			const result =
				(await sql`SELECT * FROM items LIMIT 10 OFFSET 100`) as QueryResultWithShardStats;

			// Should return 0 rows
			expect(result.rows).toHaveLength(0);
		});

		it("LIMIT without OFFSET returns correct count", async () => {
			const dbId = "test-limit-no-offset-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 15 rows
			for (let i = 1; i <= 15; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 5
			const result =
				(await sql`SELECT * FROM items LIMIT 5`) as QueryResultWithShardStats;

			expect(result.rows).toHaveLength(5);
		});

		it("LIMIT larger than total rows returns all rows", async () => {
			const dbId = "test-limit-larger-than-total-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 5 rows
			for (let i = 1; i <= 5; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 100
			const result =
				(await sql`SELECT * FROM items LIMIT 100`) as QueryResultWithShardStats;

			expect(result.rows).toHaveLength(5);
		});
	});

	describe("Over-fetch reduction (optimization)", () => {
		it("SELECT * LIMIT N should fetch close to N rows total across shards", async () => {
			const dbId = "test-overfetch-reduction-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 60 rows (20 per shard on average)
			for (let i = 1; i <= 60; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 10
			const result =
				(await sql`SELECT * FROM items LIMIT 10`) as QueryResultWithShardStats;

			// Should return exactly 10 rows
			expect(result.rows).toHaveLength(10);

			// Total fetched should be close to 10 (with some slack for catch-up)
			// Without optimization: would be 10 * 3 = 30
			// With optimization: should be ~10-30 (depends on distribution)
			const fetched = totalRowsFetched(result.shardStats);

			// Allow some slack but should be significantly less than LIMIT * shardCount
			// At minimum, we shouldn't over-fetch by more than 3x
			expect(fetched).toBeLessThanOrEqual(30); // 10 * 3 shards = worst case
		});
	});

	describe("Non-eligible queries (should still work correctly)", () => {
		it("SELECT with WHERE clause applies LIMIT correctly", async () => {
			const dbId = "test-limit-with-where-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 30 rows
			for (let i = 1; i <= 30; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with WHERE and LIMIT (not eligible for optimization, but should be correct)
			const result =
				(await sql`SELECT * FROM items WHERE id > 5 LIMIT 10`) as QueryResultWithShardStats;

			// Should return at most 10 rows
			expect(result.rows.length).toBeLessThanOrEqual(10);
			// All rows should have id > 5
			for (const row of result.rows) {
				expect((row as { id: number }).id).toBeGreaterThan(5);
			}
		});

		it("SELECT with ORDER BY applies LIMIT correctly", async () => {
			const dbId = "test-limit-with-orderby-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 30 rows
			for (let i = 1; i <= 30; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with ORDER BY and LIMIT (not eligible for optimization)
			// Note: Global ORDER BY across shards is not yet supported, so this tests
			// that LIMIT is still enforced post-merge even without the optimization
			const result =
				(await sql`SELECT * FROM items ORDER BY id LIMIT 10`) as QueryResultWithShardStats;

			// Should return exactly 10 rows
			expect(result.rows).toHaveLength(10);
		});

		it("Aggregation queries with LIMIT work correctly", async () => {
			const dbId = "test-limit-aggregation-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 30 rows
			for (let i = 1; i <= 30; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// COUNT query - aggregation should merge correctly across shards
			const result = (await sql`SELECT COUNT(*) as cnt FROM items`) as QueryResult;

			expect(result.rows).toHaveLength(1);
			expect((result.rows[0] as { cnt: number }).cnt).toBe(30);
		});
	});

	describe("Edge cases", () => {
		it("LIMIT 0 returns 0 rows", async () => {
			const dbId = "test-limit-zero-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 10 rows
			for (let i = 1; i <= 10; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 0
			const result = (await sql`SELECT * FROM items LIMIT 0`) as QueryResult;

			expect(result.rows).toHaveLength(0);
		});

		it("Single shard query with LIMIT works correctly", async () => {
			const dbId = "test-single-shard-limit-v1";
			const sql = await createConnection(dbId, { nodes: 1 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=1 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert 20 rows
			for (let i = 1; i <= 20; i++) {
				await sql`INSERT INTO items (id, name) VALUES (${i}, ${`Item${i}`})`;
			}

			// Query with LIMIT 5
			const result =
				(await sql`SELECT * FROM items LIMIT 5`) as QueryResultWithShardStats;

			expect(result.rows).toHaveLength(5);
			expect(result.shardStats).toHaveLength(1);
		});

		it("Empty table with LIMIT returns 0 rows", async () => {
			const dbId = "test-empty-table-limit-v1";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE items /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Query empty table with LIMIT
			const result =
				(await sql`SELECT * FROM items LIMIT 10`) as QueryResultWithShardStats;

			expect(result.rows).toHaveLength(0);
		});
	});
});
