import type { Literal, SelectStatement } from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import type { ShardRowCount } from "../../topology/types";
import { extractTableName as extractTableNameFromAST } from "../../utils/ast-utils";
import type {
	QueryHandlerContext,
	QueryResult,
	ShardInfo,
	ShardStats,
	SqlParam,
} from "../types";
import { mergeResultsSimple } from "../utils";
import { executeOnShards, getCachedQueryPlanData } from "../utils/write";

// ============================================================================
// LIMIT/OFFSET Extraction Helpers
// ============================================================================

/**
 * Extract numeric value from LIMIT/OFFSET expression
 * Only supports Literal expressions; Placeholders require param remapping
 *
 * @returns numeric value or null if not a literal
 */
function extractLiteralNumber(expr: SelectStatement["limit"]): number | null {
	if (!expr) return null;
	if (expr.type === "Literal") {
		const val = (expr as Literal).value;
		if (typeof val === "number") return val;
		if (typeof val === "string") {
			const parsed = Number.parseInt(val, 10);
			return Number.isNaN(parsed) ? null : parsed;
		}
	}
	return null;
}

/**
 * Check if a SELECT is eligible for per-shard LIMIT/OFFSET rewrite optimization
 *
 * Eligibility criteria (conservative):
 * - Fan-out query (multiple shards)
 * - No ORDER BY (global ordering requires distributed top-K)
 * - No WHERE clause (row counts are for entire shard)
 * - No aggregation functions (COUNT, SUM, etc. have special merge logic)
 * - LIMIT is a literal number
 * - OFFSET is absent or a literal number
 */
function isEligibleForLimitRewrite(
	statement: SelectStatement,
	shardCount: number,
): {
	eligible: boolean;
	globalLimit: number | null;
	globalOffset: number;
} {
	// Must be a fan-out query
	if (shardCount <= 1) {
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	// No ORDER BY
	if (statement.orderBy && statement.orderBy.length > 0) {
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	// No WHERE clause
	if (statement.where) {
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	// No aggregation functions in select
	const hasAggregation = statement.select?.some((col) => {
		const expr = col.expression;
		if (expr?.type === "FunctionCall") {
			const funcName = ((expr as { name?: string }).name || "").toUpperCase();
			return ["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(funcName);
		}
		return false;
	});
	if (hasAggregation) {
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	// LIMIT must be present and literal
	const globalLimit = extractLiteralNumber(statement.limit);
	if (globalLimit === null) {
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	// OFFSET must be absent or literal
	const globalOffset = extractLiteralNumber(statement.offset) ?? 0;
	if (
		statement.offset &&
		globalOffset === 0 &&
		statement.offset.type !== "Literal"
	) {
		// Non-literal offset present
		return { eligible: false, globalLimit: null, globalOffset: 0 };
	}

	return { eligible: true, globalLimit, globalOffset };
}

// ============================================================================
// Per-Shard LIMIT/OFFSET Planning
// ============================================================================

interface ShardLimitPlan {
	shard: ShardInfo;
	skip: number; // per-shard OFFSET
	take: number; // per-shard LIMIT
}

/**
 * Compute per-shard OFFSET/LIMIT using prefix sums of row counts
 *
 * Given global OFFSET=O and LIMIT=L, and shards with row counts C_0..C_{n-1}:
 * - prefixBefore_i = sum(C_0..C_{i-1})
 * - skip_i = max(0, O - prefixBefore_i)
 * - available_i = max(0, C_i - skip_i)
 * - take_i = min(available_i, remaining) where remaining starts at L
 *
 * @param shardsToQuery - Shards to query (will be sorted by shard_id)
 * @param rowCounts - Per-shard row counts from Topology
 * @param globalOffset - Global OFFSET value
 * @param globalLimit - Global LIMIT value
 * @returns Array of shard plans with take > 0
 */
function computePerShardLimitPlan(
	shardsToQuery: ShardInfo[],
	rowCounts: ShardRowCount[],
	globalOffset: number,
	globalLimit: number,
): ShardLimitPlan[] {
	// Build a map of shard_id -> row_count
	const countMap = new Map<number, number>();
	for (const rc of rowCounts) {
		countMap.set(rc.shard_id, rc.row_count);
	}

	// Sort shards by shard_id for deterministic ordering
	const sortedShards = [...shardsToQuery].sort(
		(a, b) => a.shard_id - b.shard_id,
	);

	const plans: ShardLimitPlan[] = [];
	let prefixBefore = 0;
	let remaining = globalLimit;

	for (const shard of sortedShards) {
		if (remaining <= 0) break;

		const shardRowCount = countMap.get(shard.shard_id) ?? 0;
		const skip = Math.max(0, globalOffset - prefixBefore);
		const available = Math.max(0, shardRowCount - skip);
		const take = Math.min(available, remaining);

		if (take > 0) {
			plans.push({ shard, skip, take });
			remaining -= take;
		}

		prefixBefore += shardRowCount;
	}

	return plans;
}

/**
 * Create a new SELECT statement with rewritten LIMIT/OFFSET
 */
function rewriteSelectLimitOffset(
	statement: SelectStatement,
	skip: number,
	take: number,
): SelectStatement {
	return {
		...statement,
		limit: {
			type: "Literal",
			value: take,
			raw: String(take),
		},
		offset:
			skip > 0
				? {
						type: "Literal",
						value: skip,
						raw: String(skip),
					}
				: undefined,
	};
}

// ============================================================================
// Global Pagination
// ============================================================================

/**
 * Apply global OFFSET and LIMIT to merged results
 *
 * @param rows - Merged rows from all shards
 * @param globalOffset - Global OFFSET to skip
 * @param globalLimit - Global LIMIT to take
 * @param rewriteApplied - If true, only apply LIMIT (OFFSET already consumed per-shard)
 */
function applyGlobalPagination(
	rows: Record<string, unknown>[],
	globalOffset: number,
	globalLimit: number | null,
	rewriteApplied: boolean,
): Record<string, unknown>[] {
	let result = rows;

	// Only apply OFFSET if rewrite was not applied
	if (!rewriteApplied && globalOffset > 0) {
		result = result.slice(globalOffset);
	}

	// Always apply LIMIT if present
	if (globalLimit !== null) {
		result = result.slice(0, globalLimit);
	}

	return result;
}

// ============================================================================
// Main SELECT Handler
// ============================================================================

/**
 * Execute a SELECT query on the appropriate shards
 *
 * This handler:
 * 1. Gets the query plan (which shards to query) from topology
 * 2. For eligible queries, computes per-shard LIMIT/OFFSET to reduce over-fetch
 * 3. Executes the query on target shards
 * 4. Merges results from all shards
 * 5. Applies global pagination (OFFSET/LIMIT) for correctness
 * 6. If results are short due to stale row counts, queries additional shards
 * 7. Adds cache statistics
 */
export async function handleSelect(
	statement: SelectStatement,
	_query: string,
	params: SqlParam[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { cache, topology, databaseId } = context;
	const tableName = extractTableNameFromAST(statement);

	if (!tableName) {
		throw new Error("Could not determine table name from SELECT query");
	}

	// STEP 1: Get cached query plan data
	const { planData, cacheHit } = await getCachedQueryPlanData(
		context,
		tableName,
		statement,
		params,
	);

	logger.info`Query plan determined for SELECT ${{ cacheHit }} ${{ shardsSelected: planData.shardsToQuery.length }} ${{ indexesUsed: planData.virtualIndexes.length }}`;

	const shardsToQuery = planData.shardsToQuery;

	// Extract global LIMIT/OFFSET for correctness enforcement
	const globalLimit = extractLiteralNumber(statement.limit);
	const globalOffset = extractLiteralNumber(statement.offset) ?? 0;

	// STEP 2: Check eligibility for per-shard LIMIT/OFFSET rewrite
	const eligibility = isEligibleForLimitRewrite(
		statement,
		shardsToQuery.length,
	);
	let rewriteApplied = false;
	let allResults: QueryResult[] = [];
	let allShardStats: ShardStats[] = [];

	if (eligibility.eligible && eligibility.globalLimit !== null) {
		// Fetch row counts from Topology
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const rowCounts = await topologyStub.getTableShardRowCounts(tableName);

		// Compute per-shard plan
		const limitPlans = computePerShardLimitPlan(
			shardsToQuery,
			rowCounts,
			eligibility.globalOffset,
			eligibility.globalLimit,
		);

		if (limitPlans.length > 0) {
			rewriteApplied = true;

			logger.info`LIMIT/OFFSET rewrite applied ${{ shardsOptimized: limitPlans.length }} ${{ totalShards: shardsToQuery.length }} ${{
				globalLimit: eligibility.globalLimit,
			}} ${{ globalOffset: eligibility.globalOffset }}`;

			// Execute per-shard with rewritten LIMIT/OFFSET
			for (const plan of limitPlans) {
				const rewrittenStmt = rewriteSelectLimitOffset(
					statement,
					plan.skip,
					plan.take,
				);
				const { results, shardStats } = await executeOnShards(
					context,
					[plan.shard],
					rewrittenStmt,
					params,
				);
				allResults.push(...results);
				allShardStats.push(...shardStats);
			}

			// STEP 3 (fallback): Check if we got fewer rows than expected due to stale counts
			const mergedForCheck = mergeResultsSimple(allResults, statement);
			const rowsGot = mergedForCheck.rows.length;
			const expected = eligibility.globalLimit;

			if (rowsGot < expected) {
				// Query additional shards that weren't in the original plan
				const queriedShardIds = new Set(
					limitPlans.map((p) => p.shard.shard_id),
				);
				const additionalShards = shardsToQuery
					.filter((s) => !queriedShardIds.has(s.shard_id))
					.sort((a, b) => a.shard_id - b.shard_id);

				let stillNeeded = expected - rowsGot;

				for (const shard of additionalShards) {
					if (stillNeeded <= 0) break;

					// Catch-up limit: at least stillNeeded, but batch for efficiency
					const catchUpLimit = Math.min(stillNeeded + 20, 200);
					const catchUpStmt = rewriteSelectLimitOffset(
						statement,
						0,
						catchUpLimit,
					);
					const { results, shardStats } = await executeOnShards(
						context,
						[shard],
						catchUpStmt,
						params,
					);
					allResults.push(...results);
					allShardStats.push(...shardStats);

					// Recalculate how many we still need
					const newMerged = mergeResultsSimple(allResults, statement);
					stillNeeded = expected - newMerged.rows.length;
				}

				logger.info`Stale row count fallback triggered ${{ additionalShardsQueried: additionalShards.length }} ${{ stillNeeded }}`;
			}
		} else {
			// All shards would return 0 rows based on row counts
			// This can happen if OFFSET exceeds total rows
			logger.info`LIMIT/OFFSET rewrite: no shards selected (OFFSET exceeds total rows)`;
		}
	}

	// STEP 4: If rewrite not applied, execute on all shards normally
	if (!rewriteApplied) {
		const { results, shardStats } = await executeOnShards(
			context,
			shardsToQuery,
			statement,
			params,
		);
		allResults = results;
		allShardStats = shardStats;
	}

	// STEP 5: Merge results from all shards
	const result = mergeResultsSimple(allResults, statement);

	// STEP 6: Apply global pagination for correctness
	if (globalLimit !== null || globalOffset > 0) {
		result.rows = applyGlobalPagination(
			result.rows,
			globalOffset,
			globalLimit,
			rewriteApplied,
		);
		result.rowsAffected = result.rows.length;
	}

	// STEP 7: Add cache statistics
	const stats = cache.getStats();
	result.cacheStats = {
		cacheHit,
		totalHits: stats.hits,
		totalMisses: stats.misses,
		cacheSize: stats.size,
	};

	// Add shard statistics
	result.shardStats = allShardStats;

	const totalFetched = allShardStats.reduce(
		(sum, s) => sum + s.rowsReturned,
		0,
	);
	logger.info`SELECT query completed ${{ shardsQueried: allShardStats.length }} ${{ rowsFetched: totalFetched }} ${{ rowsReturned: result.rows.length }} ${{ rewriteApplied }}`;

	return result;
}
