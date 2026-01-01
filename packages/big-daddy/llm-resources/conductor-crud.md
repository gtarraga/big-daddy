# Conductor CRUD Operations

## SELECT (`crud/select.ts`)
**Flow:**
1. Extract table name from AST
2. Get cached query plan → `shardsToQuery`
3. Check eligibility for LIMIT/OFFSET optimization
4. If eligible: compute per-shard LIMIT/OFFSET using row counts
5. Execute on shards (with rewritten LIMIT/OFFSET if eligible)
6. Merge results with `mergeResultsSimple()`
7. Apply global pagination (OFFSET/LIMIT) for correctness
8. Handle stale row count fallback (query additional shards if needed)
9. Add cache stats

**Key:** Uses topology cache to skip Topology DO calls on cache hits.

### Distributed LIMIT/OFFSET (Phase 2)

**Correctness:** Global LIMIT/OFFSET is always enforced post-merge, guaranteeing correct row counts.

**Optimization:** For eligible fan-out queries, per-shard LIMIT/OFFSET is computed using row counts to reduce over-fetch:
- Eligibility: No WHERE, no ORDER BY, no aggregation, literal LIMIT/OFFSET
- Uses prefix sum algorithm across shards sorted by shard_id
- Fallback: If results are short (stale counts), additional shards are queried

**Not yet supported (future work):**
- Global ORDER BY + LIMIT (distributed top-K) requires k-way merge
- See "Future: Distributed Top-K" section below

---

## INSERT (`crud/insert.ts`)
**Flow:**
1. Get query plan data (all shards for table)
2. `groupInsertByShards()` - hash shard key to determine target shard per row
3. Log write if resharding in progress
4. Execute per-shard INSERTs with remapped parameters
5. Dispatch index maintenance events
6. Invalidate cache
7. **Bump per-shard row counts via `batchBumpTableShardRowCounts()`**

**Key Functions:**
- `groupInsertByShards()` - Groups rows by target shard, remaps placeholder indices

**Important:** 
- Multi-row INSERTs are split by shard key hash
- Row counts use VALUES.length (not SQLite's rowsWritten which includes index writes)

---

## UPDATE (`crud/update.ts`)
**Flow:**
1. Get query plan
2. Log write if resharding
3. If indexes exist: `[SELECT before, UPDATE, SELECT after]` batch
4. Execute on shards
5. Dispatch index maintenance with old/new row diffs
6. Invalidate cache

**Key:** Captures indexed column values before AND after for proper index sync.

---

## DELETE (`crud/delete.ts`)
**Flow:**
1. Get query plan
2. Log write if resharding
3. If indexes exist: `[SELECT capture, DELETE]` batch
4. Execute on shards
5. Dispatch index maintenance with old rows
6. Invalidate cache
7. **Decrement per-shard row counts via `batchBumpTableShardRowCounts()`**

---

## Shared Utilities (`utils/write.ts`)

### `executeOnShards()`
- Batches shards in groups of 7 (Cloudflare subrequest limit)
- Injects `_virtualShard` filter via `injectVirtualShard()`
- Uses Effect for error handling
- Returns `{ results, shardStats }`

### `getCachedQueryPlanData()`
- Checks TopologyCache first
- Falls back to Topology DO on miss
- Caches result with appropriate TTL

### `invalidateCacheForWrite()`
- Invalidates table cache
- Invalidates affected index caches

### `logWriteIfResharding()`
- Logs writes to queue during active resharding for replay

---

## Virtual Shard Injection (`utils/helpers.ts`)

### For INSERT:
```typescript
// Adds _virtualShard column and placeholder per row
columns: [...original, '_virtualShard']
values: [...original, { type: 'Placeholder', parameterIndex: N }]
```

### For SELECT/UPDATE/DELETE:
```typescript
// Adds WHERE clause filter
WHERE original_condition AND _virtualShard = ?
```

---

## Index Maintenance (`utils/index-maintenance.ts`)

### `prepareIndexMaintenanceQueries()`
Returns statement batches based on operation:
- INSERT: `[INSERT]`
- DELETE: `[SELECT, DELETE]`
- UPDATE: `[SELECT before, UPDATE, SELECT after]`

### `dispatchIndexSyncingFromQueryResults()`
Generates and queues index events:
- INSERT → `add` events for new values
- DELETE → `remove` events for old values
- UPDATE → `remove` old + `add` new (diff-based)

---

## Future: Distributed Top-K (ORDER BY + LIMIT)

**Not implemented.** Global ORDER BY + LIMIT across shards requires:

### Why it's hard
- Correct global ordering requires merging *sorted* shard streams
- OFFSET complicates it (need to discard first O rows of merged order)
- Without matching local index, shard queries may be full scans

### Approach (when implemented)
1. **Eligibility:** Single-column ORDER BY, simple collation, no expressions
2. **Execution:**
   - Query each shard with `ORDER BY ... LIMIT (OFFSET + LIMIT)`
   - K-way merge shard results using priority queue by ORDER BY key
   - Apply OFFSET/LIMIT on merged stream
3. **Optimization:** If local SQLite index matches ORDER BY, avoid full scan
