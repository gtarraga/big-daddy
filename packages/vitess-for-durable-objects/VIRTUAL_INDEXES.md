# Virtual Indexes Specification

## Implementation Status

**Current Status:** ✅ **COMPLETE** - All Core Features Implemented and Tested

**Phases:**
- ✅ Phase 1: Queue Infrastructure (COMPLETED)
- ✅ Phase 2: Foundation (Index Creation) (COMPLETED)
- ✅ Phase 3: Query Optimization (COMPLETED)
- ✅ Phase 4: Index Maintenance (COMPLETED)
- ⏳ Phase 5: Advanced Features (Future)

**What's Working:**
- ✅ Cloudflare Queue producer/consumer configured
- ✅ Job types defined (IndexBuildJob)
- ✅ Conductor has `enqueueIndexJob()` helper
- ✅ Queue consumer processes index build jobs
- ✅ Topology schema: virtual_indexes and virtual_index_entries tables
- ✅ Topology methods: createVirtualIndex, updateIndexStatus, batchUpsertIndexEntries, getIndexedShards, dropVirtualIndex
- ✅ Topology methods for index maintenance: addShardToIndexEntry, removeShardFromIndexEntry
- ✅ Batch upsert using single-entry writes (simple, safe, and fast given DO write performance)
- ✅ CREATE INDEX parsing in Conductor
- ✅ handleCreateIndex method in Conductor (validates, creates index definition, enqueues job)
- ✅ IF NOT EXISTS support for CREATE INDEX
- ✅ UNIQUE index support
- ✅ Queue consumer builds indexes from existing data (processBuildIndexJob)
- ✅ Query optimization uses virtual indexes to reduce shard fan-out
- ✅ Synchronous index maintenance for INSERT operations
- ✅ Synchronous index maintenance for UPDATE operations (only updates changed indexed columns)
- ✅ Synchronous index maintenance for DELETE operations
- ✅ Comprehensive test suite: 57 tests passing
  - 12 tests for Topology virtual index methods
  - 6 tests for CREATE INDEX in Conductor
  - 10 tests for end-to-end virtual index flows
  - Tests for index building, query optimization, and index maintenance

**Implementation Highlights:**
- **Dual-layer indexing**: Creates BOTH SQLite indexes on shards AND virtual indexes in Topology
  - SQLite indexes: Fast individual shard queries (reduces rows scanned = lower cost)
  - Virtual indexes: Shard-level routing optimization (reduces shard fan-out)
- **Smart UPDATE optimization**: Only updates indexes for columns that are in the SET clause AND have changed values
- **Pre-fetch strategy**: Fetches old values before UPDATE/DELETE to maintain indexes correctly
- **Strong consistency**: All index maintenance happens synchronously during writes
- **NULL handling**: NULL values are properly excluded from indexes
- **Efficient routing**: Queries automatically use indexes when WHERE clause matches indexed columns
- **Cost optimization**: Dramatically reduces Cloudflare SQL billing (charged per row scanned)

---

## Overview

Virtual indexes provide a **dual-layer indexing strategy** that optimizes both shard-level routing and individual shard query performance:

1. **Virtual indexes (metadata)**: Stored in Topology DO, map `value → [shard_ids]` for efficient query routing
2. **SQLite indexes (physical)**: Created on each storage shard for fast individual shard queries

This dual approach ensures optimal performance and minimal cost on Cloudflare's infrastructure.

## Problem

Without indexes, queries like `SELECT * FROM users WHERE email = 'alice@example.com'` must:
1. Query **all 100 shards** (expensive DO invocations)
2. Each shard does a **full table scan** (charged per row scanned on Cloudflare)
3. Results are merged at the Conductor

**Cost impact:** If each shard has 1000 rows, this scans 100,000 rows total!

## Solution: Dual-Layer Indexing

### Layer 1: Virtual Indexes (Shard Routing)

Store index metadata in Topology that maps: `(table, index_column, value) → [shard_ids]`

**Example:**
Table: `users` with 100 shards
Index: `email`

```typescript
virtual_indexes: {
  'users:email:alice@example.com': [3, 47],      // Alice's records on shards 3 and 47
  'users:email:bob@example.com': [12],           // Bob's record on shard 12
  'users:email:charlie@example.com': [3, 88, 91] // Charlie has records on 3 shards
}
```

Now `WHERE email = 'alice@example.com'` only queries shards 3 and 47 instead of all 100.

### Layer 2: SQLite Indexes (Per-Shard Performance)

On each shard, a real SQLite index exists:
```sql
CREATE INDEX idx_email ON users(email)
```

When shard 3 receives `WHERE email = 'alice@example.com'`, it uses the SQLite index for instant lookup instead of scanning all rows.

### Combined Performance

**Before indexing:**
- Query all 100 shards
- Each shard scans 1000 rows
- **Total: 100,000 rows scanned** 💸💸💸

**After dual-layer indexing:**
- Query only 2 shards (virtual index routing)
- Each shard uses SQLite index (scans ~1 row via index)
- **Total: ~2 rows scanned** 💰

**Result: 50,000x reduction in rows scanned = massive cost savings!**

## Architecture

### 1. Index Definition (Topology)

```typescript
interface VirtualIndex {
  table_name: string;
  column_name: string;
  index_type: 'hash' | 'unique';  // unique = at most one value per key
  created_at: string;
}

interface VirtualIndexEntry {
  table_name: string;
  column_name: string;
  key_value: string;              // The indexed value (stringified)
  shard_ids: number[];            // Which shards contain this value
  updated_at: string;
}
```

### 2. Index Maintenance (Synchronous)

Index maintenance for INSERT/UPDATE/DELETE happens synchronously during the write operation to ensure strong consistency and avoid extra queue overhead.

#### On INSERT:
```sql
INSERT INTO users (id, email, name) VALUES (123, 'alice@example.com', 'Alice')
```

**Process (Conductor):**
1. Determines target shard (e.g., shard 47) based on `id` hash
2. Executes INSERT on shard 47
3. **Synchronously** updates virtual indexes:
   - For each indexed column (e.g., email)
   - Get index entry for `users:email:alice@example.com`
   - Add shard 47 to the shard_ids array
   - Upsert the entry in Topology
4. Returns success to client

#### On UPDATE:
```sql
UPDATE users SET email = 'newemail@example.com' WHERE id = 123
```

**Process (Conductor):**
1. Routes to shard 47 (based on id)
2. Fetches old indexed column values: `SELECT * FROM users WHERE id = 123`
3. Executes UPDATE on shard 47
4. **Synchronously** updates virtual indexes:
   - **Only updates indexes for columns in the SET clause that actually changed**
   - Remove shard 47 from `users:email:alice@example.com`
   - Add shard 47 to `users:email:newemail@example.com`
   - If a value's shard_ids becomes empty, delete that entry
   - Skips index update if indexed column not in SET clause
5. Returns success to client

**Optimization**: If `UPDATE users SET name = 'Alice' WHERE id = 123`, the email index is not touched at all.

#### On DELETE:
```sql
DELETE FROM users WHERE id = 123
```

**Process (Conductor):**
1. Routes to shard 47
2. Fetches indexed column values before delete: `SELECT email FROM users WHERE id = 123`
3. Executes DELETE on shard 47
4. **Synchronously** updates virtual indexes:
   - Remove shard 47 from `users:email:alice@example.com`
   - If shard_ids becomes empty, delete the entry
5. Returns success to client

#### Why Synchronous Maintenance?

**Benefits:**
- ✅ **Strongly consistent** - Index always reflects current data state
- ✅ **Simpler architecture** - No queue consumer for updates needed
- ✅ **Lower latency** - No queue round-trip delay
- ✅ **Lower cost** - No queue invocations for every write
- ✅ **Immediate query benefits** - New data is immediately optimized

**Trade-offs:**
- ⚠️ **Slightly slower writes** - Added Topology RPC for each indexed column
- ✅ **Acceptable** - Single DO RPC adds ~10-20ms vs 100ms+ for queue round-trip
- ✅ **Still fast** - Writes complete in <50ms total including index updates

**Note:** Initial index building (CREATE INDEX) remains async via queue since it needs to scan all existing data.

### 3. Index Usage (Query Routing)

```sql
SELECT * FROM users WHERE email = 'alice@example.com'
```

**Enhanced routing logic:**
1. Parse query → detect `WHERE email = 'alice@example.com'`
2. Check Topology for virtual index on `users.email`
3. Lookup `users:email:alice@example.com` → get [3, 47]
4. Query only shards 3 and 47 (instead of all 100)
5. Merge results

### 4. Index Creation (Async with Cloudflare Queue)

```sql
CREATE INDEX idx_email ON users(email)
```

**Queue-based architecture to avoid blocking:**

#### Immediate Response (< 10ms):
1. Conductor receives `CREATE INDEX` statement
2. Validates syntax and column exists
3. Creates index definition in Topology with `status: 'building'`
4. Enqueues index build job to Cloudflare Queue
5. Returns success immediately to client

#### Background Processing (Cloudflare Queue Consumer):
```typescript
interface IndexBuildJob {
  type: 'build_index';
  table_name: string;
  column_name: string;
  index_name: string;
  database_id: string;
}
```

**Build process:**
1. Queue consumer receives job
2. Query all shards in batches: `SELECT DISTINCT column_value FROM table`
3. For each distinct value:
   - Query shards to find which contain that value
   - Create virtual_index_entry in Topology
4. Update index status: `building` → `ready`
5. If failure: Update status to `failed`, include error message

#### Index Status:
```typescript
type IndexStatus =
  | 'building'   // Initial state, background job processing
  | 'ready'      // Built and ready to use
  | 'failed'     // Build failed, see error_message
  | 'rebuilding' // Rebuild in progress (triggered manually or by repair job)
```

**Query behavior during build:**
- Index with `status: 'building'` → ignored, query all shards (normal behavior)
- Index with `status: 'ready'` → used for query optimization
- Index with `status: 'failed'` → ignored, query all shards

## Implementation Phases

### Phase 1: Queue Infrastructure ✅ COMPLETED
- ✅ Set up Cloudflare Queue binding in wrangler.jsonc
  - Producer: `INDEX_QUEUE` binding on `vitess-index-jobs` queue
  - Consumer: Configured with batch_size=10, timeout=5s, retries=3
- ✅ Create queue consumer handler
  - Queue consumer configuration added to main `wrangler.jsonc`
  - `src/queue-consumer.ts` - Queue message handler (`queueHandler` function)
  - Exported from `src/index.ts` as `queue` handler
  - Single worker handles both HTTP requests and queue messages
- ✅ Define job types (IndexBuildJob, IndexUpdateJob)
  - `src/Queue/types.ts` - TypeScript interfaces for all job types
  - MessageBatch and Message types for Cloudflare Queue integration
- ✅ Implement job dispatcher in Conductor
  - Added `indexQueue?: Queue` to ConductorClient constructor
  - Added `enqueueIndexJob()` helper method
  - Integrated with `createConductor()` function
- ✅ Generated types with `npm run cf-typegen`
  - `worker-configuration.d.ts` includes INDEX_QUEUE binding
- [ ] Add queue monitoring/observability (future enhancement)

### Phase 2: Foundation (Index Creation) ✅ COMPLETED
- ✅ Topology schema for virtual_indexes table
- ✅ Topology schema for virtual_index_entries table
- ✅ Topology CRUD methods for virtual indexes
- ✅ Parse CREATE INDEX statement
- ✅ Immediate: Validate and create index definition (status: 'building')
- ✅ Enqueue IndexBuildJob
- ✅ Tests for Topology virtual index methods
- ✅ Tests for CREATE INDEX in Conductor
- ✅ Queue consumer: Build index from existing data
- ✅ Queue consumer: Update status to 'ready' or 'failed'
- ✅ End-to-end tests for index building
- [ ] Add DROP INDEX support (Topology method exists, needs Conductor integration) - Future

### Phase 3: Query Optimization ✅ COMPLETED
- ✅ Extend determineShardTargets to check for indexes
- ✅ Lookup virtual_index_entries for simple WHERE clauses
- ✅ Use index to reduce shard list
- ✅ Handle index status (only use 'ready' indexes)
- ✅ Tests for index-based routing
- ✅ Tests demonstrating shard fan-out reduction

### Phase 4: Index Maintenance (Synchronous) ✅ COMPLETED
- ✅ Add Topology methods: addShardToIndexEntry, removeShardFromIndexEntry
- ✅ On INSERT: Synchronously update index entries with new values
- ✅ On UPDATE: Fetch old values, synchronously update indexes (remove from old, add to new)
- ✅ On UPDATE: Smart optimization - only update indexes for changed columns in SET clause
- ✅ On DELETE: Fetch old values, synchronously remove from index entries
- ✅ Tests for index maintenance during INSERT/UPDATE/DELETE
- ✅ Tests verify old values are removed and new values are added correctly

### Phase 5: Advanced Features
- [x] Composite indexes: `CREATE INDEX ON users(country, city)`
- [x] IN queries: `WHERE col IN (val1, val2)`
- [ ] Explore why we actually need to fetch old index values
- [ ] Ensure topology is not fetched multiple times in single conductor invocation
- [ ] Refactor index conductor code
- [ ] Index validation/repair background job
- [ ] Index statistics (cardinality, usage metrics)
- [ ] Index rebuild command
- [ ] Range queries (requires different structure)

## Key Design Decisions

### 1. Why Virtual (Metadata-Only)?
- **No storage overhead on shards** - each shard doesn't maintain its own index
- **Centralized consistency** - single source of truth in Topology
- **Fast updates** - only Topology metadata changes, no shard writes
- **Works with existing data** - can build indexes without changing shard schema

### 2. Why String Keys?
- Simplifies implementation (no complex key encoding)
- Works with any data type (numbers, strings, dates)
- Easy to serialize/deserialize
- Consistent with how SQLite handles comparisons

### 3. Consistency Guarantees
- **Eventually consistent** - index may briefly be out of sync after failures
- **Acceptable for reads** - may query extra shards, still returns correct results
- **Repair via background job** - periodic index validation/rebuild

### 4. Storage Concerns
- Each unique value = one Topology entry
- High cardinality columns (like email) → many entries
- **Mitigation:** Limit to columns with reasonable cardinality
- **Future:** Shard the index itself if Topology becomes too large

## Example Queries

### Simple Equality (Index Used)
```sql
SELECT * FROM users WHERE email = 'alice@example.com'
-- Lookup: users:email:alice@example.com → [3, 47]
-- Query: Only shards 3, 47
```

### Compound Condition (Partial Index Use)
```sql
SELECT * FROM users WHERE email = 'alice@example.com' AND age > 25
-- Lookup: users:email:alice@example.com → [3, 47]
-- Query: Shards 3, 47 with full WHERE clause
-- Each shard filters by age > 25
```

### Shard Key + Index (Best Case)
```sql
SELECT * FROM users WHERE id = 123 AND email = 'alice@example.com'
-- Route by id → shard 47
-- Query: Only shard 47
-- No index lookup needed (shard key takes precedence)
```

### No Index Available
```sql
SELECT * FROM users WHERE name LIKE '%Alice%'
-- No index on name
-- Query: All shards (existing behavior)
```

## Cloudflare Queue Configuration

### wrangler.toml
```toml
[[queues.producers]]
queue = "vitess-index-jobs"
binding = "INDEX_QUEUE"

[[queues.consumers]]
queue = "vitess-index-jobs"
max_batch_size = 10
max_batch_timeout = 5
max_retries = 3
dead_letter_queue = "vitess-index-dlq"
```

### Queue Consumer Worker
Separate worker that processes index jobs:
- Receives batches of up to 10 jobs
- Processes in parallel where possible
- Retries failed jobs (max 3 times)
- Dead letter queue for persistent failures

### Job Types
```typescript
type IndexJob = IndexBuildJob;

interface IndexBuildJob {
  type: 'build_index';
  database_id: string;
  table_name: string;
  column_name: string;
  index_name: string;
  created_at: string;
}
```

**Note:** IndexUpdateJob has been removed - index maintenance now happens synchronously during INSERT/UPDATE/DELETE operations.

## Open Questions

1. **Index cardinality limits?** Should we warn/error on high-cardinality columns?
   - *Recommendation:* Warn if > 100k unique values, error if > 1M

2. **Index size limits?** What's the max size for virtual_index_entries in Topology?
   - *Consideration:* Durable Object storage limits, may need to shard index itself

3. **Queue batch size?** How many index updates to batch together?
   - *Recommendation:* Start with 10, tune based on performance

4. **Composite indexes?** Support `(col1, col2)` in Phase 1 or defer?
   - *Recommendation:* Defer to Phase 5

5. **NULL handling?** How do we index NULL values?
   - *Recommendation:* Index as string "NULL", distinguish from actual "NULL" string

6. **Retry strategy?** What if queue consumer fails?
   - *Solution:* Cloudflare Queue handles retries (max 3), then DLQ

7. **Index consistency?** What if index update fails but query succeeds?
   - *Solution:* Eventually consistent, repair job validates periodically
