# Table Resharding Implementation Plan

## Overview

This document outlines the implementation strategy for the `PRAGMA reshardTable` feature, which allows dynamic rebalancing of table data across multiple shards without downtime.

## Problem Statement

When a table is initially created, it defaults to a single shard. As data grows, we need to redistribute it across multiple shards for better performance and scalability. The challenge is doing this **without blocking reads/writes** and **safely handling failures**.

### The Write Handling Challenge

During resharding, writes and updates continue hitting the old shard while we copy data to new shards. This creates a consistency problem:

```
Problem:
  T0: Copy row (name='John') to new shards
  T1: User updates row (name='Jane')

  Result: New shards have stale data (name='John')
```

### The Solution: Change Log Capture & Replay

We solve this using server-side transaction logging:

1. **Log writes** - During Phase 3, all writes to the resharding table are captured in a queue
2. **Copy data** - Queue worker copies existing data to new shards
3. **Replay changes** - After copy, replay all captured changes to new shards
4. **Verify consistency** - Compare row counts to ensure no data loss
5. **Switch** - Mark new shards active, old shard deleted

This ensures **zero data loss** and **no write latency impact** because:
- ✅ Writes still go to old shard only (single write path)
- ✅ Changes are captured asynchronously
- ✅ Idempotent replay is safe (UPDATE applied twice = idempotent)
- ✅ Verification catches any mismatches

## Solution Architecture

The resharding process is split into **5 distinct phases** to ensure data consistency and availability:

### Phase 1: Create Pending Shards

**When**: User executes `PRAGMA reshardTable('table_name', new_shard_count)`

**What happens**:
1. Conductor validates the request (table exists, new shard count is valid)
2. **Creates new shards** in the storage layer and registers them in topology
3. **Marks all new shards as "PENDING"** status (critical!)
4. **Old shards remain ACTIVE** and continue handling reads/writes

**Why pending state matters**:
- Query planning in the Conductor checks shard status and **ignores PENDING shards**
- Reads/writes continue hitting only ACTIVE (old) shards
- No application-level changes needed - resharding is completely transparent

**Topology state after Phase 1**:
```
Table: users (num_shards: 1 → 3)

Old Shard (ACTIVE):
  - shard_id: 0
  - status: active
  - location: node_1

New Shards (PENDING):
  - shard_id: 1
  - status: pending        ← Not used by conductor yet
  - location: node_2

  - shard_id: 2
  - status: pending        ← Not used by conductor yet
  - location: node_3
```

### Phase 2: Dispatch Queue Job

**When**: After new shards are successfully created

**What happens**:
1. Create a `ReshardTableJob` message containing:
   - Table name
   - Source shard ID (old shard)
   - Target shard IDs (new shards)
   - Shard key and distribution strategy (hash/range)
2. **Enqueue the job** to the INDEX_QUEUE (reusing existing queue infrastructure)
3. Return success to the user immediately (async operation)

**Job structure**:
```typescript
interface ReshardTableJob extends IndexJob {
  type: 'ReshardTableJob'
  table_name: string
  source_shard_id: number
  target_shard_ids: number[]
  shard_key: string
  shard_strategy: 'hash' | 'range'
}
```

### Phase 3: Async Data Copy with Change Log Replay

**When**: Queue worker picks up the ReshardTableJob

**The Challenge**: While we're copying data, users continue writing/updating the old shard. We need to capture these changes and apply them to new shards.

**Solution: Change Log Replay**

```
Phase 3A: Start Change Log
  ↓
  Enable write logging for this table
  All INSERT/UPDATE/DELETE operations are captured

Phase 3B: Copy Existing Data
  ↓
  Fetch all rows from source shard
  Redistribute to target shards in batches
  Meanwhile, all changes are being logged

Phase 3C: Replay Captured Changes
  ↓
  After copy completes, replay all logged changes
  Apply to all target shards
  Changes are idempotent (safe to apply multiple times)

Phase 3D: Verify Data Integrity
  ↓
  Count rows in source shard vs sum of target shards
  If mismatch, mark FAILED and abort
```

**Change Log Capture Mechanism**:

During Phase 3, the Conductor intercepts writes:
```typescript
if (reshardingInProgress(tableName)) {
  // Log the change to resharding queue
  await logReshardingChange({
    table: tableName,
    operation: 'INSERT' | 'UPDATE' | 'DELETE',
    query: originalQuery,
    params: queryParams,
    timestamp: Date.now()
  });
}

// Then execute normally on old shard (single write path!)
await executeOnOldShard(query, params);
```

**Idempotent Replay**:
- Changes can be safely replayed multiple times
- `UPDATE users SET name='Jane' WHERE id=1` → same result whether applied once or twice
- `DELETE FROM users WHERE id=999` → idempotent (already deleted = no error)
- Safe against duplicates in log

**Batch Processing**:
1. **Copy phase**: Stream rows in batches (1000 rows/batch) to new shards
2. **Replay phase**: Apply change log entries in order to ensure consistency
3. **Verification**: Compare row counts to detect issues

**Error Handling Strategy**:
```
For copy batches:
  Try:
    Insert batch into target shards
    Update progress tracking
  Catch error:
    Retry up to N times with exponential backoff
    If retries exhausted:
      Mark target shards as "FAILED"
      Stop processing
      Return failure

For replay phase:
  Try:
    Apply change log entries to target shards
  Catch error:
    Log error details
    Mark as "REPLAY_FAILED"
    Continue to next change (don't retry individual changes)
    During verification phase, detect data mismatch
```

**Why this approach**:
- ✅ **Single write path** - All writes go to old shard, no routing changes
- ✅ **Server-side capture** - Application doesn't need to know about resharding
- ✅ **Idempotent** - Safe to replay changes
- ✅ **Ordered** - Changes applied in temporal order
- ✅ **Consistent** - Both copy and replay happen, no data loss

### Phase 4: Atomic Status Switch

**When**: All data successfully copied to new shards

**What happens**:
1. **Verify data integrity** - Count rows in new shards vs source shard
2. **Mark new shards as ACTIVE**: All target shards status → `active`
3. **Mark old shard as TO_BE_DELETED**: Source shard status → `to_be_deleted`
4. **Update topology metadata**: Update `num_shards` field if needed

**Atomicity**:
- All status updates happen in a single Topology DO call
- Conductor respects the new topology on next cache miss
- Existing queries on old shards continue until they finish

**Conductor behavior post-switch**:
```
Query planning now includes:
- ACTIVE shards (old + new)
- Excludes PENDING shards
- Excludes TO_BE_DELETED shards
- (Eventually excludes FAILED shards)

Effect: New queries automatically use new shard distribution
```

### Phase 5: Delete Old Shard Data

**When**: After status switch (TO_BE_DELETED is durable)

**What happens**:
1. **Verify no queries** are still targeting the old shard
2. **Delete all data** from the old shard's storage
3. **Remove shard metadata** from topology (optional - can keep for audit trail)

**Safety considerations**:
- Done AFTER new shards are marked active
- Old shard marked TO_BE_DELETED prevents conductor from using it
- Can be done asynchronously
- If this phase fails, old shard data remains (safe to retry)

## Data Flow Diagram

```
User Query
    ↓
┌─────────────────────────────────────────────────────────┐
│ PRAGMA reshardTable('users', 3)                         │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 1: Create Pending Shards                          │
│ - Create shards 1, 2, 3                                 │
│ - Mark as PENDING                                       │
│ - Old shard 0 stays ACTIVE                              │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 2: Dispatch Queue Job                             │
│ - Enqueue ReshardTableJob                               │
│ - Return to user immediately                            │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ Parallel Execution (User can continue reading/writing)  │
│                                                         │
│ Queue Worker              │  Conductor                  │
│ ──────────────────────────────────────────────────────  │
│ Phase 3A: Start Change Log│  All writes to users table │
│                           │  are LOGGED to queue        │
│ Phase 3B: Copy Data       │  Example:                   │
│ - Fetch from shard 0      │  INSERT, UPDATE, DELETE     │
│ - Stream to 1,2,3 in      │  → logged to queue          │
│   batches (1000/batch)    │  → executed on shard 0      │
│                           │                             │
│ Phase 3C: Replay Log      │  Users get consistent data  │
│ - Fetch all logged        │  even though copy ongoing   │
│   changes from queue      │                             │
│ - Apply to shards 1,2,3   │                             │
│   (idempotent replay)     │                             │
│                           │                             │
│ Phase 3D: Verify          │                             │
│ - Count rows: source vs   │                             │
│   sum(targets)            │                             │
│ - If mismatch: FAILED     │                             │
└─────────────────────────────────────────────────────────┘
    ↓ (all data copied successfully)
┌─────────────────────────────────────────────────────────┐
│ Phase 4: Atomic Status Switch                           │
│ - Verify data integrity                                 │
│ - Mark shards 1,2,3 as ACTIVE                           │
│ - Mark shard 0 as TO_BE_DELETED                         │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ Conductor Cache Miss → New Topology Fetched             │
│ - Now uses shards 1, 2, 3 for this table                │
│ - Reads distributed across 3 shards                     │
│ - Writes distributed across 3 shards                    │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 5: Delete Old Shard Data                          │
│ - Confirm shard 0 is TO_BE_DELETED                      │
│ - Delete all data from shard 0                          │
│ - Clean up shard metadata (optional)                    │
└─────────────────────────────────────────────────────────┘
```

## Shard Status State Machine

```
           CREATE
             ↓
        PENDING ───────→ ACTIVE ───────→ TO_BE_DELETED
             ↓                              (during cleanup)
           FAILED
         (on error)
```

**Status Meanings**:
- **PENDING**: Shard exists but not yet used by conductor. Data being populated.
- **ACTIVE**: Shard is operational. Conductor routes reads/writes to it.
- **TO_BE_DELETED**: Shard is being decommissioned. No longer used by conductor, waiting for data deletion.
- **FAILED**: Resharding job failed. Shard has partial data and should not be used.

## Implementation Components

### 1. Conductor Changes

**File**: `src/engine/conductor.ts`

Add handling for `PragmaStatement` type and write logging:
```typescript
// Main query execution flow
if (statement.type === 'PragmaStatement') {
  return this.handlePragma(statement, query, cid);
}

// Add logging during write operations
if (statement.type === 'InsertStatement' ||
    statement.type === 'UpdateStatement' ||
    statement.type === 'DeleteStatement') {

  const tableName = extractTableName(statement);
  const reshardingState = await this.topology.getReshardingState(tableName);

  if (reshardingState && reshardingState.status === 'copying') {
    // Phase 3A: Log the change to queue for later replay
    await this.indexQueue.send({
      type: 'ReshardingChangeLog',
      resharding_id: reshardingState.change_log_id,
      table_name: tableName,
      operation: statement.type === 'InsertStatement' ? 'INSERT' :
                 statement.type === 'UpdateStatement' ? 'UPDATE' :
                 'DELETE',
      query: query,
      params: params,
      timestamp: Date.now()
    });
  }

  // Then execute normally on active shard (single write path)
  return this.executeOnShards(query, params, statement, ...);
}

private async handlePragma(statement: PragmaStatement, query: string, cid: string): Promise<QueryResult> {
  if (statement.name.toLowerCase() === 'reshardtable') {
    return this.handleReshardTable(statement, cid);
  }
  throw new Error(`Unsupported PRAGMA: ${statement.name}`);
}

private async handleReshardTable(stmt: PragmaStatement, cid: string): Promise<QueryResult> {
  const [tableName, newShardCount] = stmt.arguments;

  if (!tableName || !newShardCount) {
    throw new Error('PRAGMA reshardTable requires table name and shard count');
  }

  const table = tableName.type === 'Literal' ? tableName.value : tableName.name;
  const count = newShardCount.type === 'Literal' ? newShardCount.value : parseInt(newShardCount.name);

  // Phase 1: Create pending shards
  const changeLogId = crypto.randomUUID();
  const newShards = await this.topology.createPendingShards(table, count, changeLogId);

  // Phase 2: Dispatch queue job
  const jobId = crypto.randomUUID();
  await this.indexQueue.send({
    type: 'ReshardTableJob',
    id: jobId,
    table_name: table,
    source_shard_id: 0,  // Assuming current single shard is ID 0
    target_shard_ids: newShards.map(s => s.shard_id),
    shard_key: '...',  // Get from table metadata
    shard_strategy: 'hash',  // Get from table metadata
    change_log_id: changeLogId
  });

  return {
    rows: [{
      job_id: jobId,
      status: 'queued',
      message: `Resharding ${table} to ${count} shards`,
      change_log_id: changeLogId
    }]
  };
}
```

### 2. Topology Changes

**File**: `src/engine/topology.ts`

Update `TableShard` interface:
```typescript
interface TableShard {
  shard_id: number
  table_name: string
  status: 'active' | 'pending' | 'to_be_deleted' | 'failed'  // Add new statuses
  node_id: string
  created_at: number
  updated_at: number
}
```

Update query planning to ignore non-ACTIVE shards:
```typescript
// In getQueryPlanData()
const activeShards = tableShards.filter(s => s.status === 'active');
const shardsToQuery = determineShards(activeShards, ...);
```

### 3. Queue Job Types

**File**: `src/engine/queue/types.ts`

```typescript
export interface ReshardTableJob extends IndexJob {
  type: 'ReshardTableJob'
  table_name: string
  source_shard_id: number
  target_shard_ids: number[]
  shard_key: string
  shard_strategy: 'hash' | 'range'
  change_log_id: string  // Points to where changes are being logged
  progress?: {
    phase: 'copying' | 'replaying' | 'verifying'
    rows_copied: number
    total_rows: number
    changes_replayed: number
    last_key?: string
    status: 'in_progress' | 'completed' | 'failed'
  }
}

// Changes are logged to queue with this structure
export interface ReshardingChangeLog extends Message {
  type: 'ReshardingChangeLog'
  resharding_id: string  // Links to ReshardTableJob
  table_name: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  query: string
  params: any[]
  timestamp: number
}
```

### 4. Queue Worker Handler

**File**: `src/queue-consumer.ts`

```typescript
if (job.type === 'ReshardTableJob') {
  await handleReshardTableJob(job, env);
}

async function handleReshardTableJob(job: ReshardTableJob, env: Env) {
  const {
    table_name,
    source_shard_id,
    target_shard_ids,
    change_log_id,
    shard_key,
    shard_strategy
  } = job;

  try {
    // Phase 3A: Changes are being logged by Conductor (already started)

    // Phase 3B: Copy existing data to new shards
    const totalRows = await copyShardData(
      source_shard_id,
      target_shard_ids,
      table_name,
      shard_key,
      shard_strategy
    );

    // Phase 3C: Replay all changes captured during copy
    const changesReplayed = await replayChangeLog(
      change_log_id,
      target_shard_ids,
      shard_key
    );

    // Phase 3D: Verify data integrity
    const sourceCount = await countRows(source_shard_id);
    const targetCount = await countRows(target_shard_ids);

    if (sourceCount !== targetCount) {
      throw new Error(
        `Row count mismatch: source=${sourceCount}, targets=${targetCount}`
      );
    }

    // Mark resharding complete (ready for Phase 4 atomic switch)
    await topology.markReshardingComplete(table_name);

  } catch (error) {
    await topology.markReshardingFailed(table_name, error.message);
    throw error;
  }
}

async function replayChangeLog(
  changeLogId: string,
  targetShardIds: number[],
  shardKey: string
): Promise<number> {
  const changes = await indexQueue.getAllMessages(changeLogId);
  let replayed = 0;

  for (const change of changes) {
    try {
      // Parse query to determine affected shards
      const stmt = parse(change.query);
      const affectedShardIds = determineAffectedShards(
        stmt,
        change.params,
        targetShardIds,
        shardKey
      );

      // Apply change to all affected target shards (idempotent)
      for (const shardId of affectedShardIds) {
        await storage.executeQuery(
          change.query,
          change.params,
          shardId
        );
      }

      replayed++;
    } catch (error) {
      // Log error but continue - verification phase will catch mismatches
      logger.error(`Failed to replay change ${change.id}`, error);
    }
  }

  return replayed;
}
```

### 5. Storage Layer Changes

**File**: `src/engine/storage.ts`

Add method to batch-copy rows:
```typescript
async copyRowsBetweenShards(
  sourceData: any[],
  targetShardIds: number[],
  shardKey: string,
  shardStrategy: 'hash' | 'range'
): Promise<void>
```

## Concrete Example: Write During Resharding

Let's trace a real scenario where a write happens during Phase 3B (copy):

```
Timeline:
T0:00 - User runs: PRAGMA reshardTable('users', 3)
        Phase 1: Create shards 1, 2, 3 (PENDING)
        Phase 2: Dispatch ReshardTableJob
        Change log ID: abc123

T0:05 - Queue worker starts Phase 3B: Copy data
        Fetching rows from shard 0 in batches
        Progress: 0/1000 rows copied

T0:10 - Meanwhile, user executes: INSERT INTO users (id, name) VALUES (999, 'Alice')

        Conductor flow:
        1. Parse INSERT statement
        2. Check: Is resharding in progress for 'users'? YES
        3. Log to queue:
           {
             type: 'ReshardingChangeLog',
             resharding_id: 'abc123',
             operation: 'INSERT',
             query: 'INSERT INTO users (id, name) VALUES (?, ?)',
             params: [999, 'Alice'],
             timestamp: T0:10
           }
        4. Execute INSERT on shard 0 (ACTIVE)
        5. Return success to user (no latency impact)

T0:15 - Queue worker continues copying
        Progress: 500/1000 rows copied
        (Change logs keep getting queued)

T0:20 - User updates: UPDATE users SET name='Alicia' WHERE id=999

        Conductor flow:
        1. Log to queue (same as above, but UPDATE)
        2. Execute on shard 0
        3. Return success

T0:25 - Queue worker finishes Phase 3B
        All 1000 rows copied to shards 1, 2, 3

        Now starts Phase 3C: Replay logs
        Fetches all ReshardingChangeLog messages:
        - INSERT (id=999, name='Alice') at T0:10
        - UPDATE (id=999, name='Alicia') at T0:20

        For each change:
        1. Parse INSERT statement
        2. Determine target shard: hash(999) = shard 2
        3. Apply: INSERT INTO shard 2

        4. Parse UPDATE statement
        5. Determine target shard: hash(999) = shard 2
        6. Apply: UPDATE on shard 2

        Result: Shard 2 now has row with id=999, name='Alicia'

T0:30 - Phase 3D: Verify
        Source (shard 0): 1001 rows
        Targets (1+2+3): 1001 rows
        ✓ Match! Proceed to Phase 4

T0:31 - Phase 4: Atomic switch
        Mark shards 1, 2, 3 as ACTIVE
        Mark shard 0 as TO_BE_DELETED

T0:35 - Conductor cache miss
        Fetches new topology
        Future writes to 'users' table go to shards 1, 2, 3

T0:40 - Phase 5: Delete old data
        Delete all rows from shard 0
        Metadata cleanup
```

**Key observations**:
- ✅ User's write latency is **unchanged** (no extra writes)
- ✅ All data is **consistent** (copy + replay captures everything)
- ✅ Resharding happens **transparently** (application doesn't know)
- ✅ No **blocking** (users continue reading/writing on old shard)

## Timeline & Safety Guarantees

| Phase | Duration | Risk Level | Rollback Option |
|-------|----------|------------|-----------------|
| 1. Create PENDING shards | Seconds | Low | Delete pending shards |
| 2. Dispatch queue job | Milliseconds | None | Cancel job before processing |
| 3. Copy data async | Minutes to hours | Medium | Retry, mark FAILED if needed |
| 4. Atomic switch | Milliseconds | Low | Switch back (if needed) |
| 5. Delete old data | Seconds | Very Low | Already copied elsewhere |

## Error Scenarios & Recovery

### Scenario: Copy Job Fails Mid-Way

**What happens**:
1. Queue worker catches exception
2. Retries with exponential backoff (3 attempts)
3. If all retries fail: marks target shards as FAILED
4. User can retry with `PRAGMA reshardTable` again (old shards still ACTIVE)

**User action**: Fix the underlying issue and re-run PRAGMA

### Scenario: Node Dies During Copy

**What happens**:
1. Queue worker detects node is down
2. Fails the batch
3. Retries against same target shard (node comes back up)
4. Or marks FAILED if node stays down

**Result**: Safe - old shards still active, users unaffected

### Scenario: Status Switch Fails

**What happens**:
1. Topology update fails
2. New shards stay PENDING
3. Old shards stay ACTIVE
4. Queue job marked as incomplete

**Recovery**: User can retry the resharding operation

## Monitoring & Observability

Track resharding operations with:
- **Logs**: Job start, batch progress, status transitions, errors
- **Metrics**: Rows/sec throughput, job duration, failure rate
- **Alerts**: Job failures, timeout thresholds, data mismatch

Example query to monitor:
```sql
-- Show active resharding jobs
SELECT job_id, table_name, progress FROM resharding_jobs
WHERE status = 'in_progress'
ORDER BY started_at DESC;

-- Show failed reshards (user action needed)
SELECT table_name, source_shard_id, error_message
FROM resharding_jobs
WHERE status = 'failed'
ORDER BY updated_at DESC;
```

## Future Enhancements

1. **Online verification** - Compare row counts/checksums during Phase 4
2. **Partial shard rebalancing** - Redistribute only hot shards
3. **Priority queues** - Prioritize resharding smaller tables
4. **Progress tracking API** - Let users query resharding status
5. **Scheduled resharding** - Off-peak automatic rebalancing based on growth metrics

## Critical Missing Piece: Change Log Capture & Replay

**This was the key insight addressing the write handling challenge.**

Without change log replay, we would have a data consistency problem: writes during Phase 3 would only hit the old shard, leaving the new shards with stale data.

By capturing all writes in a queue during Phase 3B (copy) and replaying them in Phase 3C (after copy completes), we guarantee:
- **Eventual consistency**: New shards eventually have all data including writes during copy
- **No write latency**: Writes still go to old shard only, no dual-write overhead
- **No application changes**: Server-side solution, transparent to users
- **Idempotent**: Safe to replay changes (deterministic result)
- **Verifiable**: Count rows before/after to catch mismatches

This approach is inspired by **write-ahead logs in databases** and **CDC (Change Data Capture)** patterns.

## Summary

This plan achieves **zero-downtime resharding** by:
- ✅ Keeping reads/writes on old shards during copy
- ✅ Using PENDING status to hide incomplete shards
- ✅ **Capturing changes in a transaction log** (new!)
- ✅ Replaying changes to new shards after copy (new!)
- ✅ Async processing via queue (no blocking)
- ✅ Retry logic for transient failures
- ✅ Atomic status switch (all-or-nothing)
- ✅ Safe cleanup of old data after verification

**Next Steps**: Implement the five components outlined above, starting with Conductor changes to handle PragmaStatement and write logging.
