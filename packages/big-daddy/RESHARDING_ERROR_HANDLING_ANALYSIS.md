# Resharding Error Handling and Cleanup Logic Analysis

## Overview
This document provides a comprehensive analysis of the resharding state management, error handling, and cleanup logic in the Big Daddy distributed database system.

## 1. ReshardingState Type Definition

**File**: `/home/thomas/Projects/databases/packages/big-daddy/src/engine/topology.ts` (lines 41-50)

```typescript
export interface ReshardingState {
  table_name: string;
  source_shard_id: number;
  target_shard_ids: string; // Stored as JSON array
  change_log_id: string;
  status: 'pending_shards' | 'copying' | 'replaying' | 'verifying' | 'complete' | 'failed';
  error_message: string | null;
  created_at: number;
  updated_at: number;
}
```

### Status Values:
- **pending_shards**: Initial state after createPendingShards() - pending shards created but resharding not started
- **copying**: Data copy phase in progress (Phase 3B)
- **replaying**: Change log replay phase (Phase 3C) - not yet used in current implementation
- **verifying**: Data integrity verification phase (Phase 3D)
- **complete**: Resharding completed successfully
- **failed**: Resharding failed with error message stored

---

## 2. Status Update Methods in Topology

### 2.1 Mark Resharding as Failed
**File**: `src/engine/topology.ts` (lines 951-964)

```typescript
async markReshardingFailed(tableName: string, errorMessage: string): Promise<void> {
  this.ensureCreated();

  const now = Date.now();
  this.ctx.storage.sql.exec(
    `UPDATE resharding_states SET status = 'failed', error_message = ?, updated_at = ? WHERE table_name = ?`,
    errorMessage,
    now,
    tableName
  );
}
```

**When it's called**:
- In `queue-consumer.ts` at line 477 when any phase fails (copy, replay, verify)
- Captures error message for debugging and investigation

**Key features**:
- Atomically updates status to 'failed' with error message
- Updates timestamp for audit trail
- Does NOT clean up pending shards - allows retry with createPendingShards()

### 2.2 Mark Resharding as Complete
**File**: `src/engine/topology.ts` (lines 939-948)

```typescript
async markReshardingComplete(tableName: string): Promise<void> {
  this.ensureCreated();

  const now = Date.now();
  this.ctx.storage.sql.exec(
    `UPDATE resharding_states SET status = 'complete', updated_at = ? WHERE table_name = ?`,
    now,
    tableName
  );
}
```

**When it's called**:
- In `queue-consumer.ts` at line 465 after successful data deletion (Phase 5)
- Marks the entire resharding operation as complete

### 2.3 Start Resharding (Transition to Copying Phase)
**File**: `src/engine/topology.ts` (lines 924-933)

```typescript
async startResharding(tableName: string): Promise<void> {
  this.ensureCreated();

  const now = Date.now();
  this.ctx.storage.sql.exec(
    `UPDATE resharding_states SET status = 'copying', updated_at = ? WHERE table_name = ?`,
    now,
    tableName
  );
}
```

**When it's called**:
- In `queue-consumer.ts` at line 415 at the start of Phase 3B (copy data)
- Transitions from 'pending_shards' to 'copying'

---

## 3. Pending Shard Status Management

### 3.1 TableShard Status Values
**File**: `src/engine/topology.ts` (lines 28-35)

```typescript
export interface TableShard {
  table_name: string;
  shard_id: number;
  node_id: string;
  status: 'active' | 'pending' | 'to_be_deleted' | 'failed';
  created_at: number;
  updated_at: number;
}
```

**Status lifecycle during resharding**:
1. **pending**: Created by createPendingShards() - not queried by conductor
2. **active**: Marked active by atomicStatusSwitch() - now queried by conductor
3. **to_be_deleted**: Source shard marked by atomicStatusSwitch() - excluded from queries

### 3.2 Creating Pending Shards
**File**: `src/engine/topology.ts` (lines 814-903)

```typescript
async createPendingShards(tableName: string, newShardCount: number, changeLogId: string): Promise<TableShard[]> {
  // 1. Validate table exists
  // 2. Get current max shard_id
  const maxShardId = maxShardResult[0]?.max_id ?? -1;

  // 3. Create new pending shards, distributed across nodes
  for (let i = 0; i < newShardCount; i++) {
    const shardId = maxShardId + 1 + i;
    const nodeIndex = shardId % nodes.length;
    const nodeId = nodes[nodeIndex]!.node_id;

    this.ctx.storage.sql.exec(
      `INSERT INTO table_shards (table_name, shard_id, node_id, status, created_at, updated_at)
       VALUES (?, ?, ?, 'pending', ?, ?)`,
      tableName,
      shardId,
      nodeId,
      now,
      now
    );
  }

  // 4. Create resharding state
  this.ctx.storage.sql.exec(
    `DELETE FROM resharding_states WHERE table_name = ?`, tableName);
  // Allows retry after failure
  
  this.ctx.storage.sql.exec(
    `INSERT INTO resharding_states (table_name, source_shard_id, target_shard_ids, change_log_id, status, error_message, created_at, updated_at)
     VALUES (?, ?, ?, ?, 'pending_shards', NULL, ?, ?)`,
    tableName,
    sourceShardId,
    JSON.stringify(targetShardIds),
    changeLogId,
    now,
    now
  );

  return newShards;
}
```

**Key design patterns**:
- Pending shards are NOT excluded from normal table operations (no conductor filtering)
- They simply exist in topology but aren't used by queries until marked 'active'
- Initial resharding state status = 'pending_shards'
- Allows retry by deleting any previous failed resharding state first

### 3.3 Conductor Query Planning - Filters to Active Shards Only
**File**: `src/engine/topology.ts` (lines 359-362)

```typescript
const tableShards = this.ctx.storage.sql.exec(
  `SELECT * FROM table_shards WHERE table_name = ? AND status = 'active' ORDER BY shard_id`,
  tableName
).toArray() as unknown as TableShard[];
```

**Behavior**:
- Only 'active' shards are returned for query execution
- 'pending' shards created during resharding are NOT queried
- 'to_be_deleted' shards are NOT queried after atomic switch

---

## 4. Atomic Status Switch

**File**: `src/engine/topology.ts` (lines 971-1012)

```typescript
async atomicStatusSwitch(tableName: string): Promise<void> {
  // Get resharding state
  const reshardingState = await this.getReshardingState(tableName);
  if (!reshardingState) {
    throw new Error(`No resharding in progress for table '${tableName}'`);
  }

  const targetShardIds: number[] = JSON.parse(reshardingState.target_shard_ids);
  const sourceShardId = reshardingState.source_shard_id;
  const now = Date.now();

  // Note: Cloudflare Durable Objects automatically coalesces all SQL operations
  // within a single request into atomic writes

  // Mark all target shards as active
  for (const shardId of targetShardIds) {
    this.ctx.storage.sql.exec(
      `UPDATE table_shards SET status = 'active', updated_at = ? WHERE table_name = ? AND shard_id = ?`,
      now,
      tableName,
      shardId
    );
  }

  // Mark source shard as to_be_deleted
  this.ctx.storage.sql.exec(
    `UPDATE table_shards SET status = 'to_be_deleted', updated_at = ? WHERE table_name = ? AND shard_id = ?`,
    now,
    tableName,
    sourceShardId
  );

  // Update resharding state to 'verifying'
  this.ctx.storage.sql.exec(
    `UPDATE resharding_states SET status = 'verifying', updated_at = ? WHERE table_name = ?`,
    now,
    tableName
  );
}
```

**Key features**:
- All updates within single request are atomic on Cloudflare Durable Objects
- NO explicit BEGIN/COMMIT needed
- Conductor's getQueryPlanData() filters to 'active' status only
- After switch, queries automatically route to new shards
- Source shard marked 'to_be_deleted' but NOT queried anymore

---

## 5. Error Handling in Phase 3 Resharding Process

**File**: `src/queue-consumer.ts` (lines 393-480)

```typescript
async function processReshardTableJob(job: ReshardTableJob, env: Env, correlationId?: string): Promise<void> {
  const tableName = job.table_name;
  const sourceShardId = job.source_shard_id;
  const targetShardIds = job.target_shard_ids;

  logger.setTags({
    table: tableName,
    reshardingId: job.change_log_id,
  });

  const topologyId = env.TOPOLOGY.idFromName(job.database_id);
  const topologyStub = env.TOPOLOGY.get(topologyId);

  try {
    // Mark resharding as starting
    await topologyStub.startResharding(tableName);
    logger.info('Resharding started', { table: tableName, status: 'copying' });

    // Phase 3B: Copy data
    const copyStats = await copyShardData(
      env, job.database_id, sourceShardId, targetShardIds,
      tableName, job.shard_key, job.shard_strategy
    );
    logger.info('Phase 3B: Data copy completed', { ...copyStats });

    // Phase 3C: Replay captured changes
    const replayStats = await replayChangeLog(
      env, job.database_id, job.change_log_id, targetShardIds,
      tableName, job.shard_key, job.shard_strategy
    );
    logger.info('Phase 3C: Change log replay completed', { ...replayStats });

    // Phase 3D: Verify data integrity
    const verifyStats = await verifyIntegrity(
      env, job.database_id, sourceShardId, targetShardIds, tableName
    );
    logger.info('Phase 3D: Data verification completed', { ...verifyStats });

    // Check verification results
    if (!verifyStats.isValid) {
      throw new Error(`Data verification failed: ${verifyStats.error}`);
    }

    // Phase 4: Atomic status switch
    await topologyStub.atomicStatusSwitch(tableName);
    logger.info('Phase 4: Atomic status switch completed', { table: tableName });

    // Phase 5: Delete old shard data
    await deleteOldShardData(env, job.database_id, sourceShardId, tableName);
    logger.info('Phase 5: Old shard data deleted', { table: tableName, shardId: sourceShardId });

    // Mark resharding as complete
    await topologyStub.markReshardingComplete(tableName);
    logger.info('Resharding completed successfully', {
      table: tableName,
      status: 'complete',
    });

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Resharding failed', {
      table: tableName,
      error: errorMessage,
      status: 'failed',
    });
    // CLEANUP: Mark resharding as failed
    await topologyStub.markReshardingFailed(tableName, errorMessage);
    throw error;
  }
}
```

### 5.1 Phase 3B Error Handling: Copy Data

**File**: `src/queue-consumer.ts` (lines 486-609)

```typescript
async function copyShardData(
  env: Env,
  databaseId: string,
  sourceShardId: number,
  targetShardIds: number[],
  tableName: string,
  shardKey: string,
  shardStrategy: 'hash' | 'range'
): Promise<{ rows_copied: number; duration: number }> {
  const startTime = Date.now();
  let rowsCopied = 0;

  try {
    // Get source shard data
    const rows = (result as any).rows as Record<string, any>[];

    // Copy rows to target shards using shard key hash
    for (const row of rows) {
      const shardKeyValue = row[shardKey];
      if (shardKeyValue === undefined) {
        logger.warn('Row missing shard key', { shardKey, row });
        continue;
      }

      const targetShardIndex = hashToShard(shardKeyValue, targetShardIds.length);
      const targetShardId = targetShardIds[targetShardIndex];
      const targetNodeId = targetShardMap.get(targetShardId);

      if (!targetNodeId) {
        throw new Error(`Target shard ${targetShardId} node mapping not found`);
      }

      const targetStorageId = env.STORAGE.idFromName(targetNodeId);
      const targetStorageStub = env.STORAGE.get(targetStorageId);

      try {
        await targetStorageStub.executeQuery({
          query,
          params,
          queryType: 'INSERT',
        });
        rowsCopied++;
      } catch (error) {
        logger.error('Failed to copy row to target shard', {
          shardKeyValue,
          targetShardId,
          error: error instanceof Error ? error.message : String(error),
        });
        throw error; // Re-throw to propagate to processReshardTableJob
      }
    }

    return { rows_copied: rowsCopied, duration: Date.now() - startTime };
  } catch (error) {
    logger.error('Phase 3B: Data copy failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error; // Propagates to processReshardTableJob catch block
  }
}
```

**Error flow**:
1. Fails to copy a row → throws error
2. Error caught in Phase 3B catch block
3. Logged and re-thrown
4. Caught in processReshardTableJob catch block
5. markReshardingFailed() called

### 5.2 Phase 3C Error Handling: Replay Change Log

**File**: `src/queue-consumer.ts` (lines 616-646)

```typescript
async function replayChangeLog(
  env: Env,
  databaseId: string,
  changeLogId: string,
  targetShardIds: number[],
  tableName: string,
  shardKey: string,
  shardStrategy: 'hash' | 'range'
): Promise<{ changes_replayed: number; duration: number }> {
  const startTime = Date.now();
  let changesReplayed = 0;

  try {
    // In production, this would fetch change log entries from a persistent log
    logger.info('Phase 3C: No change log entries to replay (or would fetch from persistent log)');

    const duration = Date.now() - startTime;
    return { changes_replayed: changesReplayed, duration };

  } catch (error) {
    logger.error('Phase 3C: Change log replay failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
```

**Note**: Phase 3C is a stub in the current implementation but has error handling structure in place.

### 5.3 Phase 3D Error Handling: Verify Data Integrity

**File**: `src/queue-consumer.ts` (lines 651-730)

```typescript
async function verifyIntegrity(
  env: Env,
  databaseId: string,
  sourceShardId: number,
  targetShardIds: number[],
  tableName: string
): Promise<{ isValid: boolean; error?: string; duration: number }> {
  const startTime = Date.now();

  try {
    // Get source shard row count
    const sourceCount = (sourceCountResult as any).rows[0]?.count || 0;
    logger.debug('Source shard row count', { sourceShardId, count: sourceCount });

    // Get target shards total row count
    let targetTotalCount = 0;
    for (const targetShardId of targetShardIds) {
      const targetCount = (targetCountResult as any).rows[0]?.count || 0;
      targetTotalCount += targetCount;
      logger.debug('Target shard row count', { targetShardId, count: targetCount });
    }

    const duration = Date.now() - startTime;

    // Check if counts match
    if (sourceCount !== targetTotalCount) {
      const error = `Row count mismatch: source=${sourceCount}, targets=${targetTotalCount}`;
      logger.warn('Phase 3D: Data verification failed', { error, duration });
      return { isValid: false, error, duration };
    }

    logger.info('Phase 3D: Data verification passed', { rowCount: sourceCount, duration });
    return { isValid: true, duration };

  } catch (error) {
    logger.error('Phase 3D: Data verification error', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
```

**Error handling**:
- Database errors (query failures) → thrown and propagated
- Verification failures (count mismatch) → returned as `isValid: false` with error message
- processReshardTableJob checks `if (!verifyStats.isValid)` and throws error
- Then caught in catch block and markReshardingFailed() called

---

## 6. Cleanup Logic for Failed Resharding

### 6.1 What Happens When Resharding Fails

1. **Pending shards remain in topology** with status='pending'
   - NOT deleted automatically
   - Can be retried by calling createPendingShards() again (deletes old state first)

2. **Source shard remains ACTIVE**
   - No changes made to source shard status
   - Queries still work normally against source shard

3. **ReshardingState updated to 'failed'** with error message
   - Error message captures failure reason for investigation
   - Can be queried to understand why resharding failed

4. **Change log entries remain in queue** (for retries)
   - Changes captured during copying phase still available
   - Can be replayed when resharding retried

### 6.2 Manual Retry After Failure

From test case at lines 1837-1879:

```typescript
// First resharding attempt fails
await topologyStub.createPendingShards('users', 3, changeLogId1);
await topologyStub.startResharding('users');
await topologyStub.markReshardingFailed('users', 'Row count mismatch during verification');

// Retry resharding
const changeLogId2 = 'test-log-error-1b';
await topologyStub.createPendingShards('users', 3, changeLogId2);
await topologyStub.startResharding('users');

// New resharding attempt initiated with new changeLogId
reshardingState = await topologyStub.getReshardingState('users');
expect(reshardingState!.status).toBe('copying');
expect(reshardingState!.change_log_id).toBe(changeLogId2);
```

**Key insight**: 
- createPendingShards() DELETES previous failed resharding state first (line 889)
- Allows clean retry without manual cleanup
- Old pending shards are replaced with new ones

### 6.3 Phase 5: Delete Old Shard Data

**File**: `src/queue-consumer.ts` (lines 735-774)

```typescript
async function deleteOldShardData(
  env: Env,
  databaseId: string,
  sourceShardId: number,
  tableName: string
): Promise<void> {
  logger.info('Phase 5: Starting old shard deletion', {
    sourceShardId,
  });

  try {
    const topologyId = env.TOPOLOGY.idFromName(databaseId);
    const topologyStub = env.TOPOLOGY.get(topologyId);
    const topology = await topologyStub.getTopology();

    const sourceShard = topology.table_shards.find(
      s => s.table_name === tableName && s.shard_id === sourceShardId
    );
    if (!sourceShard) {
      throw new Error(`Source shard ${sourceShardId} not found`);
    }

    const storageId = env.STORAGE.idFromName(sourceShard.node_id);
    const storageStub = env.STORAGE.get(storageId);

    // Delete all data from source shard
    await storageStub.executeQuery({
      query: `DELETE FROM ${tableName}`,
      params: [],
      queryType: 'DELETE',
    });

    logger.info('Phase 5: Old shard data deleted', { sourceShardId });

  } catch (error) {
    logger.error('Phase 5: Old shard deletion failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
```

**Key points**:
- Only executed AFTER atomic status switch completes successfully
- Deletes ALL data from source shard
- If this fails, resharding is marked failed but data remains (safe)
- Source shard still marked 'to_be_deleted' - won't receive queries

---

## 7. Resharding State Machine / Phases

```
PHASE 1 (Conductor - synchronous):
  PRAGMA reshardTable('table', 3)
    ↓
  handleReshardTable() 
    ↓
  createPendingShards()
    ↓
  Status: 'pending_shards'
  ✓ Return job_id to user immediately

PHASE 2 (Conductor - synchronous):
  Enqueue ReshardTableJob to queue
  
PHASE 3 (Queue Worker - asynchronous):

  3A: Write Logging (Conductor during normal queries)
      - INSERT/UPDATE/DELETE operations logged to queue
      - Only if reshardingState.status === 'copying'

  3B: Copy Data
      queryStoreData() → getReshardingState() checks status='copying'
      startResharding() → status='pending_shards' → status='copying'
      
      copyShardData():
        ✓ Copy all rows from source → target shards by shard key hash
        ✗ FAIL → throws error → caught in processReshardTableJob
        
      Status: 'copying'

  3C: Replay Change Log
      replayChangeLog():
        ✓ Fetch changes from queue by change_log_id
        ✓ Replay to target shards in order
        ✗ FAIL → throws error → caught in processReshardTableJob
        
      Status: 'copying' (no status change in current code)

  3D: Verify Data Integrity
      verifyIntegrity():
        - Count rows in source shard
        - Count rows in all target shards
        ✓ Counts match → continue
        ✗ Counts mismatch → return isValid:false with error
        
      processReshardTableJob checks:
        if (!verifyStats.isValid) throw Error
        
      Status: 'copying' (no status change yet)

PHASE 4 (Queue Worker - atomic):
  atomicStatusSwitch():
    - Mark all target shards: 'pending' → 'active'
    - Mark source shard: 'active' → 'to_be_deleted'
    - Update resharding state: 'verifying'
    
  Status: 'verifying'

PHASE 5 (Queue Worker - cleanup):
  deleteOldShardData():
    ✓ DELETE FROM table WHERE shard_id = source_shard_id
    ✗ FAIL → throws error → caught in catch block

COMPLETION:
  markReshardingComplete()
    - Status: 'complete'

ERROR HANDLING (all phases):
  ✗ Any error → caught in processReshardTableJob
  → markReshardingFailed(tableName, errorMessage)
  → Status: 'failed'
  → Error message stored for investigation
  → Queue message retried based on attempts
  → Can manually retry with createPendingShards() again
```

---

## 8. Query Execution During Resharding

**File**: `src/engine/conductor.ts` (lines 187-192)

```typescript
// STEP 2.5: Write Logging - Log writes during resharding (Phase 3A)
if (statement.type === 'InsertStatement' || statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') {
  await this.logWriteIfResharding(tableName, statement.type, query, params, cid);
}
```

### Write Logging Implementation

**File**: `src/engine/conductor.ts` (lines 894-936)

```typescript
private async logWriteIfResharding(
  tableName: string,
  operationType: string,
  query: string,
  params: any[],
  correlationId?: string
): Promise<void> {
  // Get the current resharding state for this table
  const topologyId = this.topology.idFromName(this.databaseId);
  const topologyStub = this.topology.get(topologyId);
  const reshardingState = await topologyStub.getReshardingState(tableName);

  // Only log if resharding is active and in copying phase
  if (!reshardingState || reshardingState.status !== 'copying') {
    return;
  }

  // Log the write operation to the queue for later replay
  const operation = (operationType === 'InsertStatement' ? 'INSERT' :
                    operationType === 'UpdateStatement' ? 'UPDATE' : 'DELETE') as 'INSERT' | 'UPDATE' | 'DELETE';

  const changeLogEntry = {
    type: 'resharding_change_log' as const,
    resharding_id: reshardingState.change_log_id,
    database_id: this.databaseId,
    table_name: tableName,
    operation,
    query,
    params,
    timestamp: Date.now(),
    correlation_id: correlationId,
  };

  try {
    await this.enqueueIndexJob(changeLogEntry);
    logger.debug('Write operation logged for resharding', {
      table: tableName,
      operation,
      reshardingId: reshardingState.change_log_id,
    });
  } catch (error) {
    // Log but don't fail - write logging should not block query execution
    logger.warn('Failed to log write during resharding', {
      table: tableName,
      operation,
      error: (error as Error).message,
    });
  }
}
```

**Key behaviors**:
- **Queries still execute on source shard** during phases 3B-3C
- **Writes are logged to queue** for replay to target shards later
- **Query routing unchanged** - conductor still routes to source shard (which is still 'active')
- **Failure to log doesn't block query** - write proceeds normally even if logging fails
- **After atomic switch** - queries automatically route to target shards (source is 'to_be_deleted')

---

## 9. Summary Table: Error Handling by Phase

| Phase | Component | Error Type | Handling | Recovery |
|-------|-----------|-----------|----------|----------|
| 1 | Conductor | Table not found | Throw immediately | User retries |
| 1 | Conductor | Invalid shard count | Throw immediately | User retries |
| 3B | Queue Worker | Source shard not found | Throw → markFailed | Manual retry |
| 3B | Queue Worker | Row copy fails | Throw → markFailed | Manual retry |
| 3C | Queue Worker | Change log fetch fails | Throw → markFailed | Manual retry |
| 3D | Queue Worker | Count mismatch | Return isValid:false → throw | Manual retry |
| 3D | Queue Worker | Verification query fails | Throw → markFailed | Manual retry |
| 4 | Queue Worker | Atomic switch fails | N/A - shouldn't happen | Would mark failed |
| 5 | Queue Worker | Old shard delete fails | Throw → markFailed | Manual cleanup |
| All | Queue Worker | Unexpected error | Catch block → markFailed | Manual retry |

---

## 10. Key Design Decisions

1. **No Automatic Cleanup on Failure**
   - Pending shards remain for retry
   - Source shard remains active
   - Allows investigation and debugging

2. **Atomic Status Switch Only After Verification Passes**
   - Changes are all-or-nothing
   - No partial switches
   - Source shard never queried after switch

3. **Error Messages Stored in ReshardingState**
   - Error captured for debugging
   - Available via getReshardingState()
   - Helps operators understand failures

4. **Manual Retry Friendly**
   - createPendingShards() clears old state
   - No special cleanup commands needed
   - Just call PRAGMA reshardTable again

5. **Write Logging is Best-Effort**
   - Failures don't block user queries
   - But are logged for observability
   - Missing logs detected by row count mismatch

---

## 11. Testing Coverage

Comprehensive tests at `/home/thomas/Projects/databases/packages/big-daddy/test/engine/resharding.test.ts`:

- **Error scenarios** (lines 1836-2015):
  - Allow retry after failed resharding
  - Handle concurrent resharding attempts
  - Preserve data integrity if resharding fails
  - Handle resharding of empty tables

- **State transitions** (lines 619-1122):
  - Verify pending shard creation
  - Verify status progression through phases
  - Verify atomic status switch

- **End-to-end workflows** (lines 1622-1831):
  - Complete resharding workflow
  - Maintain data consistency
  - Support multiple independent resharding operations

