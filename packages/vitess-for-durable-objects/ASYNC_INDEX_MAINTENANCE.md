# Async Index Maintenance Architecture

## Overview

Move UPDATE/DELETE index maintenance to be fully asynchronous via Cloudflare Queue to eliminate latency from the write path.

## Current Problems

**UPDATE operations:**
- Pre-fetch old values with SELECT query (extra shard queries - slow!)
- Maintain indexes synchronously (blocking, multiple Topology calls)
- Adds significant latency to write path

**DELETE operations:**
- Currently using RETURNING clause (modifies user's query)
- Changes the contract - user didn't ask for returned data
- Performance impact - builds result sets unnecessarily
- Memory concerns for large deletes

**Goal:** Don't modify user queries, don't block writes on index maintenance

## New Architecture: Fully Async via Queue

### Write Path (Fast, No Blocking)

1. User executes UPDATE or DELETE
2. Conductor executes query on affected shards (NO modifications, NO pre-fetching)
3. **Enqueue maintenance message** to INDEX_QUEUE with metadata
4. **Return success to user immediately**
5. Queue processes maintenance in background

### Queue Processing (Async Rebuild)

**Key Insight:** Instead of tracking what changed, we **rebuild the truth** by scanning affected shards.

#### For UPDATE

**Queue Message:**
```typescript
{
  type: 'maintain_index',
  operation: 'UPDATE',
  database_id: 'my-db',
  table_name: 'users',
  shard_ids: [0, 2, 5],              // Which shards were affected
  updated_columns: ['email', 'name'], // Which columns changed
  affected_indexes: ['idx_email']     // Indexes covering updated columns
}
```

**Processing Steps:**
1. For each affected shard (in parallel):
   - Read ALL rows on that shard for indexed columns: `SELECT id, email FROM users`
   - Build map: `{email_value => should_include_this_shard}`
2. For each affected index:
   - Get current index state from Topology
   - Compare: "These values should include this shard" vs "current state"
   - Compute changes: add shard where missing, remove shard where shouldn't exist
3. **Single Topology call** with all batched changes

#### For DELETE

**Queue Message:**
```typescript
{
  type: 'maintain_index',
  operation: 'DELETE',
  database_id: 'my-db',
  table_name: 'users',
  shard_ids: [0, 2, 5],           // Which shards were affected
  affected_indexes: ['idx_email', 'idx_country'] // All indexes on table
}
```

**Processing Steps:**
1. For each affected shard (in parallel):
   - Read ALL remaining rows: `SELECT id, email, country FROM users`
   - Build map: "These values SHOULD exist on this shard" (current truth)
2. For each index:
   - Get current entries from Topology that include this shard
   - Compare: "Should exist" vs "Currently exists"
   - Clean up: Remove shard from entries where data no longer exists
   - Ensure: Shard is in entries where data still exists
3. **Single Topology call** with all batched changes

**Why this works:** After DELETE, we scan what's LEFT. Index entries that previously included this shard but no longer have matching data will be cleaned up.

## Implementation Steps

### 1. Define Queue Job Type

```typescript
// src/Queue/types.ts
interface IndexMaintenanceJob {
  type: 'maintain_index';
  database_id: string;
  table_name: string;
  operation: 'UPDATE' | 'DELETE';
  shard_ids: number[];                    // Affected shards
  affected_indexes: string[];             // Index names that need updating
  updated_columns?: string[];             // For UPDATE: which columns changed
  created_at: string;
}

export type IndexJob = IndexBuildJob | IndexMaintenanceJob;
```

### 2. Update Conductor

**Remove:**
- `fetchOldValuesForUpdate()` - no more pre-fetching!
- `prepareQueryForIndexMaintenance()` - no more RETURNING!
- `maintainIndexes()` and all `maintainIndexesFor*()` methods
- All the complex synchronous index maintenance code

**Add:**
```typescript
// After executing UPDATE/DELETE
if ((statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement')
    && planData.virtualIndexes.length > 0) {

  await this.enqueueIndexMaintenanceJob(
    tableName,
    statement,
    shardsToQuery.map(s => s.shard_id),
    planData.virtualIndexes
  );
}

private async enqueueIndexMaintenanceJob(
  tableName: string,
  statement: UpdateStatement | DeleteStatement,
  shardIds: number[],
  virtualIndexes: Array<{index_name: string; columns: string}>
): Promise<void> {

  const updatedColumns = statement.type === 'UpdateStatement'
    ? statement.set.map(s => s.column.name)
    : undefined;

  // Determine which indexes are affected
  const affectedIndexes = updatedColumns
    ? virtualIndexes.filter(idx => {
        const indexCols = JSON.parse(idx.columns);
        return indexCols.some(col => updatedColumns.includes(col));
      })
    : virtualIndexes; // DELETE affects all indexes

  if (affectedIndexes.length === 0) return;

  const job: IndexMaintenanceJob = {
    type: 'maintain_index',
    database_id: this.databaseId,
    table_name: tableName,
    operation: statement.type === 'UpdateStatement' ? 'UPDATE' : 'DELETE',
    shard_ids: shardIds,
    affected_indexes: affectedIndexes.map(idx => idx.index_name),
    updated_columns: updatedColumns,
    created_at: new Date().toISOString()
  };

  await this.enqueueIndexJob(job);
}
```

### 3. Queue Consumer Processing

```typescript
// src/queue-consumer.ts

async function processIndexMaintenanceJob(
  job: IndexMaintenanceJob,
  env: Env
): Promise<void> {
  console.log(`Maintaining indexes for ${job.operation} on ${job.table_name}`);

  const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(job.database_id));
  const topology = await topologyStub.getTopology();

  // Get index definitions
  const indexes = topology.virtual_indexes.filter(
    idx => job.affected_indexes.includes(idx.index_name) && idx.status === 'ready'
  );

  if (indexes.length === 0) return;

  // Collect all index changes across all shards
  const allChanges: IndexChange[] = [];

  // Process each affected shard
  for (const shardId of job.shard_ids) {
    const shard = topology.table_shards.find(
      s => s.table_name === job.table_name && s.shard_id === shardId
    );
    if (!shard) continue;

    const storageStub = env.STORAGE.get(env.STORAGE.idFromName(shard.node_id));

    // For each index, rebuild entries for this shard
    for (const index of indexes) {
      const indexColumns = JSON.parse(index.columns);

      // Read current state from shard
      const columnList = indexColumns.join(', ');
      const query = `SELECT ${columnList} FROM ${job.table_name}`;
      const result = await storageStub.executeQuery({
        query,
        params: [],
        queryType: 'SELECT'
      });

      // Build set of values that SHOULD include this shard
      const shouldExist = new Set<string>();
      for (const row of result.rows) {
        const keyValue = computeIndexKey(row, indexColumns);
        if (keyValue) {
          shouldExist.add(keyValue);
        }
      }

      // Get current state from Topology
      const currentEntries = await topologyStub.getIndexEntriesForShard(
        index.index_name,
        shardId
      );
      const currentlyExists = new Set(currentEntries.map(e => e.key_value));

      // Compute changes
      // Add shard to entries where it should exist but doesn't
      for (const keyValue of shouldExist) {
        if (!currentlyExists.has(keyValue)) {
          allChanges.push({
            operation: 'add',
            index_name: index.index_name,
            key_value: keyValue,
            shard_id: shardId
          });
        }
      }

      // Remove shard from entries where it shouldn't exist but does
      for (const keyValue of currentlyExists) {
        if (!shouldExist.has(keyValue)) {
          allChanges.push({
            operation: 'remove',
            index_name: index.index_name,
            key_value: keyValue,
            shard_id: shardId
          });
        }
      }
    }
  }

  // Apply all changes in a single Topology call
  if (allChanges.length > 0) {
    await topologyStub.batchMaintainIndexes(allChanges);
    console.log(`Applied ${allChanges.length} index changes`);
  }
}

function computeIndexKey(row: Record<string, any>, columns: string[]): string | null {
  const values = [];
  for (const col of columns) {
    const val = row[col];
    if (val === null || val === undefined) {
      return null; // Don't index NULL values
    }
    values.push(val);
  }
  return columns.length === 1 ? String(values[0]) : JSON.stringify(values);
}
```

### 4. Add Topology Methods

```typescript
// Topology.ts

interface IndexChange {
  operation: 'add' | 'remove';
  index_name: string;
  key_value: string;
  shard_id: number;
}

/**
 * Get all index entries that include a specific shard
 */
async getIndexEntriesForShard(indexName: string, shardId: number): Promise<Array<{key_value: string}>> {
  this.ensureCreated();

  const entries = this.ctx.storage.sql.exec(
    `SELECT key_value, shard_ids FROM virtual_index_entries
     WHERE index_name = ?`,
    indexName
  ).toArray() as Array<{key_value: string; shard_ids: string}>;

  return entries.filter(entry => {
    const shardIds = JSON.parse(entry.shard_ids);
    return shardIds.includes(shardId);
  });
}

/**
 * Apply a batch of index changes in a single call
 */
async batchMaintainIndexes(changes: IndexChange[]): Promise<void> {
  this.ensureCreated();

  for (const change of changes) {
    if (change.operation === 'add') {
      await this.addShardToIndexEntry(change.index_name, change.key_value, change.shard_id);
    } else {
      await this.removeShardFromIndexEntry(change.index_name, change.key_value, change.shard_id);
    }
  }
}
```

### 5. Update Queue Consumer Router

```typescript
// src/queue-consumer.ts

async function processIndexJob(message: Message<IndexJob>, env: Env): Promise<void> {
  const job = message.body;

  if (job.type === 'build_index') {
    await processBuildIndexJob(job, env);
  } else if (job.type === 'maintain_index') {
    await processIndexMaintenanceJob(job, env);
  }
}
```

## Benefits

### Performance
- **No pre-fetch queries** - UPDATE/DELETE execute immediately
- **No blocking** - Index maintenance happens async
- **Batched Topology calls** - Single call per maintenance job instead of one per row
- **Write latency improvement**: Estimated 50-100ms reduction for indexed tables

### Correctness
- **Don't modify user queries** - No RETURNING injection
- **Eventually consistent** - Indexes catch up within seconds (queue latency)
- **Self-healing** - Rebuild from source of truth (shard data), handles edge cases

### Simplicity
- **Less code in Conductor** - Remove complex maintenance logic
- **Stateless processing** - Queue consumer rebuilds truth from current state
- **No coordination needed** - Each shard processed independently

## Trade-offs

### Eventual Consistency
- Index updates lag behind writes by queue processing time (~1-5 seconds typical)
- During lag, queries might hit extra shards (still correct, just slower)
- Acceptable for most use cases where exact routing optimization isn't critical immediately

### Queue Processing Cost
- Every UPDATE/DELETE enqueues a job
- Jobs need to scan affected shards
- For tables with many indexes, more work per job

### Mitigation
- Queue batching: Process multiple jobs together
- Smart scheduling: Delay processing slightly to batch updates to same table
- Monitoring: Track queue depth and processing time

## Migration Path

1. Implement new queue job type and processor
2. Add Topology methods for batch operations
3. Update Conductor to enqueue instead of maintaining
4. Deploy and test with feature flag
5. Monitor queue processing times
6. Remove old synchronous maintenance code

## Future Optimizations

1. **Smart batching**: Combine multiple maintenance jobs for same table
2. **Incremental updates**: Track change logs to avoid full shard scans
3. **Periodic reconciliation**: Background job to ensure index consistency
4. **Priority queue**: Prioritize maintenance for frequently queried tables
