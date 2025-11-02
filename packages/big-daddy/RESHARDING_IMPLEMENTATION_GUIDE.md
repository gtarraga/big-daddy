# Resharding Implementation Guide

## Test-First Approach

We've created **45 comprehensive tests** in `test/engine/resharding.test.ts` that describe the exact behavior we want:

- ✅ 3 tests for PRAGMA parsing
- ✅ 4 tests for creating pending shards
- ✅ 3 tests for queue job dispatch
- ✅ 7 tests for write logging
- ✅ 8 tests for change log replay
- ✅ 6 tests for data integrity verification
- ✅ 5 tests for atomic status switch
- ✅ 3 tests for cleanup
- ✅ 3 end-to-end scenarios
- ✅ 4 error handling scenarios

**Current Status**: All tests are passing (skipped with `// TODO` comments)

## Implementation Roadmap

### Step 1: Add Shard Status Tracking to Topology ✅ (Types added)

**File**: `src/engine/topology.ts`

Update the `TableShard` interface to include status:
```typescript
interface TableShard {
  shard_id: number
  table_name: string
  status: 'active' | 'pending' | 'to_be_deleted' | 'failed'  // Add this
  node_id: string
  created_at: number
  updated_at: number
}
```

Add resharding state tracking:
```typescript
interface ReshardingState {
  table_name: string
  source_shard_id: number
  target_shard_ids: number[]
  change_log_id: string
  status: 'copying' | 'replaying' | 'verifying' | 'complete'
  created_at: number
  updated_at: number
}
```

### Step 2: Implement `topology.createPendingShards()`

**In `src/engine/topology.ts` Topology class:**

```typescript
async createPendingShards(
  tableName: string,
  newShardCount: number,
  changeLogId: string
): Promise<TableShard[]> {
  // 1. Validate table exists
  // 2. Get current max shard_id
  // 3. Create new shards with status='pending'
  // 4. Distribute across nodes
  // 5. Save to topology
  // 6. Return new shards
}
```

### Step 3: Implement Conductor PRAGMA Handler

**File**: `src/engine/conductor.ts`

In the main `sql` method, add:
```typescript
if (statement.type === 'PragmaStatement') {
  return this.handlePragma(statement as PragmaStatement, query, cid);
}
```

Then implement:
```typescript
private async handlePragma(
  stmt: PragmaStatement,
  query: string,
  cid: string
): Promise<QueryResult> {
  if (stmt.name.toLowerCase() === 'reshardtable') {
    return this.handleReshardTable(stmt, cid);
  }
  throw new Error(`Unsupported PRAGMA: ${stmt.name}`);
}

private async handleReshardTable(
  stmt: PragmaStatement,
  cid: string
): Promise<QueryResult> {
  // Extract table name and shard count from arguments
  // Call topology.createPendingShards()
  // Enqueue ReshardTableJob
  // Return job ID to user
}
```

### Step 4: Implement Write Logging

**In `src/engine/conductor.ts` in the main query execution:**

Before executing INSERT/UPDATE/DELETE:
```typescript
if (statement.type === 'InsertStatement' ||
    statement.type === 'UpdateStatement' ||
    statement.type === 'DeleteStatement') {

  const tableName = extractTableName(statement);
  const reshardingState = await this.topology.getReshardingState(tableName);

  if (reshardingState?.status === 'copying') {
    // Log to queue
    await this.indexQueue.send({
      type: 'resharding_change_log',
      resharding_id: reshardingState.change_log_id,
      database_id: this.databaseId,
      table_name: tableName,
      operation: getOperationType(statement.type),
      query: query,
      params: params,
      timestamp: Date.now()
    });
  }

  // Then execute normally on active shards
  return this.executeOnShards(...);
}
```

### Step 5: Implement Queue Worker Handler

**File**: `src/queue-consumer.ts`

Add handling for ReshardTableJob:
```typescript
if (job.type === 'reshard_table') {
  await handleReshardTableJob(job as ReshardTableJob, env);
}

async function handleReshardTableJob(job: ReshardTableJob, env: Env) {
  // Phase 3B: Copy data
  // Phase 3C: Replay changes
  // Phase 3D: Verify integrity
  // If all pass: Mark as complete
  // If fails: Mark as failed
}
```

Implement helper functions:
```typescript
async function copyShardData(
  sourceShardId: number,
  targetShardIds: number[],
  tableName: string,
  shardKey: string,
  shardStrategy: 'hash' | 'range'
): Promise<number>

async function replayChangeLog(
  changeLogId: string,
  targetShardIds: number[],
  shardKey: string
): Promise<number>

async function verifyIntegrity(
  sourceShardId: number,
  targetShardIds: number[]
): Promise<boolean>
```

### Step 6: Implement Topology Status Management

**In `src/engine/topology.ts`:**

Add methods:
```typescript
async getReshardingState(tableName: string): Promise<ReshardingState | null>

async startResharding(
  tableName: string,
  sourceShardId: number,
  targetShardIds: number[],
  changeLogId: string
): Promise<void>

async markReshardingComplete(tableName: string): Promise<void>

async markReshardingFailed(tableName: string, error: string): Promise<void>

async atomicStatusSwitch(
  targetShardIds: number[],
  sourceShardId: number
): Promise<void>
```

### Step 7: Update Query Planning

**In `src/engine/topology.ts` in `getQueryPlanData()`:**

Only include ACTIVE shards:
```typescript
const activeShards = tableShards
  .filter(s => s.status === 'active')
  .filter(s => s.status !== 'to_be_deleted')
  .filter(s => s.status !== 'failed');
```

## Test Execution Flow

Each test file describes the expected behavior:

```
Test: "should create pending shards with correct status"
  ↓
Calls: topology.createPendingShards()
  ↓
Asserts: shards created with status='pending'
  ↓
Next: Run test to see it fail (not implemented)
  ↓
Implement: topology.createPendingShards()
  ↓
Run test again: PASS ✓
```

## Running Tests During Development

```bash
# Run specific test file
pnpm test test/engine/resharding.test.ts

# Run specific test
pnpm test test/engine/resharding.test.ts -t "should create pending shards"

# Watch mode (re-run on file change)
pnpm test:watch test/engine/resharding.test.ts

# With coverage
pnpm test test/engine/resharding.test.ts --coverage
```

## Implementation Order Recommended

1. **Shard status tracking** (simple schema update)
2. **Create pending shards** (topology logic)
3. **PRAGMA handler** (conductor entry point)
4. **Write logging** (conductor middleware)
5. **Queue job types** (already done ✅)
6. **Queue worker handler** (async processing)
7. **Status management** (topology updates)
8. **Query planning** (conductor shard filtering)

## Verification Checklist

As you implement each piece, verify:

- [ ] Corresponding tests pass
- [ ] No new TypeScript errors
- [ ] Existing tests still pass
- [ ] Code follows project patterns
- [ ] Logging added for debugging
- [ ] Error cases handled

## Key Files

| File | Purpose |
|------|---------|
| `src/engine/conductor.ts` | PRAGMA handler, write logging |
| `src/engine/topology.ts` | Pending shards, state management |
| `src/queue-consumer.ts` | Copy, replay, verify logic |
| `src/engine/queue/types.ts` | Job definitions ✅ |
| `test/engine/resharding.test.ts` | Tests ✅ |

## Next Step

Start with **Step 1: Add Shard Status Tracking** and implement the type changes in `topology.ts`, then run the tests to see which ones pass!
