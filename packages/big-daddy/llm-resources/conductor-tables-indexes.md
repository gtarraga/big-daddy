# Conductor Table & Index Operations

## CREATE TABLE (`tables/create-drop.ts`)
**Flow:**
1. Fetch topology, check if table exists
2. Handle `IF NOT EXISTS` gracefully
3. Extract metadata via `extractTableMetadata()`
4. Add table to topology
5. `injectVirtualShardColumn()` - adds `_virtualShard INTEGER NOT NULL DEFAULT 0` + composite PK
6. Execute CREATE on all storage nodes in parallel

**Key:** Uses Effect for typed error handling (`TableAlreadyExistsError`, `TopologyFetchError`, etc.)

---

## DROP TABLE (`tables/create-drop.ts`)
**Flow:**
1. Validate table exists
2. Check for active resharding (blocks drop)
3. Remove from topology (cascades to shards/indexes)
4. Execute DROP on all affected storage nodes

---

## DESCRIBE TABLE (`tables/describe.ts`)
Returns 4 sections:
- `TABLE_INFO` - metadata (pk, shard_key, strategy, num_shards)
- `SHARDS` - shard distribution with node_ids
- `INDEXES` - virtual indexes and status
- `COLUMNS` - schema from `PRAGMA table_info`

---

## TABLE STATS (`tables/describe.ts`)
- Parallel `COUNT(*)` across all shards
- Returns total_rows, avg_rows_per_shard, per-shard stats

---

## ALTER TABLE (`tables/alter.ts`)
Supported:
- `RENAME TO` - renames table in topology
- `MODIFY BLOCK_SIZE` - changes block_size (1-10000)

**Not supported:** ADD/DROP COLUMN, change data types, modify PK

---

## RESHARD TABLE (`tables/alter.ts`, `pragmas/pragma.ts`)
**Trigger:** `PRAGMA reshardTable('table', newCount)`

**Flow:**
1. Validate shard count (1-256)
2. Check no active resharding
3. `createPendingShards()` - creates new shards with 'pending' status
4. Enqueue `ReshardTableJob` per source shard
5. Return `change_log_id` for tracking

**Phases:** pending → copying → switching → complete

---

## CREATE INDEX (`indexes/create.ts`)
**Flow:**
1. Get table topology info
2. Build CREATE INDEX SQL
3. Execute on all storage shards (SQLite indexes for local perf)
4. Create virtual index in Topology with status 'building'
5. Enqueue `IndexBuildJob` for async population

**Test Environment:** Immediately processes queue via `queueHandler` import

---

## DROP INDEX (`indexes/drop.ts`)
**Flow:**
1. Validate index exists
2. Remove from topology
3. Execute DROP INDEX on all storage nodes

---

## SHOW INDEXES
Returns per-table index info: name, columns, type, status

---

## TableOperationsAPI (`tables/api.ts`)
High-level programmatic interface:
```typescript
const tableOps = createTableOperationsAPI(context);
await tableOps.listTables();
await tableOps.describe('users');
await tableOps.getStats('users');
await tableOps.drop('users', ifExists);
await tableOps.rename('old', 'new');
await tableOps.reshard('users', 8);
await tableOps.listIndexes('users');
await tableOps.dropIndex('idx_name', 'users', ifExists);
```
