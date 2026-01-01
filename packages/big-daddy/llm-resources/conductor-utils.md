# Conductor Utilities

## helpers.ts - Core Utilities

### `injectVirtualShard(statement, params, shardId)`
Modifies statements to include shard isolation:
- **INSERT**: Adds `_virtualShard` column with placeholder per row
- **SELECT/UPDATE/DELETE**: Adds `WHERE _virtualShard = ?` filter

### `injectVirtualShardColumn(statement: CreateTableStatement)`
For CREATE TABLE:
- Adds `_virtualShard INTEGER NOT NULL DEFAULT 0` as last column
- Converts single-column PK to composite: `(_virtualShard, original_pk)`
- Returns modified SQL string

### `mergeResultsSimple(results, statement)`
Merges shard results:
- **SELECT with aggregations**: Merges COUNT/SUM/MIN/MAX; AVG is an approximation (avg of avgs)
- **Regular SELECT**: Concatenates rows, strips `_virtualShard`
- **INSERT**: Returns `VALUES.length` (not SQLite's inflated count)
- **UPDATE/DELETE**: Sums `rowsAffected`

### `hashToShardId(value, numShards)`
Consistent hash function for shard routing:
```typescript
hash = (hash << 5) - hash + charCode
return Math.abs(hash) % numShards
```

---

## write.ts - Shard Execution

### `executeOnShards(context, shards, statements, params)`
- Batches in groups of 7 (Cloudflare limit)
- Injects virtual shard filter
- Parallel execution within batch, sequential between batches
- Returns `{ results, shardStats }`

**Overloads support:**
- Single statement + params
- Statement array + shared params
- `StatementWithParams[]` (each statement has own params)

### `getCachedQueryPlanData(context, tableName, statement, params)`
1. Build cache key from table + WHERE structure
2. Check TopologyCache
3. On miss: call Topology DO, cache result
4. Returns `{ planData, cacheHit }`

### `invalidateCacheForWrite(context, tableName, statement, indexes, params)`
- Always invalidates table cache
- INSERT: Invalidates specific index key values
- UPDATE: Invalidates entire affected indexes (can't determine old values)
- DELETE: Invalidates entire affected indexes

### `logWriteIfResharding(tableName, opType, query, params, context)`
During active resharding (status='copying'):
- Logs write to queue for later replay on new shards

### `enqueueIndexMaintenanceJob(context, tableName, statement, shardIds, indexes)`
Enqueues async job for UPDATE/DELETE index maintenance.

---

## index-maintenance.ts - Index Sync

### `prepareIndexMaintenanceQueries(hasIndexes, mainStmt, selectStmt, params)`
Returns `StatementWithParams[]`:
- No indexes: `[mainStatement]`
- INSERT: `[INSERT]`
- DELETE: `[SELECT, DELETE]`
- UPDATE: `[SELECT before, UPDATE, SELECT after]`

### `dispatchIndexSyncingFromQueryResults(opType, results, table, shards, indexes, context, extractor)`
1. Extract old/new rows via `rowDataExtractor` callback
2. Generate events based on operation type
3. Queue `IndexMaintenanceEventJob`

### Event Generation
- `generateInsertIndexEvents()` → `add` events
- `generateDeleteIndexEvents()` → `remove` events
- `generateUpdateIndexEvents()` → diff-based `add`/`remove`

**Deduplication:** Per-shard via Map key `{index}:{value}:{shard}`

---

## utils.ts - Small Helpers

### `extractKeyValueFromRow(columns, row, indexColumns, params)`
Builds index key from INSERT row for cache invalidation.
- Single column: `String(value)`
- Composite: `JSON.stringify(values)`
- Returns `null` if any value is NULL
