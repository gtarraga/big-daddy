# Table Operations Guide

This document describes the comprehensive table operations API available in the conductor. All operations are exposed through SQL statements and a programmatic API.

## Overview

The conductor now supports full CRUD operations on tables:

- **CREATE** - Already existed, now fully integrated
- **GET** - List and describe tables (new)
- **UPDATE** - Alter table properties, reshard (new)
- **DELETE** - Drop tables (new)

Plus comprehensive index management and status monitoring.

## Architecture

### Module Structure

```
src/engine/conductor/
├── table-crud.ts         # CREATE TABLE, DROP TABLE
├── describe-table.ts     # SHOW TABLES, DESCRIBE TABLE, TABLE STATS
├── alter-table.ts        # ALTER TABLE, RESHARD
├── drop-index.ts         # DROP INDEX, SHOW INDEXES
├── table-api.ts          # High-level TableOperationsAPI
└── index.ts              # Main router
```

### Handler Context

All table operations receive a `QueryHandlerContext`:

```typescript
interface QueryHandlerContext {
  databaseId: string;           // Database ID
  correlationId: string;        // Request tracking
  storage: DurableObjectNamespace<Storage>;
  topology: DurableObjectNamespace<Topology>;
  indexQueue?: Queue;           // For async jobs
  env?: Env;                    // Test environment only
  cache: TopologyCache;         // Query plan cache
}
```

## SQL Operations

### CREATE TABLE

Create a new table with automatic sharding:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE
) /* SHARDS=4 */;
```

Features:
- Automatically creates composite primary key with `_virtualShard`
- Creates specified number of shards (default: 1)
- Distributes shards round-robin across nodes
- Executes CREATE TABLE on all storage nodes in parallel

### DROP TABLE

Remove a table from the cluster:

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users;  -- Silently succeeds if not exists
```

Behavior:
- Validates table exists
- Prevents dropping during active resharding
- Cascades to delete all shards and indexes
- Executes DROP TABLE on all affected storage nodes

### SHOW TABLES

List all tables in the database:

```sql
SHOW TABLES;
```

Returns:
- `name` - Table name
- `primary_key` - Primary key column(s)
- `shard_key` - Column used for sharding
- `num_shards` - Number of shards
- `shard_strategy` - 'hash' or 'range'
- `created_at` - Creation timestamp

### DESCRIBE TABLE

Get detailed information about a table:

```sql
DESCRIBE TABLE users;
DESCRIBE users;  -- Alternative syntax
```

Returns sections:
1. **TABLE_INFO** - Metadata (pk, shards, strategy)
2. **SHARDS** - Shard distribution across nodes
3. **INDEXES** - All virtual indexes and their status
4. **COLUMNS** - Column definitions from schema

### TABLE STATS

Get table statistics:

```sql
SELECT * FROM TABLE_STATS('users');
```

Returns:
- `total_rows` - Total row count across all shards
- `total_shards` - Active shard count
- `avg_rows_per_shard` - Average distribution
- `shard_stats` - Per-shard row counts and health

### ALTER TABLE

Modify table properties (limited):

```sql
-- Rename table (note: complex operation, use with caution)
ALTER TABLE users RENAME TO customers;

-- Change block size (rows loaded per disk access)
ALTER TABLE users MODIFY BLOCK_SIZE 1000;
```

Supported operations:
- `RENAME TO` - Rename table
- `MODIFY BLOCK_SIZE` - Change rows per block (1-10000)

Not supported (would require migration):
- ADD/DROP COLUMN
- Change data types
- Modify primary key

### PRAGMA RESHARD

Initiate resharding to change shard count:

```sql
PRAGMA reshardTable('users', 8);
```

This initiates an async resharding operation:
1. Creates new pending shards
2. Enqueues background jobs to copy data
3. Atomic switch to new shards
4. Cleanup of old shards

Returns:
- `change_log_id` - Unique ID to track resharding
- `status` - 'queued'
- `message` - Human-readable status
- `source_shard_count`, `target_shard_count` - Transition info

### INDEX OPERATIONS

Create indexes (already existed):

```sql
CREATE INDEX idx_email ON users(email);
CREATE UNIQUE INDEX idx_email_unique ON users(email);
CREATE INDEX idx_country_city ON users(country, city);
```

Drop indexes (new):

```sql
DROP INDEX idx_email;
DROP INDEX IF EXISTS idx_email;
```

Show indexes on a table:

```sql
SHOW INDEXES FROM users;
```

Returns:
- `index_name` - Name of the index
- `columns` - Array of indexed column names
- `index_type` - 'hash' or 'unique'
- `status` - 'building', 'ready', or 'failed'

## Programmatic API

### TableOperationsAPI

High-level API for table operations:

```typescript
import { createTableOperationsAPI } from './conductor/table-api';

const tableOps = createTableOperationsAPI(context);

// List all tables
const tables = await tableOps.listTables();

// Get table info
const info = await tableOps.describe('users');

// Get statistics
const stats = await tableOps.getStats('users');

// Drop table
await tableOps.drop('users', true);  // IF EXISTS

// Rename table
await tableOps.rename('users', 'customers');

// Change block size
await tableOps.setBlockSize('users', 1000);

// Reshard table
const result = await tableOps.reshard('users', 8);
console.log(result.rows[0].change_log_id);  // Track progress

// Index operations
const indexes = await tableOps.listIndexes('users');
await tableOps.dropIndex('idx_email', 'users', true);
```

## Implementation Details

### Error Handling

All operations validate:
- Table existence
- Active resharding status
- Index status
- Node connectivity

Errors are descriptive:
```
Table 'users' not found
Index 'idx_email' already exists
Cannot DROP table 'users' while resharding is in progress
```

### Transaction Safety

Operations are safe:
- **CREATE** - Topology updated before storage nodes
- **DROP** - Topology updated, then cascaded to all nodes
- **ALTER** - Atomic topology update
- **INDEX** - Async background job for building

### Performance

- **SHOW TABLES** - O(1) metadata fetch (cached)
- **DESCRIBE** - Fetches schema from one shard + topology
- **TABLE STATS** - Parallel COUNT(*) across all shards
- **DROP** - Parallel DROP across nodes
- **ALTER** - Single atomic topology update

### Resharding

The resharding operation is async and multi-phase:

1. **Pending Phase** - Create target shards, set status to 'pending'
2. **Copying Phase** - Background jobs copy data from source to targets
3. **Switching Phase** - Atomic swap of shard status
4. **Cleanup Phase** - Delete old shards and free resources

Track progress via:
```typescript
const topology = await topologyStub.getTopology();
const resharding = topology.resharding_states.find(
  s => s.change_log_id === changeLogId
);
console.log(resharding.status);  // 'queued', 'copying', 'switching', 'complete'
```

## Limitations & Future Work

Current limitations:
- Column alterations not supported (add/drop/modify columns)
- Cannot change primary key after creation
- Rename operation is simplified (doesn't fully update shard mappings)
- No column-level constraints beyond primary key

Future enhancements:
- Full table migration for ALTER TABLE ADD/DROP COLUMN
- Schema versioning
- Index rebuilding on demand
- Automatic shard balancing
- Table-level statistics (rows, size, last modified)

## Examples

### Complete Table Lifecycle

```typescript
const sql = await createConnection('mydb', { nodes: 4 }, env);

// 1. Create table with 4 shards
await sql`CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  name TEXT,
  price REAL
) /* SHARDS=4 */`;

// 2. Create indexes
await sql`CREATE INDEX idx_name ON products(name)`;

// 3. Insert data
for (let i = 0; i < 1000; i++) {
  await sql`INSERT INTO products VALUES (${i}, ${'Product ' + i}, ${Math.random() * 100})`;
}

// 4. Check statistics
const tableOps = createTableOperationsAPI(context);
const stats = await tableOps.getStats('products');
console.log(`Total products: ${stats.rows[0].total_rows}`);

// 5. Reshard if needed
if (stats.rows[0].total_rows > 10000) {
  const reshard = await tableOps.reshard('products', 8);
  console.log(`Resharding with ID: ${reshard.rows[0].change_log_id}`);
}

// 6. Cleanup when done
await tableOps.dropIndex('idx_name');
await tableOps.drop('products');
```
