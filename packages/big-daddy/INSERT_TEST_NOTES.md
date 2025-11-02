# Testing INSERT and Resharding

## Understanding _virtualShard

Every row in the database needs a `_virtualShard` column that indicates which virtual shard it belongs to. This is critical for:
- Shard isolation during queries (WHERE _virtualShard = ?)
- Resharding verification (ensuring all data is copied correctly)

## The Issue You Hit

When you manually inserted 100 rows using SQL:
```sql
INSERT INTO users (id, email) VALUES (1, 'user1@example.com'), ...
```

The `_virtualShard` column was **not specified**, so it defaulted to 0 for all rows.

Then during resharding:
- **Source shard verification**: `SELECT COUNT(*) FROM users WHERE _virtualShard = sourceShardId`
  - This only counted rows where _virtualShard matched the source shard ID
  - Result: 70 rows (likely because shard IDs are distributed hash-based)
- **Target shards**: All 100 rows were copied over
  - Result: 100 rows total across targets
- **Verification failed**: 70 ≠ 100

## Correct Way to Test

Insert data **through the conductor**, not directly with SQL. The conductor will:
1. Parse INSERT queries
2. Automatically set `_virtualShard` to the correct shard ID based on the shard key
3. Route rows to the correct physical shards

Example (from conductor tests):
```typescript
const sql = await createConnection(dbId, { nodes: 2 }, env);
await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`;

// Insert through conductor - it handles _virtualShard automatically
await sql`INSERT INTO users (id, email) VALUES (${1}, ${'user1@example.com'})`;
await sql`INSERT INTO users (id, email) VALUES (${2}, ${'user2@example.com'})`;
// ... etc
```

## Storage Node Behavior

When the conductor sends an INSERT to a storage node:
1. The conductor includes the shard ID in the query modification
2. Storage node receives: `INSERT INTO users (id, email, _virtualShard) VALUES (?, ?, ?)`
3. Storage node executes with proper _virtualShard value

See `src/queue-consumer.ts` lines 652-656 for how INSERT data is copied with _virtualShard during resharding.

## Testing Manually

If you want to test with direct SQL inserts and resharding:

1. **Insert with _virtualShard explicitly**:
```sql
INSERT INTO users (id, email, _virtualShard) VALUES
(1, 'user1@example.com', 0),
(2, 'user2@example.com', 0),
...
(100, 'user100@example.com', 0);
```

2. **Or use the conductor tests** (recommended):
   - They properly test the full flow including resharding
   - See `test/engine/conductor.test.ts` for examples
   - See `test/engine/queue-consumer.test.ts` for resharding tests
