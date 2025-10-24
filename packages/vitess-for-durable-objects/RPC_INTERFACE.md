# RPC Interface Guide

Vitess for Durable Objects exposes an RPC interface that allows other Cloudflare Workers to execute SQL queries via service bindings. This provides a clean, type-safe way to interact with your distributed database from any worker in your infrastructure.

## Service Binding Setup

### 1. Configure the Service Binding

In your consuming worker's `wrangler.jsonc`, add a service binding:

```jsonc
{
  "name": "my-app-worker",
  "main": "src/index.ts",
  "services": [
    {
      "binding": "VITESS",
      "service": "vitess-for-durable-objects"
    }
  ]
}
```

### 2. Update TypeScript Types

Add the binding to your worker's environment interface:

```typescript
// src/types.ts
interface Env {
  VITESS: Service<VitessWorker>;
}
```

Or let Cloudflare generate types automatically:
```bash
npm run cf-typegen
```

## Usage Examples

### Basic Query Execution

```typescript
// In your worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Execute a SELECT query
    const result = await env.VITESS.sql(
      ['SELECT * FROM users WHERE id = ', ''],
      [123],
      'my-database' // optional database ID, defaults to 'default'
    );

    return Response.json(result);
  }
}
```

### Creating Tables

```typescript
// Create a table
await env.VITESS.sql(
  ['CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'],
  [],
  'my-database'
);
```

### Inserting Data

```typescript
// Insert a single row
await env.VITESS.sql(
  ['INSERT INTO users (name, email) VALUES (', ', ', ')'],
  ['Alice', 'alice@example.com'],
  'my-database'
);

// Insert multiple rows in a transaction
await env.VITESS.sql(
  ['INSERT INTO users (name, email) VALUES (', ', ', '), (', ', ', ')'],
  ['Bob', 'bob@example.com', 'Charlie', 'charlie@example.com'],
  'my-database'
);
```

### Creating Indexes

```typescript
// Create a virtual index for query optimization
await env.VITESS.sql(
  ['CREATE INDEX idx_email ON users(email)'],
  [],
  'my-database'
);

// Create a composite index
await env.VITESS.sql(
  ['CREATE INDEX idx_country_city ON users(country, city)'],
  [],
  'my-database'
);
```

### Querying with Indexes

```typescript
// This query will automatically use the idx_email index
// to route to only the shards containing matching data
const result = await env.VITESS.sql(
  ['SELECT * FROM users WHERE email = ', ''],
  ['alice@example.com'],
  'my-database'
);

console.log(result.rows); // [{ id: 1, name: 'Alice', email: 'alice@example.com' }]
```

### Updating Data

```typescript
// Update rows - index maintenance happens asynchronously
await env.VITESS.sql(
  ['UPDATE users SET email = ', ' WHERE id = ', ''],
  ['newemail@example.com', 123],
  'my-database'
);
```

### Deleting Data

```typescript
// Delete rows - index maintenance happens asynchronously
await env.VITESS.sql(
  ['DELETE FROM users WHERE id = ', ''],
  [123],
  'my-database'
);
```

## Response Format

All queries return a `QueryResult` object:

```typescript
interface QueryResult {
  rows: Record<string, any>[];  // Array of result rows
  rowsAffected?: number;         // Number of rows affected (for INSERT/UPDATE/DELETE)
}
```

### Examples:

**SELECT query:**
```typescript
{
  rows: [
    { id: 1, name: 'Alice', email: 'alice@example.com' },
    { id: 2, name: 'Bob', email: 'bob@example.com' }
  ],
  rowsAffected: 2
}
```

**INSERT/UPDATE/DELETE:**
```typescript
{
  rows: [],
  rowsAffected: 3  // 3 rows were inserted/updated/deleted
}
```

**CREATE TABLE/INDEX:**
```typescript
{
  rows: [],
  rowsAffected: 0
}
```

## HTTP API (Alternative to RPC)

If you prefer HTTP, you can also call the worker directly:

```bash
curl -X POST https://your-worker.workers.dev/sql \
  -H "Content-Type: application/json" \
  -d '{
    "database": "my-database",
    "query": "SELECT * FROM users WHERE id = ?",
    "params": [123]
  }'
```

Response:
```json
{
  "rows": [
    {
      "id": 123,
      "name": "Alice",
      "email": "alice@example.com"
    }
  ],
  "rowsAffected": 1
}
```

## Advanced: Helper Function

Create a helper function to simplify RPC calls:

```typescript
// src/db.ts
import type { QueryResult } from 'vitess-for-durable-objects';

export class Database {
  constructor(
    private vitess: Service<VitessWorker>,
    private databaseId: string = 'default'
  ) {}

  async query(
    strings: TemplateStringsArray,
    ...values: any[]
  ): Promise<QueryResult> {
    return await this.vitess.sql(
      strings as any,
      values,
      this.databaseId
    );
  }
}

// Usage in worker:
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const db = new Database(env.VITESS, 'my-database');

    // Now you can use tagged template literals!
    const userId = 123;
    const result = await db.query`SELECT * FROM users WHERE id = ${userId}`;

    return Response.json(result);
  }
}
```

## Performance Considerations

### Virtual Indexes
- **CREATE INDEX** operations are async - the index builds in the background via Cloudflare Queue
- Queries can use indexes immediately after creation, but routing optimization may take a few seconds
- INSERT operations update indexes synchronously (before the query executes)
- UPDATE/DELETE operations update indexes asynchronously (after the query executes)

### Shard Fan-out
- Queries without index conditions will hit **all shards** (full table scan)
- Queries with indexed WHERE clauses will only hit **relevant shards**
- Use indexes on frequently queried columns to minimize latency

### Connection Pooling
Service bindings are extremely fast - no TCP handshake, no connection pooling needed. Each RPC call is routed directly to the Durable Object network.

## Multi-Database Support

You can manage multiple databases within a single Vitess instance:

```typescript
// Database 1: User data
await env.VITESS.sql(
  ['CREATE TABLE users (...)'],
  [],
  'users-db'
);

// Database 2: Analytics data
await env.VITESS.sql(
  ['CREATE TABLE events (...)'],
  [],
  'analytics-db'
);

// Query different databases
const users = await env.VITESS.sql(['SELECT * FROM users'], [], 'users-db');
const events = await env.VITESS.sql(['SELECT * FROM events'], [], 'analytics-db');
```

Each database has its own:
- Topology metadata
- Shard mappings
- Virtual indexes
- Storage nodes

## Error Handling

```typescript
try {
  await env.VITESS.sql(
    ['SELECT * FROM nonexistent_table'],
    [],
    'my-database'
  );
} catch (error) {
  if (error.message.includes('not found in topology')) {
    // Table doesn't exist
    console.error('Table not found');
  } else {
    // Other SQL error
    console.error('Query failed:', error);
  }
}
```

## Best Practices

1. **Use Indexes**: Create indexes on columns used in WHERE clauses to minimize shard fan-out
2. **Batch Operations**: Group related queries together to reduce RPC overhead
3. **Async Writes**: Remember that UPDATE/DELETE index maintenance is async - eventual consistency
4. **Database Naming**: Use descriptive database IDs (e.g., 'production-users', 'dev-analytics')
5. **Error Handling**: Always wrap queries in try-catch blocks

## Next Steps

- See [VIRTUAL_INDEX.md](./VIRTUAL_INDEX.md) for details on index architecture
- See [ASYNC_INDEX_MAINTENANCE.md](./ASYNC_INDEX_MAINTENANCE.md) for async maintenance details
- Check [README.md](./README.md) for deployment and configuration
