# Conductor

The **Conductor** is the query routing layer for Vitess-for-Durable-Objects. It sits between your application and the distributed storage layer, orchestrating SQL queries across sharded databases.

## API

### Creating a Conductor

```typescript
import { createConductor } from './Conductor/Conductor';

const conductor = createConductor('my-database', env);
```

### Executing Queries with Tagged Template Literals

The Conductor uses tagged template literals for a clean, safe SQL API:

```typescript
// Simple query
const users = await conductor.sql`SELECT * FROM users`;

// Query with parameters (automatically escaped)
const userId = 123;
const user = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;

// Multiple parameters
const name = 'John';
const age = 25;
const result = await conductor.sql`
  SELECT * FROM users
  WHERE name = ${name} AND age > ${age}
`;

// INSERT query
const email = 'john@example.com';
await conductor.sql`
  INSERT INTO users (name, email)
  VALUES (${name}, ${email})
`;

// UPDATE query
const newEmail = 'newemail@example.com';
await conductor.sql`
  UPDATE users
  SET email = ${newEmail}
  WHERE id = ${userId}
`;
```

## Query Result

All queries return a `QueryResult` object:

```typescript
interface QueryResult {
  rows: Record<string, any>[];  // Query results
  rowsAffected?: number;        // Number of rows affected (for INSERT/UPDATE/DELETE)
}
```

### Example Usage

```typescript
const result = await conductor.sql`SELECT id, name, email FROM users`;

console.log(result.rows);
// [
//   { id: 1, name: 'Alice', email: 'alice@example.com' },
//   { id: 2, name: 'Bob', email: 'bob@example.com' }
// ]
```

## How it Works

The Conductor:

1. **Parses** the SQL query using the sqlite-ast parser
2. **Determines** which table(s) and shard(s) are involved
3. **Routes** the query to the appropriate Storage Durable Object(s)
4. **Merges** results if the query spans multiple shards
5. **Returns** the final result to the caller

## Benefits of Tagged Template Literals

✅ **SQL Injection Protection**: Parameters are automatically escaped
✅ **Clean Syntax**: Looks like natural SQL
✅ **Type Safety**: Full TypeScript support
✅ **IDE Support**: Syntax highlighting and autocomplete

## Example: Complete Flow

```typescript
import { createConductor } from './Conductor/Conductor';

export default {
  async fetch(request, env, ctx): Promise<Response> {
    // Create conductor for database
    const conductor = createConductor('my-app-db', env);

    // Parse request
    const url = new URL(request.url);
    const userId = url.searchParams.get('userId');

    if (!userId) {
      return new Response('Missing userId', { status: 400 });
    }

    // Execute query
    const result = await conductor.sql`
      SELECT id, name, email, created_at
      FROM users
      WHERE id = ${parseInt(userId)}
    `;

    // Return results
    return Response.json({
      user: result.rows[0] || null
    });
  }
};
```

## Roadmap

- [x] Tagged template literal API
- [x] Basic query building
- [ ] SQL query parsing
- [ ] Shard determination
- [ ] Query routing to Storage DOs
- [ ] Multi-shard query support
- [ ] Transaction support
- [ ] Query result caching
- [ ] Query optimization

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       │ conductor.sql`...`
       ▼
┌─────────────┐
│  Conductor  │  ◄── Parses SQL, determines sharding
└──────┬──────┘
       │
       ├──────────┬──────────┐
       ▼          ▼          ▼
  ┌────────┐ ┌────────┐ ┌────────┐
  │ Storage│ │ Storage│ │ Storage│  ◄── Execute on shards
  │ Shard 0│ │ Shard 1│ │ Shard 2│
  └────────┘ └────────┘ └────────┘
```
