# Conductor Architecture

## Overview
The **Conductor** is the query gateway (analogous to Vitess VTGate). It parses SQL, routes to shards, executes in parallel, and merges results.

## Entry Point
`packages/big-daddy/src/engine/conductor/index.ts`

```typescript
ConductorClient.sql<T>(strings, ...values) → QueryResult<T>
```

## Core Flow
1. **Parse** - `buildQuery()` + `parse()` from `@databases/sqlite-ast`
2. **Route** - `getCachedQueryPlanData()` → determines target shards
3. **Execute** - `executeOnShards()` → parallel batched execution (7 per batch)
4. **Merge** - `mergeResultsSimple()` → combine shard results
5. **Index Maintenance** - dispatch async jobs if virtual indexes exist

## Directory Structure
```
packages/big-daddy/src/engine/conductor/
├── index.ts          # ConductorClient class, createConductor()
├── types.ts          # QueryResult, QueryHandlerContext, error types
├── crud/             # SELECT, INSERT, UPDATE, DELETE handlers
├── tables/           # CREATE/DROP TABLE, DESCRIBE, ALTER, RESHARD
├── indexes/          # CREATE/DROP INDEX
├── pragmas/          # PRAGMA reshardTable
└── utils/            # Shared utilities
```

## Key Types

### QueryHandlerContext
```typescript
interface QueryHandlerContext {
  databaseId: string;
  correlationId: string;
  storage: DurableObjectNamespace<Storage>;
  topology: DurableObjectNamespace<Topology>;
  indexQueue?: Queue;
  env?: Env;  // For test environment queue processing
  cache: TopologyCache;
}
```

### QueryResult
```typescript
interface QueryResult<T> {
  rows: T[];
  rowsAffected?: number;
  cacheStats?: QueryCacheStats;  // SELECT only
  shardStats?: ShardStats[];
}
```

## Statement Routing
| Statement Type | Handler | File |
|---------------|---------|------|
| SELECT | `handleSelect` | `crud/select.ts` |
| INSERT | `handleInsert` | `crud/insert.ts` |
| UPDATE | `handleUpdate` | `crud/update.ts` |
| DELETE | `handleDelete` | `crud/delete.ts` |
| CREATE TABLE | `handleCreateTable` | `tables/create-drop.ts` |
| DROP TABLE | `handleDropTable` | `tables/create-drop.ts` |
| CREATE INDEX | `handleCreateIndex` | `indexes/create.ts` |
| DROP INDEX | `handleDropIndex` | `indexes/drop.ts` |
| PRAGMA | `handlePragma` | `pragmas/pragma.ts` |

## Dependencies
- `@databases/sqlite-ast` - SQL parsing and generation
- `effect` - Functional error handling in write operations
- TopologyCache - Query plan caching (30s-5min TTL)
