# LLM Resources - Big Daddy Conductor

Concise reference documentation for the Conductor component of the distributed SQL engine.

## Files

| File | Description |
|------|-------------|
| `conductor-architecture.md` | High-level architecture, entry points, statement routing |
| `conductor-crud.md` | SELECT/INSERT/UPDATE/DELETE handlers and flows |
| `conductor-tables-indexes.md` | CREATE/DROP TABLE, indexes, ALTER, RESHARD |
| `conductor-utils.md` | Utility functions: shard injection, execution, caching |
| `conductor-key-patterns.md` | Important patterns, gotchas, and pitfalls |

## Quick Reference

### Statement → Handler Mapping
```
SELECT        → crud/select.ts
INSERT        → crud/insert.ts
UPDATE        → crud/update.ts
DELETE        → crud/delete.ts
CREATE TABLE  → tables/create-drop.ts
DROP TABLE    → tables/create-drop.ts
CREATE INDEX  → indexes/create.ts
DROP INDEX    → indexes/drop.ts
PRAGMA        → pragmas/pragma.ts
```

### Key Files
- **Entry**: `conductor/index.ts` - ConductorClient class
- **Types**: `conductor/types.ts` - QueryResult, QueryHandlerContext
- **Execution**: `conductor/utils/write.ts` - executeOnShards, getCachedQueryPlanData
- **Helpers**: `conductor/utils/helpers.ts` - injectVirtualShard, mergeResultsSimple

### Core Flow
```
SQL → parse() → route via Topology → executeOnShards() → merge → result
```

## Status

| Component | Status |
|-----------|--------|
| Topology DO | ✅ Complete |
| Storage DO | ✅ Complete |
| Conductor | 🔧 Active development |

Focus areas for conductor: features and bug fixes as noted by user.
