# databases

This is a Distributed Database, it uses cloudflare durable objects and a vitess like virtual sharding mecahnism to efficiently scale out the sql lite based durable object databases to a currently unknown maximum size.

It has 3 parts

* Conductor - Typescript based Query Planner and Tierd Metadata Cache.
* Topology - Durable Object metadata store - tracks tables/shards/indexes
* Storage - N Durable Object dumb storage nodes

The query planning is also powered by our homemade sqlite-ast parser found at `packages/sqlite-ast`. This enables us to understand the users query and understand exactly where we need to read or write the data. It may be missing features as it its a WIP. We are currently working on maintaining the indexes via a queue.

The index tests delete-index-maintenance insert-index-maintenance.

The update-index-maintenance tests are failing and we need to figure out why. I vaguely remember this is where we got to last session so something is probably incomplete


● Summary: 114 Remaining any Usages

  Priority breakdown:

  1. as any Casts (46 instances) - Quick wins

  Most are cast workarounds for type system limitations:
  - Database query results: .toArray() as unknown as any[] - could use typed arrays (StorageNode[],
   TableMetadata[], etc.)
  - AST handling: statement as any - could use Statement type from sqlite-ast
  - Result extraction: (result as any).rows - could use QueryResult type

  2. any[] Array Types (25 instances) - Medium effort

  - params: any[] - should be SqlParam[] (we already did some)
  - values: any[] - temporary arrays for extracted values
  - columns: any[], row: any[] - row data structures
  - Function params/returns that should be typed

  3. AST/Expression Parameters (13 instances) - Medium-high effort

  Files needing updates:
  - topology-cache.ts: where: any, expression: any
  - topology/index.ts: where: any, statement: any, expression: any
  - virtual-indexes.ts: where: any, statement: any, expression: any
  - helpers.ts: expression: any

  These should use: Expression | BinaryExpression | Identifier types from sqlite-ast

  4. Storage Dependency (4 instances) - Blocked by type definition

  Files: topology/{crud,virtual-indexes,resharding}.ts, administration.ts
  - Need to find the actual Storage type to replace any
