# databases

This is a Distributed Database, it uses cloudflare durable objects and a vitess like virtual sharding mecahnism to efficiently scale out the sql lite based durable object databases to a currently unknown maximum size.

It has 3 parts

* Conductor - Typescript based Query Planner and Tierd Metadata Cache.
* Topology - Durable Object metadata store - tracks tables/shards/indexes
* Storage - N Durable Object dumb storage nodes

The query planning is also powered by our homemade sqlite-ast parser found at `packages/sqlite-ast`. This enables us to understand the users query and understand exactly where we need to read or write the data.


Next tasks are refactoring the conductor.
Break it up into

Fetching topology and caching it
Handling
* INSERTS
* SELECTS
* UPDATES
* DELETES

* CRUD on tables
* CRUD on Indexes

Merging Results
* Aggregations

Consider Joins -



fixing the index maintenence (FOR UPDATE/DELETE Queries)
