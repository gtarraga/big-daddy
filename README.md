# databases

This is a Distributed Database, it uses cloudflare durable objects and a vitess like virtual sharding mecahnism to efficiently scale out the sql lite based durable object databases to a currently unknown maximum size.

It has 3 parts

* Conductor - Typescript based Query Planner and Tierd Metadata Cache.
* Topology - Durable Object metadata store - tracks tables/shards/indexes
* Storage - N Durable Object dumb storage nodes

The query planning is also powered by our homemade sqlite-ast parser found at `packages/sqlite-ast`. This enables us to understand the users query and understand exactly where we need to read or write the data.



I think I already know what the bug in the sharding logic is. We have a fundamental flaw with how we deal with duplication, i.e. trying to reshard on the same node. This is because the primary key conflicts.

  We can solve this by use a composite primary key always, We should always check if there is already a composite
  primary key already but append our key in the front.

  This will make things quite a bit simpler :)

  First check the existing conductor tests and see if we have any logic relating to this and update them to guide the new behaviour

Then implement this in  handleCreateTable method in the conductor. Update the existing injectVirtualShardColumn to update the AST Rather than hacking the table. Also update the AST to make sure that we have a composite primary key and the virutal shard column is the first key in it. Preserve any existing composite key if it already exists.
