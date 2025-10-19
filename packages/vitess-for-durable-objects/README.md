# Vitess for Durable Objects

A sharded SQLite-based database system built on Cloudflare Durable Objects, designed to break past the limits imposed by D1.

## Overview

This project implements a distributed database architecture heavily inspired by a combination of **DynamoDB** and **Vitess**, leveraging Cloudflare Durable Objects to provide scalable, sharded SQLite storage.

## Architecture

The system consists of three main components:

### 1. **Gate** (Query Router)
The Gate component runs in the Cloudflare Worker and serves as the query entry point:
- Parses incoming SQL queries
- Looks up topology information to determine which shards contain the relevant data
- Remaps queries to target specific storage nodes
- Merges results from multiple storage nodes
- Returns the consolidated result to the client

### 2. **Topology** (Metadata Store)
The Topology Durable Object manages cluster metadata:
- Stores information about shard distribution
- Maintains mapping of data to storage nodes
- Provides routing information to Gate

### 3. **Storage** (Data Nodes)
Storage Durable Objects act as individual database shards:
- Each instance is a small, independent SQLite database
- Stores a subset of the total data
- Executes queries remapped by Gate
- Returns results for merging

## How It Works

1. **Query Reception**: A SQL query arrives at the Cloudflare Worker
2. **Query Parsing**: Gate parses the query to understand the operation and target tables
3. **Topology Lookup**: Gate consults the Topology Durable Object to determine which Storage nodes contain relevant data
4. **Query Remapping**: Gate transforms the original query into shard-specific queries
5. **Parallel Execution**: Remapped queries are sent to the appropriate Storage Durable Objects
6. **Result Merging**: Gate collects and merges results from all Storage nodes
7. **Response**: The consolidated result is returned to the client

## Why This Approach?

- **Scalability**: Horizontal scaling through sharding bypasses single-database limits
- **Performance**: Parallel query execution across multiple Storage nodes
- **Flexibility**: SQLite provides full SQL capabilities not available in D1
- **Edge Computing**: Leverages Cloudflare's global network for low-latency access

## Project Structure

```
src/
├── Gate/          # Query routing and result merging logic
├── Topology/      # Shard topology and metadata management
└── storage/       # Individual SQLite storage nodes
```

## Status

This project is currently in development.
