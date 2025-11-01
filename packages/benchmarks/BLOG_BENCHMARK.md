# Blog Benchmark

A realistic blog application benchmark for Big Daddy, featuring a complete blog schema with users, posts, comments, and likes. This benchmark uses Cloudflare Workflows to seed the database with test data.

## Schema

### Tables

1. **users** - User accounts with profile information
   - `id` (INTEGER PRIMARY KEY) - User ID (shard key)
   - `username` (TEXT) - Unique username
   - `email` (TEXT) - User email
   - `bio` (TEXT) - User biography
   - `created_at` (INTEGER) - Account creation timestamp
   - Index: `idx_users_username` on `username`

2. **posts** - Blog posts with author references
   - `id` (INTEGER PRIMARY KEY) - Post ID (shard key)
   - `user_id` (INTEGER) - Author user ID
   - `title` (TEXT) - Post title
   - `content` (TEXT) - Post content
   - `published_at` (INTEGER) - Publication timestamp
   - Index: `idx_posts_user_id` on `user_id`

3. **comments** - Comments on posts
   - `id` (INTEGER PRIMARY KEY) - Comment ID (shard key)
   - `post_id` (INTEGER) - Post ID
   - `user_id` (INTEGER) - Commenter user ID
   - `content` (TEXT) - Comment text
   - `created_at` (INTEGER) - Comment timestamp
   - Indexes: `idx_comments_post_id` on `post_id`, `idx_comments_user_id` on `user_id`

4. **likes** - Post likes
   - `id` (INTEGER PRIMARY KEY) - Like ID (shard key)
   - `post_id` (INTEGER) - Post ID
   - `user_id` (INTEGER) - User ID
   - `created_at` (INTEGER) - Like timestamp
   - Index: `idx_likes_post_user` on `(post_id, user_id)` for unique constraints and lookups

## Getting Started

### Prerequisites

1. Deploy Big Daddy first:
   ```bash
   cd ../big-daddy
   pnpm run deploy
   ```

2. Deploy the blog benchmark worker:
   ```bash
   cd ../benchmarks
   pnpm run deploy
   ```

### Local Development

1. Start Big Daddy in one terminal:
   ```bash
   cd ../big-daddy
   pnpm run dev
   ```

2. Start the blog benchmark in another terminal:
   ```bash
   cd ../benchmarks
   pnpm run dev
   ```

## API Endpoints

### Setup & Management

#### `POST /setup`
Initialize the database schema (creates tables and indexes).

```bash
curl -X POST http://localhost:8787/setup
```

#### `POST /seed`
Trigger the Cloudflare Workflow to seed the database with test data.

**Request Body:**
```json
{
  "users": 10,
  "posts": 50,
  "comments": 100,
  "likes": 200
}
```

**Example:**
```bash
curl -X POST http://localhost:8787/seed \
  -H 'Content-Type: application/json' \
  -d '{"users":10,"posts":50,"comments":100,"likes":200}'
```

**Response:**
```json
{
  "success": true,
  "message": "Seeding workflow started",
  "workflowId": "workflow-id-here",
  "config": {
    "numUsers": 10,
    "numPosts": 50,
    "numComments": 100,
    "numLikes": 200
  },
  "duration": 15,
  "correlationId": "uuid-here"
}
```

#### `POST /reset`
Drop all tables (clears all data).

```bash
curl -X POST http://localhost:8787/reset
```

#### `GET /stats`
Get database statistics (counts for each table).

```bash
curl http://localhost:8787/stats
```

**Response:**
```json
{
  "success": true,
  "stats": {
    "users": 10,
    "posts": 50,
    "comments": 100,
    "likes": 200
  },
  "duration": 23,
  "correlationId": "uuid-here"
}
```

### Query Endpoints

#### `GET /query/users?username=HappyPanda%`
Query users by username (uses index scan).

```bash
curl "http://localhost:8787/query/users?username=HappyPanda%"
```

#### `GET /query/posts?user_id=1000`
Query posts by user_id (uses index scan).

```bash
curl "http://localhost:8787/query/posts?user_id=1000"
```

#### `GET /query/post-with-comments?post_id=2000`
Get a post with all its comments (demonstrates multi-table queries).

```bash
curl "http://localhost:8787/query/post-with-comments?post_id=2000"
```

#### `GET /query/user-feed?user_id=1000`
Get a user's feed with posts and like counts (demonstrates complex aggregations).

```bash
curl "http://localhost:8787/query/user-feed?user_id=1000"
```

## NPM Scripts

Convenient npm scripts are provided for common operations:

```bash
# Setup the schema
pnpm run blog:setup

# Seed with test data (10 users, 50 posts, 100 comments, 200 likes)
pnpm run blog:seed

# Get statistics
pnpm run blog:stats

# Reset (drop all tables)
pnpm run blog:reset
```

## Workflow Details

The seeding process uses a Cloudflare Workflow (`SeedWorkflow`) that runs the following steps:

1. **create-connection** - Establishes SQL connection with 8 storage nodes
2. **create-schema** - Creates all tables and indexes
3. **seed-users** - Inserts user records
4. **seed-posts** - Inserts blog posts with random authors
5. **seed-comments** - Inserts comments on random posts
6. **seed-likes** - Inserts likes (with uniqueness constraints)

The workflow ensures:
- Durability: Each step is persisted and can be retried
- Consistency: Steps run in order
- Observability: Each step logs its progress
- Idempotency: Steps can be safely retried

## Example Usage Flow

```bash
# 1. First time setup - create schema
pnpm run blog:setup

# 2. Seed with small dataset for testing
curl -X POST http://localhost:8787/seed \
  -H 'Content-Type: application/json' \
  -d '{"users":5,"posts":25,"comments":50,"likes":100}'

# 3. Wait a few seconds for workflow to complete, then check stats
pnpm run blog:stats

# 4. Query the data
curl "http://localhost:8787/query/users?username=Happy%"
curl "http://localhost:8787/query/posts?user_id=1000"
curl "http://localhost:8787/query/post-with-comments?post_id=2000"

# 5. To start over, reset and re-seed
pnpm run blog:reset
pnpm run blog:setup
pnpm run blog:seed
```

## Load Testing

You can use tools like Artillery, k6, or Apache Bench to load test the endpoints:

```bash
# Example with Artillery (if configured)
pnpm run bench

# Or use a simple bash loop
for i in {1..100}; do
  curl -s "http://localhost:8787/query/posts?user_id=$(($RANDOM % 10 + 1000))" > /dev/null &
done
wait
```

## Architecture Highlights

- **Sharding**: Data is automatically distributed across 8 storage nodes based on primary keys
- **Indexes**: Virtual indexes enable efficient queries on non-shard-key columns
- **Cache**: Query plan caching reduces latency for repeated queries
- **Workflows**: Durable execution ensures reliable data seeding
- **Observability**: All requests are logged with correlation IDs for tracing

## Performance Considerations

- Queries by primary key (shard key) hit a single shard - fastest
- Queries by indexed columns may hit multiple shards but use indexes
- Full table scans hit all shards - use LIMIT for better performance
- Join-like operations (e.g., post with comments) require multiple queries in the current implementation
