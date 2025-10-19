# SQL Parser TODO List

This document tracks the SQL features that need to be implemented in the parser.

## High Priority - Core SELECT Features

- [x] **Column aliases** - `SELECT name AS user_name FROM users`
- [x] **ORDER BY clause** - `SELECT * FROM users ORDER BY created_at DESC, name ASC`
- [x] **LIMIT and OFFSET** - `SELECT * FROM users LIMIT 10 OFFSET 20`
- [x] **GROUP BY and HAVING** - `SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5`
- [x] **Function calls** - `SELECT COUNT(*), MAX(price), SUM(total) FROM orders`
- [x] **Wildcard SELECT** - `SELECT * FROM users` or `SELECT users.*, orders.id FROM users JOIN orders`

## Medium Priority - Expression Features

- [x] **Logical operators (AND/OR/NOT)** - `WHERE age > 18 AND status = 'active'`
- [x] **Comparison operators** - `<`, `>`, `<=`, `>=`, `!=`, `<>`
- [x] **IN operator** - `WHERE id IN (1, 2, 3)`
- [x] **BETWEEN operator** - `WHERE price BETWEEN 10 AND 100`
- [x] **LIKE operator** - `WHERE name LIKE '%smith%'`
- [x] **IS NULL / IS NOT NULL** - `WHERE deleted_at IS NULL`
- [x] **Parenthesized expressions** - `WHERE (age > 18 AND country = 'US') OR admin = true`
- [x] **Subqueries** - `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`

## Medium Priority - Other Statement Types

- [x] **INSERT statement** - `INSERT INTO users (name, email) VALUES ('John', 'john@example.com')`
- [x] **UPDATE statement** - `UPDATE users SET name = 'John' WHERE id = 1`
- [x] **DELETE statement** - `DELETE FROM users WHERE id = 1`
- [x] **CREATE TABLE statement** - `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`
- [x] **ALTER TABLE statement** - `ALTER TABLE users ADD COLUMN phone TEXT`

## Lower Priority - Advanced Features

- [x] **DISTINCT** - `SELECT DISTINCT category FROM products`
- [ ] **UNION / UNION ALL** - `SELECT name FROM users UNION SELECT name FROM customers`
- [ ] **WITH (CTE)** - `WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users`
- [x] **CASE expressions** - `SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users`
- [ ] **Window functions** - `SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) FROM users`
- [ ] **Table aliases** - `FROM users u` (works in JOINs but not standalone)
- [ ] **EXISTS / NOT EXISTS** - `WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`
- [ ] **Multiple statements** - Parse multiple SQL statements in one input
- [ ] **Comments in AST** - Preserve comments for formatting/documentation

## SQLite-Specific Features

- [ ] **PRAGMA statements** - `PRAGMA table_info(users)`
- [ ] **ATTACH/DETACH DATABASE** - `ATTACH DATABASE 'file.db' AS other`
- [x] **CREATE INDEX** - `CREATE INDEX idx_name ON users(name)`
- [ ] **CREATE TRIGGER** - `CREATE TRIGGER update_timestamp ...`

## Completed Features

- [x] **Basic SELECT** - `SELECT name, email FROM users`
- [x] **FROM clause** - `FROM table_name`
- [x] **WHERE clause with binary expressions** - `WHERE id = 1`
- [x] **JOIN clauses** - `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, `OUTER JOIN`
- [x] **ON conditions** - `ON users.id = orders.user_id`
- [x] **Qualified identifiers** - `users.name`, `orders.total`
- [x] **String and numeric literals** - `'hello'`, `123`, `45.67`
- [x] **Basic tokenizer** - Tokenizes SQL into tokens with types
