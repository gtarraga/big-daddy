# SQL Generator

The `generate()` function transforms an AST back into a SQL query string.

## Usage

```typescript
import { parse, generate } from '@databases/sqlite-ast';

// Parse SQL to AST
const ast = parse('SELECT name, email FROM users WHERE id = 1');

// Generate SQL from AST
const sql = generate(ast);
console.log(sql);
// Output: "SELECT name, email FROM users WHERE id = 1"
```

## Use Cases

### 1. AST Transformation
Modify the AST and regenerate SQL:

```typescript
import { parse, generate } from '@databases/sqlite-ast';

// Parse original query
const ast = parse('SELECT name FROM users');

// Add a WHERE clause
ast.where = {
  type: 'BinaryExpression',
  operator: '=',
  left: { type: 'Identifier', name: 'active' },
  right: { type: 'Literal', value: 1, raw: '1' }
};

// Generate modified SQL
const sql = generate(ast);
console.log(sql);
// Output: "SELECT name FROM users WHERE active = 1"
```

### 2. Query Builder
Build queries programmatically:

```typescript
import { generate } from '@databases/sqlite-ast';
import type { SelectStatement } from '@databases/sqlite-ast';

const query: SelectStatement = {
  type: 'SelectStatement',
  select: [
    {
      type: 'SelectClause',
      expression: { type: 'Identifier', name: 'name' }
    },
    {
      type: 'SelectClause',
      expression: { type: 'Identifier', name: 'email' }
    }
  ],
  from: { type: 'Identifier', name: 'users' },
  where: {
    type: 'BinaryExpression',
    operator: '>',
    left: { type: 'Identifier', name: 'age' },
    right: { type: 'Literal', value: 18, raw: '18' }
  }
};

const sql = generate(query);
console.log(sql);
// Output: "SELECT name, email FROM users WHERE age > 18"
```

### 3. SQL Formatting
Parse and regenerate to normalize SQL formatting:

```typescript
import { parse, generate } from '@databases/sqlite-ast';

const messySql = "select   name,email   from users where  id=1";
const ast = parse(messySql);
const cleanSql = generate(ast);
console.log(cleanSql);
// Output: "SELECT name, email FROM users WHERE id = 1"
```

## Supported Statements

The generator supports all statement types that the parser supports:

- **SELECT**: Including DISTINCT, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- **INSERT**: With or without column specifications, multiple value rows
- **UPDATE**: With SET clauses and WHERE conditions
- **DELETE**: With WHERE conditions
- **CREATE TABLE**: With column definitions and constraints
- **ALTER TABLE**: ADD COLUMN, RENAME TO, RENAME COLUMN, DROP COLUMN
- **CREATE INDEX**: With UNIQUE and IF NOT EXISTS options

## Supported Expressions

- **Identifiers**: Table and column names (including qualified names like `table.column`)
- **Literals**: Numbers, strings, NULL, booleans
- **Binary Expressions**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `LIKE`
- **Unary Expressions**: `IS NULL`, `IS NOT NULL`
- **Function Calls**: `COUNT(*)`, `MAX(age)`, etc.
- **IN Expression**: `id IN (1, 2, 3)` or `id IN (SELECT ...)`
- **BETWEEN Expression**: `age BETWEEN 18 AND 65`
- **CASE Expression**: `CASE WHEN ... THEN ... ELSE ... END`
- **Subqueries**: `SELECT * FROM (SELECT ...)`

## Round-Trip Guarantee

For valid SQL that the parser supports, the following should always be true:

```typescript
import { parse, generate } from '@databases/sqlite-ast';

const originalSql = "SELECT name FROM users WHERE id = 1";
const regeneratedSql = generate(parse(originalSql));

console.log(originalSql === regeneratedSql); // true
```

Note: Trailing semicolons are removed during parsing, so they won't be present in the generated SQL.
