---
title: SQL
nav_order: 9
---

# SQL
{: .no_toc }

ZIO-LMDB ships an experimental **SQL layer** (the `zio-lmdb-sql` module) that lets you query and
mutate collections with a familiar SQL dialect, both from an interactive REPL and programmatically.
A collection behaves like a single table: the key is the pseudo-column `_key`, and the value fields
(discovered from the collection's [schema](schema.html)) are the other columns.

{: .note }
The SQL layer is new in 3.x and still evolving. It is a deliberately small, read-mostly dialect over
a single collection — there are no joins, subqueries, or DDL. Use the [Query DSL](query-dsl.html) or
the typed collection API when you need the full programmatic power.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## The data model

| SQL concept | Maps to |
|---|---|
| table | a collection |
| `_key` pseudo-column | the collection key (decoded with its `KeyCodec`) |
| regular columns | the value's JSON fields (from the value [schema](schema.html)) |
| `_value` pseudo-column | the whole value, when it is a scalar rather than an object |

Rows are read straight from the LMDB cursor and decoded generically (the key via its recorded
`keyId`, the value as plain JSON), so the engine needs no compile-time `K`/`T` types. Most operators
stream; `ORDER BY`, `GROUP BY`/aggregates, and `DISTINCT` buffer (noted per feature below).

---

## Running the REPL

Build the self-contained REPL jar and start it:

```bash
sbt sql/assembly
java --add-opens java.base/java.nio=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     -jar sql/target/scala-3.3.7/zio-lmdb-sql.jar [databases-home]
```

`databases-home` defaults to `~/.lmdb`. A `Makefile` shortcut is provided:

```bash
make sql-console
```

You land in a shell. Connect to a database, then run statements (terminated by an optional `;`):

```text
zio-lmdb-sql  —  databases home: /home/me/.lmdb
Type SQL, or \h for help. \q to quit.
lmdb(-)> \c my-app
connected to 'my-app'
lmdb(my-app)> SELECT _key, name, age FROM users ORDER BY age DESC LIMIT 5;
```

### Meta-commands

| Command | Effect |
|---|---|
| `\c <name>` | connect to a database under the databases home |
| `\l` | list databases |
| `\dt` | list collections (= `SHOW COLLECTIONS`) |
| `\di` | list indexes (= `SHOW INDEXES`) |
| `\d <collection>` | describe a collection (= `DESCRIBE <collection>`) |
| `\format table\|json\|csv` | set the output format (default `table`) |
| `\h` | help |
| `\q` | quit |

{: .note }
**TAB completes** meta-commands, SQL keywords, database names (after `\c`), collection names (after
`\d` / `FROM` / `INTO` / `UPDATE`), and the columns of the `FROM` collection.

---

## SELECT

### Projections

```sql
SELECT * FROM users;                       -- _key plus every value field
SELECT _key FROM users;                    -- just the key
SELECT _key, name, age FROM users;         -- specific columns
SELECT name AS fullName, age AS years FROM users;  -- column aliases
```

`_key` is always available. For collections whose values are scalars (not objects), the whole value
is exposed as `_value`.

### WHERE

Comparison operators: `=`, `!=` (or `<>`), `<`, `<=`, `>`, `>=`. Combine with `AND`, `OR`, `NOT`,
and parentheses.

```sql
SELECT * FROM users WHERE age >= 18;
SELECT * FROM users WHERE age >= 18 AND country = 'FR';
SELECT * FROM users WHERE NOT (age < 18 OR country = 'XX');
SELECT * FROM orders WHERE amount > 9.99 AND amount <= 100;
SELECT * FROM users WHERE _key = 'alice';
```

String matching with `LIKE` (`%` = any run, `_` = any single char):

```sql
SELECT name FROM users WHERE name LIKE 'A%';
SELECT name FROM users WHERE name LIKE '_ob';
SELECT * FROM files WHERE path LIKE '%/2026/%';
```

Null tests:

```sql
SELECT * FROM users WHERE nickname IS NULL;
SELECT * FROM users WHERE nickname IS NOT NULL;
```

Literals: single-quoted strings (with `''` for an embedded quote), integers, decimals, `true`,
`false`, `null`.

```sql
SELECT * FROM products WHERE label = 'it''s a sale';
SELECT * FROM products WHERE inStock = true;
SELECT * FROM products WHERE discount = 9.99;
```

### Scalar functions

`LENGTH(x)` returns the character length of `x` as text (`NULL` stays `NULL`). Functions can appear
anywhere an expression is allowed (`WHERE`, `HAVING`).

```sql
SELECT * FROM users WHERE LENGTH(name) > 0;
SELECT * FROM users WHERE LENGTH(country) = 2;
```

### ORDER BY and LIMIT

```sql
SELECT _key, age FROM users ORDER BY age;          -- ascending (default)
SELECT _key, age FROM users ORDER BY age DESC;     -- descending
SELECT * FROM users ORDER BY _key LIMIT 10;
SELECT name AS n FROM users ORDER BY n;             -- order by an alias
```

### SELECT DISTINCT

De-duplicates the projected rows (buffers, like `ORDER BY`).

```sql
SELECT DISTINCT country FROM users;
SELECT DISTINCT city, country FROM users ORDER BY country LIMIT 20;
```

---

## Aggregates and GROUP BY

Aggregate functions: `COUNT(*)`, `COUNT(col)` (non-null values), `SUM(col)`, `AVG(col)`,
`MIN(col)`, `MAX(col)`. Each output column is named `func(arg)` unless you give it an `AS` alias.

### Whole-table aggregates (no GROUP BY)

```sql
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) AS total FROM orders WHERE amount > 100;
SELECT COUNT(customer) FROM orders;                 -- counts non-null customers
SELECT SUM(amount), AVG(amount) FROM orders;
SELECT MIN(amount) AS lo, MAX(amount) AS hi FROM orders;
```

{: .note }
A whole-table aggregate always returns exactly one row — `COUNT(*)` of an empty table is `0`, the
other aggregates are `NULL`.

### Grouped aggregates

```sql
SELECT customer, COUNT(*) FROM orders GROUP BY customer;
SELECT customer, COUNT(*) AS n, SUM(amount) AS total
  FROM orders
  GROUP BY customer
  ORDER BY total DESC;

SELECT country, city, COUNT(*) AS people
  FROM users
  GROUP BY country, city
  ORDER BY country;
```

Every non-aggregated column in the projection must appear in `GROUP BY`. Groups are ordered by their
key by default (so output is deterministic without an explicit `ORDER BY`). The fold keeps one
accumulator set per group, so memory scales with the number of groups, not rows.

### HAVING

`HAVING` filters groups using aggregates (and grouping columns). It may reference aggregates that are
not in the `SELECT` list.

```sql
SELECT customer, COUNT(*) AS n
  FROM orders
  GROUP BY customer
  HAVING COUNT(*) > 1;

SELECT cameraName, COUNT(*) AS count
  FROM originals
  WHERE LENGTH(cameraName) > 0
  GROUP BY cameraName
  HAVING COUNT(*) > 100
  ORDER BY count;

SELECT customer, SUM(amount) AS spent
  FROM orders
  GROUP BY customer
  HAVING SUM(amount) >= 1000 AND COUNT(*) > 5;
```

{: .warning }
Aggregates belong in `HAVING`, not `WHERE` — `WHERE … COUNT(*) > 1` is rejected. `WHERE` filters
individual rows *before* grouping; `HAVING` filters groups *after*.

---

## Clause order

The dialect follows standard SQL order. Writing clauses out of order is a parse error.

```sql
SELECT [DISTINCT] <projection>
  FROM <collection>
  [WHERE <condition>]
  [GROUP BY <columns>]
  [HAVING <condition>]
  [ORDER BY <column> [ASC|DESC]]
  [LIMIT <n>]
```

---

## INSERT / UPDATE / DELETE

Writes go through the same generic JSON path. An `INSERT` must set `_key`; the remaining columns
become the value's fields.

```sql
INSERT INTO users (_key, name, age) VALUES ('p4', 'Dave', 50);
INSERT INTO orders (_key, customer, amount) VALUES ('o9', 'Alice', 12.50);

UPDATE users SET age = 31 WHERE _key = 'p1';
UPDATE users SET country = 'FR', active = true WHERE country = 'France';

DELETE FROM users WHERE _key = 'p2';
DELETE FROM orders WHERE amount < 1;
```

Each write reports the number of affected rows as a one-row `affected` result.

---

## DESCRIBE and SHOW

```sql
SHOW COLLECTIONS;        -- or \dt
SHOW INDEXES;            -- or \di
DESCRIBE users;          -- or \d users  (also: DESC users)
```

`DESCRIBE` lists the key's codec id and the value columns with their inferred types — these come from
the collection's persisted [schema](schema.html), not from guessing:

```text
lmdb(my-app)> DESCRIBE users;
 column | type
--------+----------
 _key   | lmdb:str
 name   | string
 age    | integer
(3 rows)
```

---

## Output formats

`\format` switches how results render. All three consume the same result; `json` and `csv` stream
per row, `table` buffers to align columns.

```text
lmdb(my-app)> \format json
format: json
lmdb(my-app)> SELECT _key, name FROM users LIMIT 2;
{"_key":"p1","name":"Alice"}
{"_key":"p2","name":"Bob"}

lmdb(my-app)> \format csv
format: csv
lmdb(my-app)> SELECT _key, name FROM users LIMIT 2;
_key,name
p1,Alice
p2,Bob
```

---

## Programmatic use

The engine is a pure pipeline (`parse → bind → execute`) over `ZIO`; the REPL is just one front end.
`SqlEngine.run` parses and executes a statement against an `LMDB` service and yields a streaming
`QueryResult`.

```scala
import zio.*
import zio.lmdb.*
import zio.lmdb.sql.engine.SqlEngine
import zio.lmdb.sql.result.{Format, Renderer}

val program: ZIO[LMDB, Throwable, Unit] =
  for {
    result <- SqlEngine.run("SELECT customer, COUNT(*) AS n FROM orders GROUP BY customer ORDER BY n DESC")
    _      <- Renderer.render(Format.Table, result).runForeach(Console.printLine(_))
  } yield ()
```

`QueryResult` exposes `columns` (known eagerly) and `rows` (a `ZStream` of `JValue` documents), so
small results can be materialised with `result.toList` and large ones consumed lazily.

---

## What is supported (and what is not)

**Supported:** `SELECT` (`*`, columns, aggregates, `AS` aliases), `DISTINCT`, `WHERE`
(`= != <> < <= > >=`, `AND`/`OR`/`NOT`, parentheses, `LIKE`, `IS [NOT] NULL`, `LENGTH`), `GROUP BY`,
`HAVING`, `ORDER BY` (`ASC`/`DESC`), `LIMIT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `INSERT`/`UPDATE`/
`DELETE`, `DESCRIBE`, `SHOW COLLECTIONS`/`SHOW INDEXES`, and the `_key`/`_value` pseudo-columns.

**Not (yet) supported:** joins, subqueries, `UNION`, window functions, `CASE`, arithmetic
expressions, scalar functions beyond `LENGTH`, aggregate arguments that are expressions
(e.g. `SUM(a + b)`), and DDL (`CREATE`/`DROP`). Identifiers are letters/digits/underscore; keywords
are case-insensitive.
