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
The SQL layer is new in 3.x and still evolving. It is a deliberately small, read-mostly dialect —
`SELECT`/`INSERT`/`UPDATE`/`DELETE` with joins, aggregates, nested value-field access, and a handful
of scalar/geo/date-time functions, but no subqueries, window functions, or DDL. Use the
[Query DSL](query-dsl.html) or the typed collection API when you need the full programmatic power.

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
| dotted column path | a nested value field of any depth (`location.altitude`) |
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

### Non-interactive execution

Pass a database name as the first argument and one or more `--execute` (short `-e`) statements to run
them without entering the shell. The result goes to stdout (logs go to stderr), so it pipes cleanly;
`--format` (short `-f`) selects `table` (default), `json`, or `csv`. `--execute` is repeatable, and a
failing statement prints `error: …` to stderr and exits non-zero.

```bash
# one query, default table output
java ... -jar zio-lmdb-sql.jar my-app --execute "SELECT COUNT(*) FROM users"

# CSV to a file, and several statements in one run
java ... -jar zio-lmdb-sql.jar my-app -f csv -e "SELECT _key, name FROM users" > users.csv
java ... -jar zio-lmdb-sql.jar my-app \
     -e "SELECT COUNT(*) AS users FROM users" \
     -e "SELECT COUNT(*) AS orders FROM orders"
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

{: .note }
**Aliases are reusable.** An `AS` alias defined in the `SELECT` list can be referenced by name in
`WHERE`, `HAVING`, and `ORDER BY` — a convenience beyond standard SQL (which exposes aliases only to
`ORDER BY`). This avoids repeating a computed expression. If an alias happens to share a name with a
real column, the alias wins within that query. (An alias that names an aggregate is still rejected in
`WHERE` — aggregates belong in `HAVING`.)

### Nested fields

A column reference is a **dotted path of any depth**, so you can reach into nested objects. The first
segment is treated as a table alias when it names a table in the `FROM`/`JOIN` clauses; otherwise the
whole path indexes into the value. Paths work everywhere a column does — `SELECT`, `WHERE`,
`ORDER BY`, `GROUP BY`, and `JOIN … ON`.

```sql
-- value: { "location": { "latitude": …, "longitude": …, "altitude": … }, "dimension": { "width": …, "height": … } }
SELECT _key, o.location.altitude AS alt FROM originals o;   -- alias-qualified nested path
SELECT location.altitude FROM originals;                    -- unqualified nested path
SELECT * FROM originals o
  WHERE o.dimension.width >= 1920
  ORDER BY o.location.altitude DESC;
```

A missing field, or a step through something that is not an object, yields `NULL` (so a row without
`location` simply has a `NULL` `location.altitude`). Nested access is read-only: `INSERT`/`UPDATE`
column lists are still top-level field names.

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

String matching with `LIKE` (`%` = any run, `_` = any single char), and its negation `NOT LIKE`:

```sql
SELECT name FROM users WHERE name LIKE 'A%';
SELECT name FROM users WHERE name LIKE '_ob';
SELECT * FROM files WHERE path LIKE '%/2026/%';
SELECT name FROM users WHERE name NOT LIKE 'A%';
```

List membership with `IN` / `NOT IN`, and inclusive ranges with `BETWEEN` / `NOT BETWEEN` (both
bounds included). The list items and the range bounds may be any expression, not just literals.

```sql
SELECT * FROM users WHERE country IN ('FR', 'BE', 'CH');
SELECT * FROM users WHERE age NOT IN (0, 999);
SELECT * FROM orders WHERE amount BETWEEN 10 AND 100;
SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65;
```

A `NULL` left-hand side matches nothing, so it is excluded by `IN`/`BETWEEN` *and* by their `NOT`
forms.

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

Function calls take the form `name(arg, …)` and may appear anywhere an expression is allowed — in the
`SELECT` list, `WHERE`, `HAVING`, and `ORDER BY`. Names are case-insensitive. `NULL` arguments
propagate to a `NULL` (or non-matching) result.

`LENGTH(x)` returns the character length of `x` as text:

```sql
SELECT * FROM users WHERE LENGTH(name) > 0;
SELECT name, LENGTH(name) AS len FROM users ORDER BY len DESC;
```

#### Numeric functions

| Function | Result |
|---|---|
| `ABS(x)` | absolute value (preserves the numeric kind) |
| `FLOOR(x)` / `CEIL(x)` (alias `CEILING`) | round down / up to a whole number |
| `ROUND(x)` / `ROUND(x, n)` | round half-up to the nearest integer, or to `n` decimal places |
| `SIGN(x)` | `-1`, `0`, or `1` |
| `MOD(a, b)` | remainder of `a / b` (`NULL` when `b` is `0`) |
| `POWER(a, b)` (alias `POW`) | `a` raised to the power `b` |
| `SQRT(x)` | square root (`NULL` for a negative `x`) |

A non-numeric argument yields `NULL`.

```sql
SELECT _key, ROUND(amount, 2) AS rounded, ABS(balance) AS magnitude FROM accounts;
SELECT _key FROM orders WHERE MOD(quantity, 2) = 0;          -- even quantities
SELECT POWER(2, 10) AS kib, SQRT(area) AS side FROM shapes;
```

#### String functions

These transform or inspect text. A non-string argument is rendered to text first (so `CONCAT(name,
age)` works); a `NULL` argument yields `NULL`.

| Function | Result |
|---|---|
| `UPPER(s)` / `LOWER(s)` | case conversion |
| `TRIM(s)` / `LTRIM(s)` / `RTRIM(s)` | strip surrounding / leading / trailing whitespace |
| `SUBSTR(s, start [, len])` (alias `SUBSTRING`) | 1-based substring (`start` clamped; a missing/negative `len` runs to the end) |
| `CONCAT(a, b, …)` | concatenate the arguments as text |
| `REPLACE(s, from, to)` | replace every literal occurrence of `from` with `to` |
| `INSTR(s, sub)` | 1-based index of the first `sub` in `s`, or `0` if absent |

```sql
SELECT UPPER(name) AS name, SUBSTR(country, 1, 2) AS cc FROM users;
SELECT CONCAT(firstName, ' ', lastName) AS fullName FROM users ORDER BY fullName;
SELECT _key FROM files WHERE INSTR(path, '/2026/') > 0;
SELECT REPLACE(path, '\\', '/') AS path FROM files;
```

#### Geo functions

For values that carry geographic coordinates, two functions compute great-circle distance with the
haversine formula (in **metres**, on a mean-radius sphere — the same convention as PostGIS
`ST_DistanceSphere`):

| Function | Result |
|---|---|
| `GEO_DISTANCE(lat1, lon1, lat2, lon2)` | distance in metres |
| `GEO_DISTANCE(point, lat, lon)` | as above, reading `latitude`/`longitude` from the `point` object |
| `GEO_WITHIN(lat1, lon1, lat2, lon2, radius)` | boolean `distance <= radius` |
| `GEO_WITHIN(point, lat, lon, radius)` | as above, with an object `point` |

The object forms pair naturally with [nested fields](#nested-fields): pass `o.location` (an object
with `latitude`/`longitude`) instead of spelling out both coordinate paths.

```sql
-- originals within 5 km of central Paris, nearest first
-- (the `AS dist` alias is reused in WHERE and ORDER BY — no need to repeat the expression)
SELECT _key, GEO_DISTANCE(o.location.latitude, o.location.longitude, 48.8566, 2.3522) AS dist
  FROM originals o
  WHERE dist <= 5000
  ORDER BY dist;

-- the same query using the object-arg form and the boolean predicate
SELECT _key
  FROM originals o
  WHERE GEO_WITHIN(o.location, 48.8566, 2.3522, 5000);
```

A row whose coordinates are missing or non-numeric (e.g. an original with no `location`) yields a
`NULL` distance and is excluded by both the `<=` filter and `GEO_WITHIN`.

{: .note }
Geo filtering currently performs a full scan and computes the distance per row; there is no spatial
(bounding-box) index pushdown yet.

#### Date and time functions

Timestamps are stored as ISO-8601 text, so a `timestamp` column reads back as a string. These
functions interpret a value as an instant — an ISO-8601 **date**, **date-time**, or **offset
date-time** string (or an actual timestamp) — and extract or measure it. Calendar fields are read in
**UTC**. A value that is not a valid timestamp yields `NULL`.

| Function | Result |
|---|---|
| `YEAR(ts)` `MONTH(ts)` `DAY(ts)` | calendar year / month (1–12) / day-of-month (1–31), as an integer |
| `HOUR(ts)` `MINUTE(ts)` `SECOND(ts)` | UTC time-of-day fields, as an integer |
| `DATE_DIFF(unit, a, b)` | `a - b` as a whole number of `unit`s |
| `NOW()` | the current instant |

`DATE_DIFF`'s `unit` is a string: `second`, `minute`, `hour`, `day`, or `millisecond` (singular or
plural; `sec`/`min`/`ms` also accepted). The result is truncated toward zero and is negative when
`a` precedes `b`.

```sql
-- newest first, with the calendar fields broken out
SELECT _key, YEAR(timestamp) AS y, MONTH(timestamp) AS m, DAY(timestamp) AS d
  FROM medias
  ORDER BY timestamp DESC;

-- how many days ago each media was taken (drops anything in the future)
SELECT _key, DATE_DIFF('day', NOW(), timestamp) AS daysAgo
  FROM medias
  WHERE daysAgo >= 0
  ORDER BY daysAgo;
```

A timestamp string compares directly against an ISO-8601 literal (and against `NOW()` or a date
function's result), so range filters read naturally:

```sql
SELECT _key FROM medias WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2025-01-01';
SELECT _key FROM medias WHERE timestamp <= NOW();
```

Combined with [grouping by an expression](#grouped-aggregates), the extraction functions drive
calendar roll-ups:

```sql
-- count medias per year
SELECT YEAR(timestamp) AS year, COUNT(*) AS n
  FROM medias
  GROUP BY year
  ORDER BY year;

-- per year and month
SELECT YEAR(m.timestamp) AS dy, MONTH(m.timestamp) AS dm, COUNT(*) AS n
  FROM medias m
  GROUP BY dy, dm
  ORDER BY dy, dm;
```

{: .note }
`NOW()` is the wall clock at evaluation time; treat it as "approximately now" rather than a single
fixed instant pinned for the whole query.

#### Null handling and conversion

| Function | Result |
|---|---|
| `COALESCE(a, b, …)` | the first non-`NULL` argument, or `NULL` if all are `NULL` |
| `NULLIF(a, b)` | `NULL` when `a` equals `b`, otherwise `a` |
| `CAST(x AS <type>)` | `x` converted to `<type>`; `NULL` (or an unconvertible value) stays `NULL` |

`CAST` target types: `integer` (also `int`/`bigint`/`long`/`smallint`), `double` (also `float`/`real`),
`decimal` (also `numeric`/`number`), `string` (also `text`/`varchar`/`char`), `boolean` (also `bool`),
and `timestamp` (also `datetime`/`date`). Numeric parsing is lenient (a numeric string converts), and a
decimal/double truncates toward zero when cast to an integer.

```sql
SELECT _key, COALESCE(nickname, name, '(anonymous)') AS display FROM users;
SELECT NULLIF(status, 'unknown') AS status FROM jobs;          -- 'unknown' becomes NULL
SELECT _key FROM orders WHERE CAST(amount AS integer) >= 100;
SELECT CAST(age AS string) AS ageText, CAST('2024-01-01' AS timestamp) AS d FROM users;
```

### Arithmetic

Numeric expressions support `+`, `-`, `*`, `/`, and `%`, with the usual precedence (`*` `/` `%` bind
tighter than `+` `-`) and parentheses to override it. They may appear anywhere an expression is
allowed — `SELECT`, `WHERE`, `HAVING`, `ORDER BY` — and combine freely with functions and aliases:

```sql
SELECT _key, amount * 2 AS doubled FROM orders WHERE amount * 2 >= 20;

-- distance in kilometres, reusing the alias in WHERE and ORDER BY
SELECT b.name, GEO_DISTANCE(m.location, 48.8566, 2.3522) / 1000 AS distKm
  FROM medias m
  JOIN bags b ON m.bagId = b.id
  WHERE distKm <= 10
  ORDER BY distKm;
```

Integer operands keep an integer result for `+`/`-`/`*`/`%`; division always yields a decimal. A
non-numeric operand, or division/modulo by zero, yields `NULL` (which then fails comparisons, so the
row is excluded).

### CASE

`CASE` is a conditional expression and may appear anywhere an expression is allowed — projection,
`WHERE`, `HAVING`, `ORDER BY`, and as a `GROUP BY` bucket. Both standard forms are supported. The
**searched** form tests a boolean condition per branch; the **simple** form compares a subject value
to each branch value for equality. The first matching branch wins; with no match it yields the `ELSE`
value, or `NULL` when there is no `ELSE`.

```sql
-- searched CASE
SELECT _key,
       CASE WHEN age >= 65 THEN 'senior'
            WHEN age >= 18 THEN 'adult'
            ELSE 'minor' END AS band
  FROM users;

-- simple CASE (subject compared for equality)
SELECT _key,
       CASE country WHEN 'FR' THEN 'France'
                    WHEN 'US' THEN 'United States'
                    ELSE country END AS countryName
  FROM users;

-- as a GROUP BY bucket, referenced by its alias
SELECT CASE WHEN amount >= 100 THEN 'big' ELSE 'small' END AS bucket, COUNT(*) AS n
  FROM orders
  GROUP BY bucket
  ORDER BY bucket;

-- in WHERE
SELECT _key FROM users WHERE (CASE WHEN active THEN age ELSE 0 END) >= 18;
```

### ORDER BY and LIMIT

`ORDER BY` accepts a column, an output alias, or an arbitrary expression (such as a geo distance), and
takes **several comma-separated keys** applied left-to-right, each with its own `ASC`/`DESC`.

```sql
SELECT _key, age FROM users ORDER BY age;          -- ascending (default)
SELECT _key, age FROM users ORDER BY age DESC;     -- descending
SELECT * FROM users ORDER BY country, age DESC;    -- by country, then age within each
SELECT * FROM users ORDER BY _key LIMIT 10;
SELECT name AS n FROM users ORDER BY n;             -- order by an alias
SELECT * FROM originals o
  ORDER BY GEO_DISTANCE(o.location.latitude, o.location.longitude, 48.8566, 2.3522) LIMIT 10;  -- 10 nearest
```

### SELECT DISTINCT

De-duplicates the projected rows (buffers, like `ORDER BY`).

```sql
SELECT DISTINCT country FROM users;
SELECT DISTINCT city, country FROM users ORDER BY country LIMIT 20;
```

---

## Aggregates and GROUP BY

Aggregate functions: `COUNT(*)`, `COUNT(x)` (non-null values), `SUM(x)`, `AVG(x)`, `MIN(x)`,
`MAX(x)`. The argument `x` is **any expression**, not just a column — `SUM(price * qty)`,
`AVG(amount + tax)`, `MAX(YEAR(timestamp))` all work. A leading `DISTINCT` dedupes the argument
values first, e.g. `COUNT(DISTINCT customer)` or `SUM(DISTINCT amount)`. Each output column is named
`func(arg)` unless you give it an `AS` alias.

```sql
SELECT SUM(amount * 2) AS doubled, AVG(amount + 10) AS bumped FROM orders;
SELECT COUNT(DISTINCT customer) AS customers FROM orders;
SELECT customer, COUNT(DISTINCT amount) AS distinctAmounts FROM orders GROUP BY customer;
```

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

A `GROUP BY` key may be **any expression**, not just a column — a function call, an arithmetic
expression, a nested path — and it may be written out in full or referenced by the `SELECT` alias it
defines (a convenience beyond standard SQL). This is what makes calendar roll-ups read cleanly:

```sql
-- group by a function, referenced through its alias
SELECT YEAR(timestamp) AS year, COUNT(*) AS n
  FROM medias
  GROUP BY year
  ORDER BY year;

-- equivalently, spell the expression out in GROUP BY
SELECT YEAR(timestamp) AS year, COUNT(*) AS n
  FROM medias
  GROUP BY YEAR(timestamp);
```

A projected expression may also combine aggregates, e.g. `SUM(amount) / COUNT(*) AS mean`. Every
non-aggregated column in the projection (or in `HAVING`/`ORDER BY`) must be covered by `GROUP BY`.
Groups are ordered by their key by default (so output is deterministic without an explicit
`ORDER BY`). The fold keeps one accumulator set per group, so memory scales with the number of
groups, not rows.

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

## JOIN

Two or more collections can be joined. Tables may take an alias (`FROM sales s`, `JOIN customers c`),
and columns are then referenced qualified (`s.amount`, `c.name`). `INNER JOIN` (the default for a bare
`JOIN`) keeps only matched rows; `LEFT [OUTER] JOIN` keeps every left row, filling the right side with
`NULL` when there is no match. The `ON` condition is a conjunction of equalities (anything else becomes
a residual filter).

```sql
-- match a value field to the other collection's key
SELECT s._key, c.name, s.amount
  FROM sales s
  JOIN customers c ON s.customerId = c._key
  ORDER BY s._key;

-- keep sales whose customer is missing (c.name is NULL for them)
SELECT s._key, c.name
  FROM sales s
  LEFT JOIN customers c ON s.customerId = c._key;

-- aggregate across a join
SELECT c.country, COUNT(*) AS n, SUM(s.amount) AS total
  FROM sales s
  JOIN customers c ON s.customerId = c._key
  GROUP BY c.country
  ORDER BY total DESC;

-- join two value fields
SELECT c.name, m.tier
  FROM customers c
  JOIN markets m ON c.country = m.country;

-- chained joins
SELECT a._key, b.label, c.tier
  FROM a
  JOIN b ON a.bId = b._key
  JOIN c ON a.cId = c._key;
```

### Key-type coercion

The join key is compared by value. Because a `_key`'s exact datatype is known (from its key codec),
when one side of an equality is a `_key`, the **other side is converted to the key's type**. So a value
field stored as the string `"100"` joins to a `Long` key `100`:

```sql
-- refs.itemCode is the string "100"; items is keyed by Long
SELECT r._key, i.label
  FROM refs r
  JOIN items i ON r.itemCode = i._key;
```

{: .note }
When **both** sides are value fields (no `_key` involved), no conversion happens — the values must be of
the same type to match (e.g. two `String` columns). `NULL` never joins.

{: .warning }
A join buffers both sides (hash join), so it is not a streaming operation. Put the smaller collection on
the right (the built side). `SELECT *` over a join emits every column from every table, **qualified**
(`s._key`, `c.name`, …) to avoid name collisions.

## Clause order

The dialect follows standard SQL order. Writing clauses out of order is a parse error.

```sql
SELECT [DISTINCT] <projection>
  FROM <collection> [[AS] <alias>]
  [[INNER|LEFT [OUTER]] JOIN <collection> [[AS] <alias>] ON <equalities>]...
  [WHERE <condition>]
  [GROUP BY <expression> [, <expression>]...]
  [HAVING <condition>]
  [ORDER BY <expression> [ASC|DESC] [, <expression> [ASC|DESC]]...]
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

`SHOW INDEXES` also lists each index's declared mapping (its source collection and the fields its
key is built from), when one has been declared:

```text
lmdb(my-app)> SHOW INDEXES;
 name             | source | on
------------------+--------+---------------
 mediaByBag       | medias | bagId
 mediaByTimestamp | medias | timestamp, _key
(2 rows)
```

---

## Query planning and indexes

Declared indexes (attached in code with
[`withDeclaredIndex`](lmdb-index.html#withdeclaredindex)) persist a machine-readable mapping —
which collection they index and which record fields feed each component of their key. The engine
uses those mappings to answer queries without scanning the whole collection:

- **Equality / `BETWEEN` / `<` `<=` `>` `>=` pushdown** — `WHERE` conjuncts on indexed fields are
  matched against each index by the **leftmost-prefix rule** (equalities on the leading key
  components, then at most one range on the next component) and become a byte-range scan over the
  index, followed by point-fetches of the matching records.
- **`ORDER BY` / `LIMIT` in index order** — when the `ORDER BY` columns (all ascending) follow the
  index's remaining key components, rows stream in index order and the sort buffer is skipped, so
  `ORDER BY timestamp LIMIT 50` touches ~50 index entries instead of the whole collection.
- **Primary-key pushdown** — `_key = …` / `_key IN (…)` become point fetches, and `_key` ranges
  become a cursor range scan over the collection's own key order.
- Everything the scan cannot serve stays a **residual filter**, evaluated per fetched row. Pushed
  conjuncts are *not* re-evaluated: the index component's key ordering (e.g. instant semantics for a
  timestamp component) is authoritative for them. A `coalesce(...)` component is the exception — its
  scan is a superset, so its conjunct is re-checked.

Only conjuncts of the form `column <op> literal` combined with `AND` are pushable; an `OR`, a
function call, or a comparison between two columns falls back to a full scan (possibly combined with
the pushable conjuncts around it). Indexes attached with the opaque `withIndex`/`withIndexFull`
lambdas have no mapping and are never planned.

`EXPLAIN <select>` shows the chosen access path without executing:

```text
lmdb(my-app)> EXPLAIN SELECT * FROM medias WHERE bagId = '018f...' ORDER BY timestamp LIMIT 50;
 step   | detail
--------+------------------------------------------------------
 access | index range scan on mediaByBag [eq(bagId) ordered]
 lookup | fetch matching records from medias by primary key
 order  | scan order matches ORDER BY (no sort)
 filter | none
(4 rows)
```

{: .note }
SQL writes (`INSERT`/`UPDATE`/`DELETE`) do **not** maintain declared indexes — index updaters are
attached to the typed collection facade in application code. Mutate indexed collections through the
application, or rebuild indexes (`rebuildIndexes()`) after SQL-side writes.

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

**Supported:** `SELECT` (`*`, columns, dotted nested-field paths, scalar/geo/date-time function
expressions, aggregates, expressions over aggregates, `AS` aliases), `DISTINCT`, `INNER`/`LEFT JOIN`
(with table aliases, qualified columns, and value→key coercion), `WHERE` (`= != <> < <= > >=`,
`AND`/`OR`/`NOT`, parentheses, `LIKE`/`NOT LIKE`, `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`,
`IS [NOT] NULL`, timestamp comparison), `CASE` (searched and simple forms), arithmetic
(`+ - * / %` with precedence and parentheses), scalar functions (`LENGTH`, `UPPER`/`LOWER`,
`TRIM`/`LTRIM`/`RTRIM`, `SUBSTR`/`SUBSTRING`, `CONCAT`, `REPLACE`, `INSTR`, `COALESCE`, `NULLIF`,
`CAST`, `ABS`/`FLOOR`/`CEIL`/`ROUND`/`SIGN`/`MOD`/`POWER`/`SQRT`, `GEO_DISTANCE`,
`GEO_WITHIN`, `YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/`SECOND`, `DATE_DIFF`, `NOW`) usable in
`SELECT`/`WHERE`/`HAVING`/`ORDER BY`, `GROUP BY` by column/alias/expression, `HAVING`, multi-key
`ORDER BY` by column/alias/expression (`ASC`/`DESC`), `LIMIT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` over
arbitrary expression arguments with optional `DISTINCT` (e.g. `SUM(a * b)`, `COUNT(DISTINCT x)`),
`INSERT`/`UPDATE`/`DELETE`, `DESCRIBE`, `SHOW COLLECTIONS`/`SHOW INDEXES`, `EXPLAIN`, the
`_key`/`_value` pseudo-columns, `AS` aliases referenceable in `WHERE`/`HAVING`/`GROUP BY`/`ORDER BY`,
and index-aware planning over declared indexes (equality/range pushdown, `ORDER BY` in index order,
primary-key pushdown).

**Not (yet) supported:** `RIGHT`/`FULL`/`CROSS` joins, non-equi join conditions as the *only*
predicate, subqueries, `UNION`, window functions, unary minus on a non-literal, user-defined
scalar functions beyond the built-ins above, time-zone-aware date handling (calendar fields are read
in UTC), writing into nested fields, and DDL (`CREATE`/`DROP`). Identifiers are
letters/digits/underscore; keywords are case-insensitive.
