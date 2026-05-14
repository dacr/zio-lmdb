---
title: Configuration
nav_order: 8
---

# Configuration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

ZIO-LMDB uses the standard [ZIO Config](https://zio.dev/zio-config/) mechanism. The default configuration provider reads from **environment variables** or **Java system properties**.

---

## Parameters

| Config key | Environment variable | Default | Description |
|---|---|---|---|
| `lmdb.name` | `LMDB_NAME` | `default` | Database name; also used as the directory name under `lmdb.home` |
| `lmdb.home` | `LMDB_HOME` | `$HOME/.lmdb` | Parent directory where the database directory is created |
| `lmdb.sync` | `LMDB_SYNC` | `false` | Flush all writes to disk synchronously (safe but slower) |
| `lmdb.maxReaders` | `LMDB_MAXREADERS` | `1000` | Maximum number of concurrent read transactions |
| `lmdb.maxCollections` | `LMDB_MAXCOLLECTIONS` | `10000` | Maximum number of collections (and indexes) that can be created |
| `lmdb.mapSize` | `LMDB_MAPSIZE` | `100000000000` | Maximum database size in bytes (100 GB default) |

{: .note }
LMDB uses a memory-mapped file. `mapSize` is the **virtual address space** reserved, not the actual disk usage. You can set it large safely — the file only grows as data is written.

---

## Layers

### LMDB.live

Reads all parameters from the environment using ZIO Config's default provider (environment variables or system properties):

```scala
object MyApp extends ZIOAppDefault:
  def run =
    myProgram.provide(LMDB.live, Scope.default)
```

Configure via environment variables before launching:

```bash
LMDB_NAME=my-app \
LMDB_HOME=/var/data \
LMDB_MAXREADERS=200 \
java -jar my-app.jar
```

### LMDB.liveWithDatabaseName

Overrides only the database name; all other parameters still come from the environment:

```scala
myProgram.provide(LMDB.liveWithDatabaseName("my-database"), Scope.default)
```

Useful in tests or small scripts where the name must be fixed in code.

---

## Programmatic configuration

Provide a custom `LMDBConfig` to bypass environment-variable resolution entirely:

```scala
import zio.lmdb.*

val customLayer = LMDB.liveWithConfig(
  LMDBConfig(
    databaseName  = "my-app",
    databasesHome = Some("/var/data/lmdb"),
    maxReaders    = 500,
    mapSize       = BigInt(50_000_000_000L)   // 50 GB
  )
)

myProgram.provide(customLayer, Scope.default)
```

---

## Database location

The database directory is:

```
$LMDB_HOME / $LMDB_NAME /
```

For example, with the defaults:

```
$HOME/.lmdb/default/
```

LMDB creates two files inside this directory:
- `data.mdb` — the database data file
- `lock.mdb` — the reader lock table

---

## Filesystem sync

By default (`lmdb.sync = false`), writes are applied to the memory-mapped file but not immediately flushed to disk. The OS will flush asynchronously. This is the standard LMDB setting: on a crash the OS may lose a few seconds of writes, but the database will never be corrupted.

Set `lmdb.sync = true` if you need **durability** guarantees on every write at the cost of significantly lower write throughput.

---

## Operating the database

The `lmdb-utils` package provides standard command-line tools:

```bash
# Install (Debian/Ubuntu)
sudo apt-get install lmdb-utils

# Database statistics (all collections)
mdb_stat -a $HOME/.lmdb/my-app/

# Dump a collection as text
mdb_dump -p -s my-collection $HOME/.lmdb/my-app/

# Restore a collection from a dump
mdb_load -s my-collection $HOME/.lmdb/my-app/ < dump.txt
```

Since ZIO-LMDB stores values as JSON by default, dumps are human-readable plain text that can be edited and reloaded — ideal for simple data migrations.
