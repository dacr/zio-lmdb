# Lightning Memory Database (LMDB) for ZIO [![][ZIOLMDBManagerImg]][ZIOLMDBManagerLnk]

This is a work in progress software library, all current tests are OK, first preliminary releases are available
on maven central.

## Goal

Simple embedded database API, easy to use, with features dedicated to changes follow-up. "changes follow-up" mean that
the API provide you everything you need to know what is done by LMDB, an update mean you'll receive back both the 
previous and the newest record.

The first API only provides atomic operations with hidden transactions. API is designed to not lie, all functions signatures
describe precisely what you must expect from them, thanks to [ZIO][ZIO] and [Scala3][Scala3].  

## Usage example

```scala
test("basic usage")(
  for {
    _             <- LMDB.databaseCreate("example")
    record         = Record("John Doe", 42)
    recordId      <- Random.nextUUID.map(_.toString)
    updateState   <- LMDB.upsertOverwrite[Record]("example", recordId, record)
    gotten        <- LMDB.fetch[Record]("example", recordId).some
    deletedRecord <- LMDB.delete[Record]("example", recordId)
    gotNothing    <- LMDB.fetch[Record]("example", recordId)
  } yield assertTrue(
    updateState.previous.isEmpty,
    updateState.current == record,
    gotten == record,
    deletedRecord.contains(record),
    gotNothing.isEmpty
  )
)
```

## Requirements

When LVMDB is used for as persistence store with recent JVM it requires JVM some options :

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

[ZIOLMDBManager]:    https://github.com/dacr/zio-lmdb
[ZIOLMDBManagerImg]: https://img.shields.io/maven-central/v/fr.janalyse/zio-lmdb_3.svg
[ZIOLMDBManagerLnk]: https://search.maven.org/#search%7Cga%7C1%7Cfr.janalyse.zio-lmdb
[ZIO]: https://zio.dev/
[Scala3]: https://docs.scala-lang.org/scala3/reference/