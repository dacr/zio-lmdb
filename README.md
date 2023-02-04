# Lightning Memory Database (LMDB) for ZIO [![][ZIOLMDBManagerImg]][ZIOLMDBManagerLnk]

This is a work in progress software library, all current tests are OK, first preliminary releases are available
on maven central.

## Goal

Simple embedded database, easy to use opinionated API, with features dedicated to changes follow-up.
"changes follow-up" means that the API provide you everything you need to know about what is done by LMDB,
for example an update mean you'll receive back both the previous and the newest record.

The first API only provides atomic operations with hidden transactions. API is designed to not lie, all functions signatures
describe precisely what you must expect from them, thanks to [ZIO][ZIO] and [Scala3][Scala3].  

## Definitions

For better understanding, this library use a slightly different vocabulary from LMDB original one :  
- **Database** :  (*LMDB talk about Environment*)
  - The place where the database file is stored on your file system
  - A set of configuration for this database (expected maximum size, expected collection number)
- **Collection** : (*LMDB talk about Database*) 
  - A sorted map where to store your data
  - One database contains multiple collection
- **Transaction** : (*the same for LMDB*)
  - for global coherency within the same database
  - only one simultaneous write access is possible within the same database 


## Usage example

```scala
test("basic usage")(
  for {
    collection        <- LMDB.collectionCreate[Record]("example")
    record             = Record("John Doe", 42)
    recordId          <- Random.nextUUID.map(_.toString)
    updatedState      <- collection.upsert(recordId, previousRecord => record)
    gotten            <- collection.fetch(recordId).some
    deletedRecord     <- collection.delete(recordId)
    gotNothing        <- collection.fetch(recordId)
  } yield assertTrue(
    updateStated.previous.isEmpty,
    updateStated.current == record,
    gotten == record,
    deletedRecord.contains(record),
    gotNothing.isEmpty
  )
)
```

## Requirements

When LVMDB is used as persistence store with recent JVM it requires some JVM options :

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

[ZIOLMDBManager]:    https://github.com/dacr/zio-lmdb
[ZIOLMDBManagerImg]: https://img.shields.io/maven-central/v/fr.janalyse/zio-lmdb_3.svg
[ZIOLMDBManagerLnk]: https://search.maven.org/#search%7Cga%7C1%7Cfr.janalyse.zio-lmdb
[ZIO]: https://zio.dev/
[Scala3]: https://docs.scala-lang.org/scala3/reference/