# Lightning Memory Database (LMDB) for ZIO [![][ZIOLMDBManagerImg]][ZIOLMDBManagerLnk]

Why ZIO-lmdb ? Because I wanted a very simple **embedded** (in the same process) ACID database for small
applications while keeping deployment, maintenance, upgrades as simple as possible.

ZIO-lmdb is based on the powerful [lmdb-java][JLMDB] library and bring a higher level API in order
to enhance the developer experience.

So ZIO-lmdb is an embedded key/value database, with an easy to use opinionated API, choices have been made
to make the developer experience as simple as possible :
- JSON based storage using zio-json,
- safe update by using a lambda which will be called with the previous value if it exists and returns the new value,
- identifiers are managed by the developer, just use [UUID][UUID] or [ULID][ZIO-ULID].

API is designed to not lie, all functions signatures describe precisely
what you must expect from them, thanks to [ZIO][ZIO] and [Scala3][Scala3].  

## Definitions

For a better understanding, this library use a slightly different vocabulary from LMDB original one :  
- **Database** :  (*LMDB talk about Environment*)
  - The place where the database file is stored on your file system
  - A set of configuration for this database (expected maximum size, expected collection number)
- **Collection** : (*LMDB talk about Database*) 
  - A sorted map where to store your data
  - One database contains multiple collection
- **Transaction** : (*the same for LMDB*)
  - for global coherency within the same database
  - only one simultaneous write access is possible within the same database 


## Usages example

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

Code snippets, runnable using scala-cli :
- [ZIO LMDB feeding with French town postal codes](https://gist.github.com/6d24baf827ae0c590133e0f27f1ef20b)
- [ZIO LMDB simple example](https://gist.github.com/dcb8a11f095ef0a2a95c24701e6eb804)

## Operating lmdb databases

LMDB standard tools can be used to manage the databases content : `sudo apt-get install lmdb-utils`
- to get some database statistics : `mdb_stat -a database_directory_path/`
- to dump the content of a database : `mdb_dump -a -p database_directory_path/`
- to dump the content of a database collection : `mdb_dump -s collectionName -p database_directory_path/`
- to restore some collection or the entire database use the command named `mdb_load` which uses the same format as for `mdb_dump` 

As zio-lmdb is using json format, dumps are just text, which can be edited and then loaded back. So simple data migration is straightforward.

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
[JLMDB]: https://github.com/lmdbjava/lmdbjava
[LMDB]: https://www.symas.com/lmdb
[ZIO-ULID]: https://zio-ulid.bilal-fazlani.com/
[UUID]: https://en.wikipedia.org/wiki/Universally_unique_identifier