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


## Configuration

Configuration is based on the standard ZIO config mechanism, the default configuration provider uses environnment variables
or java properties to resolve this library configuration parameters.

 
| Configuration key   | Environment variable | Description                                                    | Default value    |
|---------------------|----------------------|----------------------------------------------------------------|------------------|
| lmdb.name           | LMDB_NAME            | Database name, which will be also used as the directory name   | default          |
| lmdb.home           | LMDB_HOME            | Where to store the database directory                          | $HOME/.lmdb      |
| lmdb.sync           | LMDB_SYNC            | Synchronize the file system with all database write operations | false            |
| lmdb.maxReaders     | LMDB_MAXREADERS      | The maximum number of readers                                  | 100              |
| lmdb.maxCollections | LMDB_MAXCOLLECTIONS  | The maximum number of collections which can be created         | 10_000           |
| lmdb.mapSize        | LMDB_MAPSIZE         | The maximum size of the whole database including metadata      | 100_000_000_000L |


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

To run the previous logic, you'll have to provide the LMDB layer, two layers are available :
- `LMDB.live` : Fully configurable using standard zio-config
- `LMDB.liveWithDatabaseName("chosen-database-name")` : to override/force the database name
  (quite useful when writing scala scripts)

Code snippets, runnable using scala-cli :
- [ZIO LMDB simple example (scala-3)](https://gist.github.com/dacr/dcb8a11f095ef0a2a95c24701e6eb804)
- [ZIO LMDB simple example (scala-2)](https://gist.github.com/dacr/9d2c4171d1b1e7a40a244ef456725d25)
- [ZIO LMDB feeding with French town postal codes](https://gist.github.com/dacr/6d24baf827ae0c590133e0f27f1ef20b)
- [ZIO LMDB using custom configuration provider](https://gist.github.com/dacr/790df1705c7ec19ae2fe4098dad8d762)

ZIO-LMDB based Applications :
- [zwords][ZWORDS-CODE] which can be played [zwords game][ZWORDS-LIVE]
- [code-examples-manager][CEM] zio-lmdb integration is in progress...

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

## Contributors :)

- [François Armand](https://github.com/fanf) : for scala 2.13 support initiative


[ZIOLMDBManager]:    https://github.com/dacr/zio-lmdb
[ZIOLMDBManagerImg]: https://img.shields.io/maven-central/v/fr.janalyse/zio-lmdb_3.svg
[ZIOLMDBManagerLnk]: https://mvnrepository.com/artifact/fr.janalyse/zio-lmdb
[ZIO]: https://zio.dev/
[Scala3]: https://docs.scala-lang.org/scala3/reference/
[JLMDB]: https://github.com/lmdbjava/lmdbjava
[LMDB]: https://www.symas.com/lmdb
[ZIO-ULID]: https://zio-ulid.bilal-fazlani.com/
[UUID]: https://en.wikipedia.org/wiki/Universally_unique_identifier
[ZWORDS-CODE]: https://github.com/dacr/zwords
[ZWORDS-LIVE]: https://zwords.mapland.fr/
[CEM]: https://github.com/dacr/code-examples-manager