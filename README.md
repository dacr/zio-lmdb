# Lightning Memory Database (LMDB) for ZIO
[![][ZIOLMDBManagerImg]][ZIOLMDBManagerLnk] [![scaladoc][ScalaDocImg]][ScalaDoc]

Why ZIO-lmdb? Because I wanted a straightforward **embedded** (in the same process) ACID database for small
applications while keeping deployment, maintenance, upgrades as simple as possible.

ZIO-lmdb is based on the powerful [lmdb-java][JLMDB] library and brings a higher level API to enhance the developer experience.

So ZIO-lmdb is an embedded key/value database with an easy-to-use opinionated API.
Choices have been made to make the developer experience as simple as possible :
- JSON-based default storage using zio-json,
  - *Custom serialization is supported*
- Safe update by using a lambda which will be called with the previous value if it exists and returns the new value,
- Identifiers are managed by the developer, just use [UUID][UUID] or [ULID][ZIO-ULID].
  - Remember that identifiers are automatically lexicographically sorted :)

API is designed to not lie. All functions signatures describe precisely
what you must expect from them, thanks to [ZIO][ZIO] and [Scala3][Scala3].  

## Definitions

For a better understanding, this library uses slightly different vocabulary from LMDB original one :  
- **Database** : (*LMDB talk about Environment*)
  - The place where the database file is stored on your file system
  - A set of configurations for this database (expected maximum size, expected collection number)
- **Collection** : (*LMDB talk about Database*) 
  - A sorted Map ([B+ Tree][btree]) where your data is stored
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
//> using scala 3.8.1
//> using dep fr.janalyse::zio-lmdb:2.3.1
//> using javaOpt --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED

import zio.*, zio.json.*, zio.lmdb.*, zio.lmdb.json.*
import java.io.File, java.util.UUID, java.time.OffsetDateTime

case class Record(uuid: UUID, name: String, age: Int, addedOn: OffsetDateTime) derives LMDBCodecJson

object SimpleExample extends ZIOAppDefault {
  override def run = example.provide(LMDB.liveWithDatabaseName("lmdb-data-simple-example"), Scope.default)

  val collectionName = "examples"
  val example        = for {
    examples  <- LMDB.collectionCreate[UUID,Record](collectionName, failIfExists = false)
    recordId  <- Random.nextUUID
    dateTime  <- Clock.currentDateTime
    record     = Record(recordId, "John Doe", 42, dateTime)
    _         <- examples.upsertOverwrite(recordId, record)
    gotten    <- examples.fetch(recordId).some
    collected <- examples.collect()
    _         <- Console.printLine(s"collection $collectionName contains ${collected.size} records")
    _         <- ZIO.foreachDiscard(collected)(record => Console.printLine(record))
    lmdb      <- ZIO.service[LMDB]
    _         <- Console.printLine("""LMDB standard tools can be used to manage the database content : sudo apt-get install lmdb-utils""")
    _         <- Console.printLine(s"""To get some statistics     : mdb_stat -s $collectionName ${lmdb.databasePath}/""")
    _         <- Console.printLine(s"""To dump collection content : mdb_dump -p -s $collectionName ${lmdb.databasePath}/""")
  } yield ()
}

SimpleExample.main(Array.empty)
```

To run the such application logic, you'll have to provide the LMDB layer. Two layers are available :
- `LMDB.live` : Fully configurable using standard zio-config
- `LMDB.liveWithDatabaseName("chosen-database-name")` : to override/force the database name
  (quite useful when writing scala scripts)

### ZIO-LMDB based Applications
- [sotohp - photos management][SOTOHP] which uses zio-lmdb intensively
- [code-examples-manager - snippets/gists management][CEM] lmdb used for caching and data sharing
- [zwords - wordle like game][ZWORDS-CODE] which can be played [zwords game][ZWORDS-LIVE]

### Code snippets using ZIO-LMDB, runnable with [scala-cli][SCL]
- [ZIO LMDB simple example (scala-3)](https://gist.github.com/dacr/dcb8a11f095ef0a2a95c24701e6eb804)
- [ZIO LMDB feeding with French town postal codes](https://gist.github.com/dacr/6d24baf827ae0c590133e0f27f1ef20b)
- [ZIO LMDB using custom configuration provider](https://gist.github.com/dacr/790df1705c7ec19ae2fe4098dad8d762)
- [Extract photos records from elasticsearch and save them into LMDB](https://gist.github.com/dacr/6ea121f251ad316a64657cbe78085ab7)
- [Export code examples and executions results from lmdb to elastisearch](https://gist.github.com/dacr/f25da8222b2ac644c3195c5982b7367e)

## Operating lmdb databases

LMDB standard tools can be used to manage the LMDB database content : `sudo apt-get install lmdb-utils`
- to get some database statistics : `mdb_stat -a database_directory_path/`
- to dump the content of a database : `mdb_dump -a -p database_directory_path/`
- to dump the content of a database collection : `mdb_dump -s collectionName -p database_directory_path/`
- to restore some collection or the entire database use the command named `mdb_load` which uses the same format as for `mdb_dump` 

As zio-lmdb is using JSON format, dumps are just text, which can be edited and then loaded back. So simple data migration is straightforward.

## Requirements

When LVMDB is used as a persistence store with recent JVM, it requires some JVM options :

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
[SOTOHP]: https://github.com/dacr/sotohp
[SCL]: https://scala-cli.virtuslab.org/
[ScalaDocImg]: https://javadoc.io/badge2/fr.janalyse/zio-lmdb_3/scaladoc.svg
[ScalaDoc]: https://javadoc.io/doc/fr.janalyse/zio-lmdb_3/latest/zio/lmdb/LMDB$.html
[btree]: https://en.wikipedia.org/wiki/B%2B_tree