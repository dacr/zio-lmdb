<a href="https://dacr.github.io/zio-lmdb/"><img src="docs/logo-horizontal.svg" alt="ZIO-LMDB" width="480" height="144"></a>

[![][ZIOLMDBManagerImg]][ZIOLMDBManagerLnk] [![scaladoc][ScalaDocImg]][ScalaDoc] [![docs][DocsImg]][Docs]

Embedded, ACID, key-value database for [ZIO][ZIO] — zero infrastructure, zero ops, just a file.

Built on [lmdb-java][JLMDB] with a type-safe, ZIO-native API:
- **Three collection kinds** — `LMDBCollection` (1 key → 1 value), `LMDBMulti` (1 key → N values), `LMDBIndex` (1 key → N keys)
- **Customizable codecs** — JSON (`derives LMDBCodecJson`), bytes optimized codecs for keys, or your own ones
- **Atomic transactions** — single-collection or cross-collection, always consistent
- **Lexicographic ordering** — keys are sorted; range scans and pagination come for free
- **Scala-CLI friendly** — add one dependency line and run

## Install

```scala
// sbt
libraryDependencies += "fr.janalyse" %% "zio-lmdb" % "2.8.6"

// scala-cli
//> using dep fr.janalyse::zio-lmdb:2.8.6
//> using javaOpt --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

## Quick example

```scala
import zio.*, zio.lmdb.*, zio.lmdb.json.*
import java.util.UUID

case class Person(name: String, age: Int) derives LMDBCodecJson

object Example extends ZIOAppDefault:
  def run = (for {
    people <- LMDB.collectionCreate[UUID, Person]("people", failIfExists = false)
    id     <- Random.nextUUID
    _      <- people.insert(id, Person("Alice", 30))
    _      <- people.update(id, previous => previous.copy(age = previous.age + 1))
    result <- people.fetch(id)
    _      <- Console.printLine(result)
  } yield ()).provide(LMDB.liveWithDatabaseName("my-app"), Scope.default)
```

## Documentation

Full API reference, transactions, codecs, indexes, query DSL and configuration:  
**[dacr.github.io/zio-lmdb][Docs]**

## Real-world usage

- [sotohp][SOTOHP] — photo management, uses zio-lmdb intensively
- [code-examples-manager][CEM] — snippets and gist management
- [zwords][ZWORDS-CODE] — a Wordle-like game ([play it][ZWORDS-LIVE])

## Runnable snippets ([scala-cli][SCL])

- [CRUD example](https://gist.github.com/dacr/dcb8a11f095ef0a2a95c24701e6eb804)
- [Transaction example](https://gist.github.com/dacr/f69159308f971361a2643393d4b9bf3f)
- [French postal codes](https://gist.github.com/dacr/6d24baf827ae0c590133e0f27f1ef20b)
- [Custom configuration provider](https://gist.github.com/dacr/790df1705c7ec19ae2fe4098dad8d762)
- [Elasticsearch → LMDB import](https://gist.github.com/dacr/6ea121f251ad316a64657cbe78085ab7)
- [LMDB → Elasticsearch export](https://gist.github.com/dacr/f25da8222b2ac644c3195c5982b7367e)

[DocsImg]:           https://img.shields.io/badge/docs-GitHub%20Pages-blue
[Docs]:              https://dacr.github.io/zio-lmdb/
[ZIOLMDBManager]:    https://github.com/dacr/zio-lmdb
[ZIOLMDBManagerImg]: https://img.shields.io/maven-central/v/fr.janalyse/zio-lmdb_3.svg
[ZIOLMDBManagerLnk]: https://mvnrepository.com/artifact/fr.janalyse/zio-lmdb
[ZIO]:               https://zio.dev/
[JLMDB]:             https://github.com/lmdbjava/lmdbjava
[ZIO-ULID]:          https://zio-ulid.bilal-fazlani.com/
[ZWORDS-CODE]:       https://github.com/dacr/zwords
[ZWORDS-LIVE]:       https://zwords.mapland.fr/
[CEM]:               https://github.com/dacr/code-examples-manager
[SOTOHP]:            https://github.com/dacr/sotohp
[SCL]:               https://scala-cli.virtuslab.org/
[ScalaDocImg]:       https://javadoc.io/badge2/fr.janalyse/zio-lmdb_3/scaladoc.svg
[ScalaDoc]:          https://javadoc.io/doc/fr.janalyse/zio-lmdb_3/latest/zio/lmdb/LMDB$.html
