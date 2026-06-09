/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package zio.lmdb.sql

import zio.*
import zio.test.*
import zio.lmdb.*
import zio.lmdb.json.{JValue, LMDBCodecJson}
import zio.lmdb.json.JValue.*
import zio.lmdb.schema.LMDBSchema
import zio.lmdb.sql.engine.SqlEngine
import zio.lmdb.sql.result.QueryResult

import java.time.Instant

object SqlEngineSpec extends ZIOSpecDefault {

  final case class Person(name: String, age: Long) derives LMDBCodecJson, LMDBSchema
  final case class Order(customer: String, amount: Long) derives LMDBCodecJson, LMDBSchema
  final case class Customer(name: String, country: String) derives LMDBCodecJson, LMDBSchema
  final case class Sale(customerId: String, amount: Long) derives LMDBCodecJson, LMDBSchema
  final case class Item(label: String) derives LMDBCodecJson, LMDBSchema
  final case class Ref(itemCode: String) derives LMDBCodecJson, LMDBSchema
  final case class Market(country: String, tier: String) derives LMDBCodecJson, LMDBSchema

  // Nested model (à la sotohp `originals`): exercises dotted paths into sub-objects.
  final case class Dim(width: Long, height: Long) derives LMDBCodecJson, LMDBSchema
  final case class GPoint(latitude: Double, longitude: Double, altitude: Double) derives LMDBCodecJson, LMDBSchema
  final case class Original(mediaPath: String, dimension: Dim, location: Option[GPoint]) derives LMDBCodecJson, LMDBSchema

  // Timestamped model: exercises date/time functions, comparisons and grouping by calendar fields.
  final case class Media(mediaPath: String, timestamp: Instant) derives LMDBCodecJson, LMDBSchema

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete(): Unit
  }

  private val lmdbLayer: ZLayer[Any, Throwable, LMDB] = ZLayer.scoped {
    for {
      dir  <- ZIO.acquireRelease(ZIO.attempt(java.nio.file.Files.createTempDirectory("lmdb-sql-test")))(p => ZIO.attempt(deleteRecursively(p.toFile)).ignore)
      lmdb <- LMDBLive.setup(LMDBConfig.default.copy(databasesHome = Some(dir.toString)))
    } yield lmdb
  }

  /** Run a SQL string and materialise its rows. */
  private def query(sql: String): ZIO[LMDB, SqlError, List[JValue]] =
    SqlEngine.run(sql).flatMap(_.toList)

  private def field(row: JValue, name: String): JValue =
    row match { case MapV(m) => m.getOrElse(name, NullV); case _ => NullV }

  private val seed =
    for {
      people <- LMDB.collectionCreate[String, Person]("people")
      _      <- people.upsertOverwrite("p1", Person("Alice", 30))
      _      <- people.upsertOverwrite("p2", Person("Bob", 25))
      _      <- people.upsertOverwrite("p3", Person("Carol", 40))
    } yield people

  private val seedOrders =
    for {
      orders <- LMDB.collectionCreate[String, Order]("orders")
      _      <- orders.upsertOverwrite("o1", Order("Alice", 10))
      _      <- orders.upsertOverwrite("o2", Order("Alice", 30))
      _      <- orders.upsertOverwrite("o3", Order("Bob", 5))
    } yield orders

  /** sales.customerId references a customers _key; s4 points at an absent customer (LEFT-join orphan). */
  private val seedSales =
    for {
      customers <- LMDB.collectionCreate[String, Customer]("customers")
      _         <- customers.upsertOverwrite("c1", Customer("Alice", "FR"))
      _         <- customers.upsertOverwrite("c2", Customer("Bob", "US"))
      sales     <- LMDB.collectionCreate[String, Sale]("sales")
      _         <- sales.upsertOverwrite("s1", Sale("c1", 10))
      _         <- sales.upsertOverwrite("s2", Sale("c1", 30))
      _         <- sales.upsertOverwrite("s3", Sale("c2", 5))
      _         <- sales.upsertOverwrite("s4", Sale("c3", 7))
    } yield ()

  /** refs.itemCode holds the numeric key as a *string*, to exercise value→key coercion. items keyed by Long. */
  private val seedItems =
    for {
      items <- LMDB.collectionCreate[Long, Item]("items")
      _     <- items.upsertOverwrite(100L, Item("Widget"))
      _     <- items.upsertOverwrite(200L, Item("Gadget"))
      refs  <- LMDB.collectionCreate[String, Ref]("refs")
      _     <- refs.upsertOverwrite("r1", Ref("100"))
      _     <- refs.upsertOverwrite("r2", Ref("200"))
    } yield ()

  private val seedMarkets =
    for {
      customers <- LMDB.collectionCreate[String, Customer]("customers")
      _         <- customers.upsertOverwrite("c1", Customer("Alice", "FR"))
      _         <- customers.upsertOverwrite("c2", Customer("Bob", "US"))
      markets   <- LMDB.collectionCreate[String, Market]("markets")
      _         <- markets.upsertOverwrite("m1", Market("FR", "A"))
      _         <- markets.upsertOverwrite("m2", Market("US", "B"))
    } yield ()

  private val seedOriginals =
    for {
      originals <- LMDB.collectionCreate[String, Original]("originals")
      _         <- originals.upsertOverwrite("o1", Original("/a.jpg", Dim(1920, 1080), Some(GPoint(48.85, 2.35, 35.0))))
      _         <- originals.upsertOverwrite("o2", Original("/b.jpg", Dim(800, 600), Some(GPoint(40.71, -74.0, 10.0))))
      _         <- originals.upsertOverwrite("o3", Original("/c.jpg", Dim(640, 480), None))
    } yield ()

  /** Paris landmarks (near), London (far), and one with no location, for geo-distance queries. */
  private val seedGeo =
    for {
      originals <- LMDB.collectionCreate[String, Original]("originals")
      _         <- originals.upsertOverwrite("louvre", Original("/louvre.jpg", Dim(1, 1), Some(GPoint(48.8606, 2.3376, 34.0))))   // ~1.2 km from ref
      _         <- originals.upsertOverwrite("eiffel", Original("/eiffel.jpg", Dim(1, 1), Some(GPoint(48.8584, 2.2945, 330.0))))  // ~4.2 km from ref
      _         <- originals.upsertOverwrite("london", Original("/london.jpg", Dim(1, 1), Some(GPoint(51.5074, -0.1278, 11.0))))  // ~343 km from ref
      _         <- originals.upsertOverwrite("nowhere", Original("/x.jpg", Dim(1, 1), None))                                       // excluded (null)
    } yield ()

  /** Timestamps spanning two years / three months, for date/time function and grouping queries. */
  private val seedMedias =
    for {
      medias <- LMDB.collectionCreate[String, Media]("medias")
      _      <- medias.upsertOverwrite("m1", Media("/m1.jpg", Instant.parse("2023-05-10T08:00:00Z")))
      _      <- medias.upsertOverwrite("m2", Media("/m2.jpg", Instant.parse("2024-01-15T10:30:00Z")))
      _      <- medias.upsertOverwrite("m3", Media("/m3.jpg", Instant.parse("2024-01-20T12:00:00Z")))
      _      <- medias.upsertOverwrite("m4", Media("/m4.jpg", Instant.parse("2024-03-05T09:15:00Z")))
      _      <- medias.upsertOverwrite("m5", Media("/m5.jpg", Instant.parse("2024-03-25T18:45:00Z")))
    } yield ()

  override def spec = suite("SqlEngine")(
    test("geo: filter within a radius, nearest-first, with projection and the object-arg form") {
      // Reference point: central Paris (Notre-Dame ~48.8566, 2.3522).
      for {
        _    <- seedGeo
        near <- query(
                  """SELECT _key, geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) AS dist
                    |FROM originals o
                    |WHERE geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) <= 50000
                    |ORDER BY dist""".stripMargin
                )
        obj  <- query(
                  """SELECT _key
                    |FROM originals o
                    |WHERE geo_within(o.location, 48.8566, 2.3522, 50000)
                    |ORDER BY _key""".stripMargin
                )
      } yield assertTrue(
        near.map(r => field(r, "_key")) == List(StringV("louvre"), StringV("eiffel")),         // nearest-first; London + null excluded
        near.map(r => field(r, "dist")).forall { case DoubleV(d) => d <= 50000.0; case _ => false },
        field(near.head, "dist").asInstanceOf[DoubleV].value < field(near(1), "dist").asInstanceOf[DoubleV].value, // ascending by distance
        obj.map(r => field(r, "_key")) == List(StringV("eiffel"), StringV("louvre"))           // object-arg GEO_WITHIN agrees (ordered by _key)
      )
    },
    test("date/time GROUP BY: aggregate by YEAR(timestamp) using the SELECT alias") {
      for {
        _    <- seedMedias
        rows <- query(
                  """SELECT year(timestamp) AS year, count(*)
                    |FROM medias m
                    |GROUP BY year
                    |ORDER BY year""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "year"))     == List(LongV(2023), LongV(2024)),
        rows.map(r => field(r, "count(*)")) == List(LongV(1), LongV(4))
      )
    },
    test("date/time GROUP BY: group by YEAR and MONTH, with a multi-key ORDER BY") {
      for {
        _    <- seedMedias
        rows <- query(
                  """SELECT year(m.timestamp) AS dy, month(m.timestamp) AS dm, count(*) AS n
                    |FROM medias m
                    |GROUP BY dy, dm
                    |ORDER BY dy, dm""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "dy")) == List(LongV(2023), LongV(2024), LongV(2024)),
        rows.map(r => field(r, "dm")) == List(LongV(5), LongV(1), LongV(3)),
        rows.map(r => field(r, "n"))  == List(LongV(1), LongV(2), LongV(2))
      )
    },
    test("date/time extraction functions (YEAR/MONTH/DAY) in a projection") {
      for {
        _    <- seedMedias
        rows <- query(
                  """SELECT _key, year(timestamp) AS y, month(timestamp) AS mo, day(timestamp) AS d
                    |FROM medias
                    |WHERE _key = 'm4'""".stripMargin
                )
      } yield assertTrue(
        field(rows.head, "y")  == LongV(2024),
        field(rows.head, "mo") == LongV(3),
        field(rows.head, "d")  == LongV(5)
      )
    },
    test("date/time comparison: filter by a timestamp bound and by NOW()") {
      for {
        _   <- seedMedias
        cut <- query(
                 """SELECT _key
                   |FROM medias
                   |WHERE timestamp >= '2024-01-01T00:00:00Z'
                   |ORDER BY timestamp""".stripMargin
               )
        pst <- query("select _key from medias where timestamp <= now() order by _key")
      } yield assertTrue(
        cut.map(r => field(r, "_key")) == List(StringV("m2"), StringV("m3"), StringV("m4"), StringV("m5")), // m1 (2023) excluded
        pst.map(r => field(r, "_key")) == List(StringV("m1"), StringV("m2"), StringV("m3"), StringV("m4"), StringV("m5"))
      )
    },
    test("date/time distance: DATE_DIFF in days, reusing the alias in WHERE and ORDER BY") {
      for {
        _    <- seedMedias
        rows <- query(
                  """SELECT _key, date_diff('day', timestamp, '2024-01-01T00:00:00Z') AS days
                    |FROM medias
                    |WHERE days >= 0
                    |ORDER BY days""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("m2"), StringV("m3"), StringV("m4"), StringV("m5")), // m1 (before the ref) is negative → excluded
        rows.map(r => field(r, "days")) == List(LongV(14), LongV(19), LongV(64), LongV(84))
      )
    },
    test("an AS alias is referenceable in WHERE (and ORDER BY)") {
      for {
        _    <- seedGeo
        rows <- query(
                  """SELECT _key, geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) AS dist
                    |FROM originals o
                    |WHERE dist <= 50000
                    |ORDER BY dist""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("louvre"), StringV("eiffel")),
        rows.map(r => field(r, "dist")).forall { case DoubleV(d) => d <= 50000.0; case _ => false }
      )
    },
    test("an AS alias is referenceable in HAVING") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT customer, count(*) AS n
                    |FROM orders
                    |GROUP BY customer
                    |HAVING n > 1
                    |ORDER BY customer""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "customer")) == List(StringV("Alice")),
        rows.map(r => field(r, "n"))        == List(LongV(2))
      )
    },
    test("an aggregate alias used in WHERE is still rejected (it belongs in HAVING)") {
      for {
        _    <- seedOrders
        exit <- query(
                  """SELECT customer, count(*) AS n
                    |FROM orders
                    |WHERE n > 1
                    |GROUP BY customer""".stripMargin
                ).exit
      } yield assert(exit)(Assertion.fails(Assertion.isSubtype[SqlError.Unsupported](Assertion.anything)))
    },
    test("arithmetic expressions in SELECT/WHERE/ORDER BY, reusing an AS alias (distance in km)") {
      for {
        _    <- seedGeo
        rows <- query(
                  """SELECT _key,
                    |       geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) / 1000 AS distKm
                    |FROM originals o
                    |WHERE distKm <= 10
                    |ORDER BY distKm""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("louvre"), StringV("eiffel")),         // london (~343 km) excluded
        rows.map(r => field(r, "distKm")).forall { case DecimalV(d) => d <= BigDecimal(10); case _ => false },
        field(rows.head, "distKm").asInstanceOf[DecimalV].value < field(rows(1), "distKm").asInstanceOf[DecimalV].value
      )
    },
    test("integer arithmetic stays integral and works in projection and WHERE") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT _key, amount * 2 AS doubled
                    |FROM orders
                    |WHERE amount * 2 >= 20
                    |ORDER BY amount""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key"))    == List(StringV("o1"), StringV("o2")),
        rows.map(r => field(r, "doubled")) == List(LongV(20), LongV(60))
      )
    },
    test("nested value paths: SELECT, WHERE and ORDER BY descend into sub-objects") {
      for {
        _    <- seedOriginals
        rows <- query(
                  """SELECT _key, o.location.altitude AS alt, o.dimension.width AS w
                    |FROM originals o
                    |WHERE o.dimension.width >= 800
                    |ORDER BY o.location.altitude DESC""".stripMargin
                )
        deep <- query("select location.altitude from originals where _key = 'o1'")
        miss <- query("select o.location.altitude as alt from originals o where _key = 'o3'")
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("o1"), StringV("o2")),       // o3 (width 640) excluded
        rows.map(r => field(r, "alt"))  == List(DecimalV(BigDecimal("35.0")), DecimalV(BigDecimal("10.0"))),
        rows.map(r => field(r, "w"))    == List(LongV(1920), LongV(800)),
        field(deep.head, "altitude")    == DecimalV(BigDecimal("35.0")),
        field(miss.head, "alt")         == NullV                                     // o3 has no location → nested path is NULL
      )
    },
    test("SELECT with WHERE, ORDER BY and projection over real stored data") {
      for {
        _    <- seed
        rows <- query("select _key, name, age from people where age >= 30 order by age")
      } yield assertTrue(
        rows.map(r => field(r, "name")) == List(StringV("Alice"), StringV("Carol")),
        rows.map(r => field(r, "age"))  == List(LongV(30), LongV(40)),
        rows.map(r => field(r, "_key")) == List(StringV("p1"), StringV("p3"))
      )
    },
    test("SELECT * exposes the _key pseudo-column and all value fields") {
      for {
        _    <- seed
        rows <- query("select * from people where _key = 'p2'")
      } yield assertTrue(
        rows.size == 1,
        field(rows.head, "_key") == StringV("p2"),
        field(rows.head, "name") == StringV("Bob"),
        field(rows.head, "age")  == LongV(25)
      )
    },
    test("LIMIT bounds the result") {
      for {
        _    <- seed
        rows <- query("select _key from people order by _key limit 2")
      } yield assertTrue(rows.map(r => field(r, "_key")) == List(StringV("p1"), StringV("p2")))
    },
    test("LIKE filters on a string column") {
      for {
        _    <- seed
        rows <- query("select name from people where name like 'A%'")
      } yield assertTrue(rows.map(r => field(r, "name")) == List(StringV("Alice")))
    },
    test("COUNT(*) counts all rows, and with WHERE counts the matches") {
      for {
        _   <- seed
        all <- query("select count(*) from people")
        big <- query("select count(*) from people where age >= 30")
        nun <- query("select count(*) from people where age > 100")
      } yield assertTrue(
        all.size == 1,
        field(all.head, "count(*)") == LongV(3),
        field(big.head, "count(*)") == LongV(2),
        field(nun.head, "count(*)") == LongV(0) // empty result is still one row, count 0
      )
    },
    test("COUNT(col) counts only non-null values of that column") {
      for {
        _   <- seed
        _   <- query("insert into people (_key, name) values ('p4', 'Dave')")
        age <- query("select count(age) from people")
        nme <- query("select count(name) from people")
      } yield assertTrue(field(age.head, "count(age)") == LongV(3), field(nme.head, "count(name)") == LongV(4))
    },
    test("SUM / AVG / MIN / MAX aggregate over rows") {
      for {
        _ <- seed
        r <- query("select sum(age), min(age), max(age) from people")
        a <- query("select avg(age) from people where age >= 30") // (30 + 40) / 2 = 35
      } yield assertTrue(
        field(r.head, "sum(age)") == DecimalV(BigDecimal(95)),
        field(r.head, "min(age)") == LongV(25),
        field(r.head, "max(age)") == LongV(40),
        field(a.head, "avg(age)") == DecimalV(BigDecimal(35))
      )
    },
    test("GROUP BY with COUNT and SUM, ordered by the group key") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT customer, count(*), sum(amount)
                    |FROM orders
                    |GROUP BY customer
                    |ORDER BY customer""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "customer"))    == List(StringV("Alice"), StringV("Bob")),
        rows.map(r => field(r, "count(*)"))     == List(LongV(2), LongV(1)),
        rows.map(r => field(r, "sum(amount)"))  == List(DecimalV(BigDecimal(40)), DecimalV(BigDecimal(5)))
      )
    },
    test("alias (AS count) and ORDER BY the alias") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT customer, count(*) AS count
                    |FROM orders
                    |GROUP BY customer
                    |ORDER BY count""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "count"))    == List(LongV(1), LongV(2)), // Bob:1, Alice:2 — ascending by the alias
        rows.map(r => field(r, "customer")) == List(StringV("Bob"), StringV("Alice"))
      )
    },
    test("LENGTH() is usable in WHERE") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT DISTINCT customer
                    |FROM orders
                    |WHERE length(customer) > 3
                    |ORDER BY customer""".stripMargin
                )
      } yield assertTrue(rows.map(r => field(r, "customer")) == List(StringV("Alice"))) // "Bob" has length 3
    },
    test("string functions: UPPER/LOWER, TRIM/LTRIM/RTRIM, and NULL propagation") {
      for {
        _    <- seed
        rows <- query(
                  """SELECT upper(name) AS u, lower(name) AS l,
                    |       trim('  x  ') AS t, ltrim('  x  ') AS lt, rtrim('  x  ') AS rt,
                    |       upper(nickname) AS missing
                    |FROM people
                    |WHERE _key = 'p1'""".stripMargin
                )
      } yield assertTrue(
        field(rows.head, "u")       == StringV("ALICE"),
        field(rows.head, "l")       == StringV("alice"),
        field(rows.head, "t")       == StringV("x"),
        field(rows.head, "lt")      == StringV("x  "),
        field(rows.head, "rt")      == StringV("  x"),
        field(rows.head, "missing") == NullV // no such field → NULL propagates
      )
    },
    test("string functions: SUBSTR, CONCAT, REPLACE, INSTR (and SUBSTR usable in WHERE)") {
      for {
        _    <- seed
        rows <- query(
                  """SELECT _key,
                    |       substr(name, 1, 3) AS s, concat(name, '-', age) AS c,
                    |       replace(name, 'a', 'X') AS r, instr(name, 'o') AS i
                    |FROM people
                    |WHERE _key = 'p3'""".stripMargin
                )
        whr  <- query("select _key from people where substr(name, 1, 1) = 'A'")
      } yield assertTrue(
        field(rows.head, "s") == StringV("Car"),       // substr("Carol", 1, 3)
        field(rows.head, "c") == StringV("Carol-40"),  // concat coerces the age to text
        field(rows.head, "r") == StringV("CXrol"),     // replace 'a' → 'X'
        field(rows.head, "i") == LongV(4),             // 'o' is the 4th character of "Carol"
        whr.map(r => field(r, "_key")) == List(StringV("p1")) // only "Alice" starts with 'A'
      )
    },
    test("HAVING filters groups by an aggregate") {
      for {
        _    <- seedOrders
        rows <- query(
                  """SELECT customer, count(*) AS count
                    |FROM orders
                    |GROUP BY customer
                    |HAVING count(*) > 1
                    |ORDER BY customer""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "customer")) == List(StringV("Alice")), // Bob has only 1 order
        rows.map(r => field(r, "count"))    == List(LongV(2))
      )
    },
    test("an aggregate in WHERE is rejected (belongs in HAVING)") {
      for {
        _    <- seedOrders
        exit <- query(
                  """SELECT customer
                    |FROM orders
                    |WHERE count(*) > 1
                    |GROUP BY customer""".stripMargin
                ).exit
      } yield assert(exit)(Assertion.fails(Assertion.isSubtype[SqlError.Unsupported](Assertion.anything)))
    },
    test("SELECT DISTINCT removes duplicate projected rows") {
      for {
        _    <- seedOrders
        rows <- query("select distinct customer from orders order by customer")
      } yield assertTrue(rows.map(r => field(r, "customer")) == List(StringV("Alice"), StringV("Bob")))
    },
    test("a non-aggregated column without GROUP BY is rejected") {
      for {
        _    <- seed
        exit <- query("select name, count(*) from people").exit
      } yield assert(exit)(Assertion.fails(Assertion.isSubtype[SqlError.Unsupported](Assertion.anything)))
    },
    test("DESCRIBE shows the key's codec id and the value columns (no guessing)") {
      for {
        _    <- seed
        rows <- query("describe people")
        cols  = rows.map(r => (field(r, "column"), field(r, "type")))
      } yield assertTrue(
        cols.contains((StringV("_key"), StringV("lmdb:str"))),
        cols.contains((StringV("name"), StringV("string"))),
        cols.contains((StringV("age"), StringV("integer")))
      )
    },
    test("SHOW COLLECTIONS lists the collection") {
      for {
        _    <- seed
        rows <- query("show collections")
      } yield assertTrue(rows.exists(r => field(r, "name") == StringV("people")))
    },
    test("INSERT then read back through the typed pipeline") {
      for {
        _   <- seed
        ins <- query("insert into people (_key, name, age) values ('p4', 'Dave', 50)")
        got <- query("select name, age from people where _key = 'p4'")
      } yield assertTrue(
        ins == List(MapV(scala.collection.immutable.ListMap("affected" -> LongV(1)))),
        got.size == 1,
        field(got.head, "name") == StringV("Dave"),
        field(got.head, "age")  == LongV(50)
      )
    },
    test("UPDATE modifies matching rows") {
      for {
        _   <- seed
        upd <- query("update people set age = 99 where _key = 'p1'")
        got <- query("select age from people where _key = 'p1'")
      } yield assertTrue(field(upd.head, "affected") == LongV(1), field(got.head, "age") == LongV(99))
    },
    test("DELETE removes matching rows") {
      for {
        _    <- seed
        del  <- query("delete from people where _key = 'p2'")
        rows <- query("select _key from people order by _key")
      } yield assertTrue(
        field(del.head, "affected") == LongV(1),
        rows.map(r => field(r, "_key")) == List(StringV("p1"), StringV("p3"))
      )
    },
    test("INNER JOIN matches a value field to the other collection's _key") {
      for {
        _    <- seedSales
        rows <- query(
                  """SELECT s._key, c.name, s.amount
                    |FROM sales s
                    |JOIN customers c ON s.customerId = c._key
                    |ORDER BY s._key""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key"))   == List(StringV("s1"), StringV("s2"), StringV("s3")), // s4 (absent customer) excluded
        rows.map(r => field(r, "name"))   == List(StringV("Alice"), StringV("Alice"), StringV("Bob")),
        rows.map(r => field(r, "amount")) == List(LongV(10), LongV(30), LongV(5))
      )
    },
    test("LEFT JOIN keeps unmatched left rows with NULLs") {
      for {
        _    <- seedSales
        rows <- query(
                  """SELECT s._key, c.name
                    |FROM sales s
                    |LEFT JOIN customers c ON s.customerId = c._key
                    |ORDER BY s._key""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("s1"), StringV("s2"), StringV("s3"), StringV("s4")),
        rows.map(r => field(r, "name")) == List(StringV("Alice"), StringV("Alice"), StringV("Bob"), NullV)
      )
    },
    test("GROUP BY over a JOIN aggregates joined columns") {
      for {
        _    <- seedSales
        rows <- query(
                  """SELECT c.country, count(*) AS n, sum(s.amount) AS total
                    |FROM sales s
                    |JOIN customers c ON s.customerId = c._key
                    |GROUP BY c.country
                    |ORDER BY c.country""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "country")) == List(StringV("FR"), StringV("US")),
        rows.map(r => field(r, "n"))       == List(LongV(2), LongV(1)),
        rows.map(r => field(r, "total"))   == List(DecimalV(BigDecimal(40)), DecimalV(BigDecimal(5)))
      )
    },
    test("JOIN coerces a value field to the joined _key's datatype (string → Long key)") {
      for {
        _    <- seedItems
        rows <- query(
                  """SELECT r._key, i.label
                    |FROM refs r
                    |JOIN items i ON r.itemCode = i._key
                    |ORDER BY r._key""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "_key"))  == List(StringV("r1"), StringV("r2")),
        rows.map(r => field(r, "label")) == List(StringV("Widget"), StringV("Gadget"))
      )
    },
    test("JOIN on two value fields of the same type") {
      for {
        _    <- seedMarkets
        rows <- query(
                  """SELECT c.name, m.tier
                    |FROM customers c
                    |JOIN markets m ON c.country = m.country
                    |ORDER BY c.name""".stripMargin
                )
      } yield assertTrue(
        rows.map(r => field(r, "name")) == List(StringV("Alice"), StringV("Bob")),
        rows.map(r => field(r, "tier")) == List(StringV("A"), StringV("B"))
      )
    },
    test("an unknown collection is a clean error") {
      for {
        exit <- query("select * from nope").exit
      } yield assert(exit)(Assertion.fails(Assertion.equalTo(SqlError.UnknownCollection("nope"))))
    }
  ).provide(lmdbLayer) @@ TestAspect.sequential
}
