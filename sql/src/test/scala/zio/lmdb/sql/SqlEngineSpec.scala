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

object SqlEngineSpec extends ZIOSpecDefault {

  final case class Person(name: String, age: Long) derives LMDBCodecJson, LMDBSchema
  final case class Order(customer: String, amount: Long) derives LMDBCodecJson, LMDBSchema
  final case class Customer(name: String, country: String) derives LMDBCodecJson, LMDBSchema
  final case class Sale(customerId: String, amount: Long) derives LMDBCodecJson, LMDBSchema
  final case class Item(label: String) derives LMDBCodecJson, LMDBSchema
  final case class Ref(itemCode: String) derives LMDBCodecJson, LMDBSchema
  final case class Market(country: String, tier: String) derives LMDBCodecJson, LMDBSchema

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

  override def spec = suite("SqlEngine")(
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
        rows <- query("select customer, count(*), sum(amount) from orders group by customer order by customer")
      } yield assertTrue(
        rows.map(r => field(r, "customer"))    == List(StringV("Alice"), StringV("Bob")),
        rows.map(r => field(r, "count(*)"))     == List(LongV(2), LongV(1)),
        rows.map(r => field(r, "sum(amount)"))  == List(DecimalV(BigDecimal(40)), DecimalV(BigDecimal(5)))
      )
    },
    test("alias (AS count) and ORDER BY the alias") {
      for {
        _    <- seedOrders
        rows <- query("select customer, count(*) as count from orders group by customer order by count")
      } yield assertTrue(
        rows.map(r => field(r, "count"))    == List(LongV(1), LongV(2)), // Bob:1, Alice:2 — ascending by the alias
        rows.map(r => field(r, "customer")) == List(StringV("Bob"), StringV("Alice"))
      )
    },
    test("LENGTH() is usable in WHERE") {
      for {
        _    <- seedOrders
        rows <- query("select distinct customer from orders where length(customer) > 3 order by customer")
      } yield assertTrue(rows.map(r => field(r, "customer")) == List(StringV("Alice"))) // "Bob" has length 3
    },
    test("HAVING filters groups by an aggregate") {
      for {
        _    <- seedOrders
        rows <- query("select customer, count(*) as count from orders group by customer having count(*) > 1 order by customer")
      } yield assertTrue(
        rows.map(r => field(r, "customer")) == List(StringV("Alice")), // Bob has only 1 order
        rows.map(r => field(r, "count"))    == List(LongV(2))
      )
    },
    test("an aggregate in WHERE is rejected (belongs in HAVING)") {
      for {
        _    <- seedOrders
        exit <- query("select customer from orders where count(*) > 1 group by customer").exit
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
        rows <- query("select s._key, c.name, s.amount from sales s join customers c on s.customerId = c._key order by s._key")
      } yield assertTrue(
        rows.map(r => field(r, "_key"))   == List(StringV("s1"), StringV("s2"), StringV("s3")), // s4 (absent customer) excluded
        rows.map(r => field(r, "name"))   == List(StringV("Alice"), StringV("Alice"), StringV("Bob")),
        rows.map(r => field(r, "amount")) == List(LongV(10), LongV(30), LongV(5))
      )
    },
    test("LEFT JOIN keeps unmatched left rows with NULLs") {
      for {
        _    <- seedSales
        rows <- query("select s._key, c.name from sales s left join customers c on s.customerId = c._key order by s._key")
      } yield assertTrue(
        rows.map(r => field(r, "_key")) == List(StringV("s1"), StringV("s2"), StringV("s3"), StringV("s4")),
        rows.map(r => field(r, "name")) == List(StringV("Alice"), StringV("Alice"), StringV("Bob"), NullV)
      )
    },
    test("GROUP BY over a JOIN aggregates joined columns") {
      for {
        _    <- seedSales
        rows <- query("select c.country, count(*) as n, sum(s.amount) as total from sales s join customers c on s.customerId = c._key group by c.country order by c.country")
      } yield assertTrue(
        rows.map(r => field(r, "country")) == List(StringV("FR"), StringV("US")),
        rows.map(r => field(r, "n"))       == List(LongV(2), LongV(1)),
        rows.map(r => field(r, "total"))   == List(DecimalV(BigDecimal(40)), DecimalV(BigDecimal(5)))
      )
    },
    test("JOIN coerces a value field to the joined _key's datatype (string → Long key)") {
      for {
        _    <- seedItems
        rows <- query("select r._key, i.label from refs r join items i on r.itemCode = i._key order by r._key")
      } yield assertTrue(
        rows.map(r => field(r, "_key"))  == List(StringV("r1"), StringV("r2")),
        rows.map(r => field(r, "label")) == List(StringV("Widget"), StringV("Gadget"))
      )
    },
    test("JOIN on two value fields of the same type") {
      for {
        _    <- seedMarkets
        rows <- query("select c.name, m.tier from customers c join markets m on c.country = m.country order by c.name")
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
