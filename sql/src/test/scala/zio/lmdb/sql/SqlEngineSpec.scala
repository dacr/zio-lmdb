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
      } yield assertTrue(
        all.size == 1,
        field(all.head, "count") == LongV(3),
        field(big.head, "count") == LongV(2)
      )
    },
    test("COUNT(col) counts only non-null values of that column") {
      for {
        _   <- seed
        _   <- query("insert into people (_key, name) values ('p4', 'Dave')")
        age <- query("select count(age) from people")
        nme <- query("select count(name) from people")
      } yield assertTrue(field(age.head, "count") == LongV(3), field(nme.head, "count") == LongV(4))
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
    test("an unknown collection is a clean error") {
      for {
        exit <- query("select * from nope").exit
      } yield assert(exit)(Assertion.fails(Assertion.equalTo(SqlError.UnknownCollection("nope"))))
    }
  ).provide(lmdbLayer) @@ TestAspect.sequential
}
