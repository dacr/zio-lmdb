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
import zio.lmdb.keycodecs.timestamp.TimestampCodec.given
import zio.lmdb.schema.LMDBSchema
import zio.lmdb.sql.engine.SqlEngine

import java.time.Instant

/** End-to-end coverage of index-aware planning: queries served by declared indexes / primary-key pushdown must return exactly what a full scan returns, and EXPLAIN must show the chosen path.
  */
object SqlPlannerSpec extends ZIOSpecDefault {

  final case class Event(label: String, priority: Long, ts: Instant, tag: Option[String]) derives LMDBCodecJson, LMDBSchema

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete(): Unit
  }

  private val lmdbLayer: ZLayer[Any, Throwable, LMDB] = ZLayer.scoped {
    for {
      dir  <- ZIO.acquireRelease(ZIO.attempt(java.nio.file.Files.createTempDirectory("lmdb-sql-planner-test")))(p => ZIO.attempt(deleteRecursively(p.toFile)).ignore)
      lmdb <- LMDBLive.setup(LMDBConfig.default.copy(databasesHome = Some(dir.toString)))
    } yield lmdb
  }

  private def query(sql: String): ZIO[LMDB, SqlError, List[JValue]] =
    SqlEngine.run(sql).flatMap(_.toList)

  private def field(row: JValue, name: String): JValue =
    row match { case MapV(m) => m.getOrElse(name, NullV); case _ => NullV }

  private def keysOf(rows: List[JValue]): List[JValue] = rows.map(field(_, "_key"))

  /** The EXPLAIN "access" detail line of a statement. */
  private def accessOf(sql: String): ZIO[LMDB, SqlError, String] =
    query(s"EXPLAIN $sql").map(rows => rows.collectFirst { case r if field(r, "step") == StringV("access") => field(r, "detail") }.collect { case StringV(s) => s }.getOrElse(""))

  private val seed =
    for {
      byTag   <- LMDB.indexCreate[String, String]("events_by_tag", failIfExists = false)
      byPrio  <- LMDB.indexCreate[(Long, String), String]("events_by_prio", failIfExists = false)
      byTs    <- LMDB.indexCreate[(Instant, String), String]("events_by_ts", failIfExists = false)
      byTagTs <- LMDB.indexCreate[String, (Instant, String)]("events_by_tag_ts", failIfExists = false)
      events  <- LMDB
                   .collectionCreate[String, Event]("events", failIfExists = false)
                   .flatMap(
                     _.withDeclaredIndex(byTag)(
                       from = IdxKey.of(IdxKey.fieldOpt("tag")((_, e: Event) => e.tag)),
                       to = IdxKey.of(IdxKey.primaryKey)
                     )
                   )
                   .flatMap(
                     _.withDeclaredIndex(byPrio)(
                       from = IdxKey.tuple(IdxKey.field("priority")((_, e: Event) => e.priority), IdxKey.primaryKey),
                       to = IdxKey.of(IdxKey.primaryKey)
                     )
                   )
                   .flatMap(
                     _.withDeclaredIndex(byTs)(
                       from = IdxKey.tuple(IdxKey.field("ts")((_, e: Event) => e.ts), IdxKey.primaryKey),
                       to = IdxKey.of(IdxKey.primaryKey)
                     )
                   )
                   .flatMap(
                     _.withDeclaredIndex(byTagTs)(
                       from = IdxKey.of(IdxKey.fieldOpt("tag")((_, e: Event) => e.tag)),
                       to = IdxKey.tuple(IdxKey.field("ts")((_, e: Event) => e.ts), IdxKey.primaryKey)
                     )
                   )
      _       <- events.upsertOverwrite("e1", Event("alpha", 5, Instant.parse("2024-01-10T08:00:00Z"), Some("hot")))
      _       <- events.upsertOverwrite("e2", Event("beta", 5, Instant.parse("2024-02-20T09:00:00Z"), Some("cold")))
      _       <- events.upsertOverwrite("e3", Event("gamma", 7, Instant.parse("2024-03-05T10:00:00Z"), Some("hot")))
      _       <- events.upsertOverwrite("e4", Event("delta", 2, Instant.parse("2024-04-01T11:00:00Z"), None))
      _       <- events.upsertOverwrite("e5", Event("alpine", 5, Instant.parse("2024-05-15T12:00:00Z"), Some("hot")))
      _       <- events.upsertOverwrite("e6", Event("epsilon", 9, Instant.parse("2023-12-25T07:00:00Z"), Some("cold")))
    } yield ()

  override def spec = suite("SQL planner over declared indexes")(
    test("equality on an indexed field uses the index and returns exactly the matching rows") {
      for {
        _      <- seed
        rows   <- query("SELECT _key, label FROM events WHERE priority = 5 ORDER BY _key")
        access <- accessOf("SELECT _key FROM events WHERE priority = 5")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e1"), StringV("e2"), StringV("e5")),
        access.contains("index range scan on events_by_prio")
      )
    },
    test("equality on an optional indexed field: unindexed (absent) rows are correctly excluded") {
      for {
        _    <- seed
        hot  <- query("SELECT _key FROM events WHERE tag = 'hot' ORDER BY _key")
        none <- query("SELECT _key FROM events WHERE tag = 'nope'")
      } yield assertTrue(
        keysOf(hot) == List(StringV("e1"), StringV("e3"), StringV("e5")),
        none.isEmpty
      )
    },
    test("BETWEEN on a timestamp component is served by a range scan in index order") {
      for {
        _      <- seed
        rows   <- query("SELECT _key, ts FROM events WHERE ts BETWEEN '2024-01-01T00:00:00Z' AND '2024-03-31T23:59:59Z' ORDER BY ts")
        access <- accessOf("SELECT _key FROM events WHERE ts BETWEEN '2024-01-01T00:00:00Z' AND '2024-03-31T23:59:59Z' ORDER BY ts")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e1"), StringV("e2"), StringV("e3")),
        access.contains("index range scan on events_by_ts"),
        access.contains("range")
      )
    },
    test("strict bounds exclude the boundary value") {
      for {
        _    <- seed
        rows <- query("SELECT _key FROM events WHERE ts > '2024-01-10T08:00:00Z' AND ts < '2024-04-01T11:00:00Z' ORDER BY ts")
      } yield assertTrue(keysOf(rows) == List(StringV("e2"), StringV("e3")))
    },
    test("equality + ORDER BY on a composite TO_KEY streams in index order without sorting") {
      for {
        _       <- seed
        rows    <- query("SELECT _key, ts FROM events WHERE tag = 'hot' ORDER BY ts")
        explain <- query("EXPLAIN SELECT _key FROM events WHERE tag = 'hot' ORDER BY ts")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e1"), StringV("e3"), StringV("e5")),
        explain.exists(r => field(r, "detail") == StringV("scan order matches ORDER BY (no sort)")),
        explain.exists(r => field(r, "step") == StringV("access") && field(r, "detail").toString.contains("events_by_tag_ts"))
      )
    },
    test("ORDER BY + LIMIT with no WHERE picks an order-matching index scan") {
      for {
        _      <- seed
        rows   <- query("SELECT _key FROM events ORDER BY ts LIMIT 2")
        access <- accessOf("SELECT _key FROM events ORDER BY ts LIMIT 2")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e6"), StringV("e1")),
        access.contains("index range scan on events_by_ts"),
        access.contains("ordered")
      )
    },
    test("residual predicates still filter after an index scan") {
      for {
        _       <- seed
        rows    <- query("SELECT _key FROM events WHERE priority = 5 AND label LIKE 'alp%' ORDER BY _key")
        explain <- query("EXPLAIN SELECT _key FROM events WHERE priority = 5 AND label LIKE 'alp%'")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e1"), StringV("e5")),
        explain.exists(r => field(r, "step") == StringV("filter") && field(r, "detail") == StringV("label like 'alp%'"))
      )
    },
    test("aggregates run over the index-served row source") {
      for {
        _    <- seed
        rows <- query("SELECT COUNT(*) AS n FROM events WHERE priority = 5")
      } yield assertTrue(rows.map(field(_, "n")) == List(LongV(3)))
    },
    test("_key equality and IN use point fetches") {
      for {
        _        <- seed
        one      <- query("SELECT _key, label FROM events WHERE _key = 'e3'")
        many     <- query("SELECT _key FROM events WHERE _key IN ('e1', 'e4', 'zz') ORDER BY _key")
        access   <- accessOf("SELECT _key FROM events WHERE _key = 'e3'")
        accessIn <- accessOf("SELECT _key FROM events WHERE _key IN ('e1', 'e4')")
      } yield assertTrue(
        one.map(field(_, "label")) == List(StringV("gamma")),
        keysOf(many) == List(StringV("e1"), StringV("e4")),
        access.contains("primary-key fetch"),
        accessIn.contains("primary-key fetch")
      )
    },
    test("_key range predicates use a primary-key range scan in key order") {
      for {
        _      <- seed
        rows   <- query("SELECT _key FROM events WHERE _key BETWEEN 'e2' AND 'e4' ORDER BY _key")
        strict <- query("SELECT _key FROM events WHERE _key > 'e2' AND _key < 'e5'")
        access <- accessOf("SELECT _key FROM events WHERE _key >= 'e2' ORDER BY _key")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e2"), StringV("e3"), StringV("e4")),
        keysOf(strict) == List(StringV("e3"), StringV("e4")),
        access.contains("primary-key range scan"),
        access.contains("ordered")
      )
    },
    test("an OR predicate cannot be pushed down and falls back to a full scan") {
      for {
        _      <- seed
        rows   <- query("SELECT _key FROM events WHERE priority = 5 OR priority = 7 ORDER BY _key")
        access <- accessOf("SELECT _key FROM events WHERE priority = 5 OR priority = 7")
      } yield assertTrue(
        keysOf(rows) == List(StringV("e1"), StringV("e2"), StringV("e3"), StringV("e5")),
        access.contains("full scan")
      )
    },
    test("DELETE and UPDATE go through the planned access path") {
      for {
        _         <- seed
        _         <- query("UPDATE events SET label = 'renamed' WHERE _key = 'e2'")
        renamed   <- query("SELECT label FROM events WHERE _key = 'e2'")
        _         <- query("DELETE FROM events WHERE priority = 7")
        remaining <- query("SELECT _key FROM events ORDER BY _key")
      } yield assertTrue(
        renamed.map(field(_, "label")) == List(StringV("renamed")),
        keysOf(remaining) == List(StringV("e1"), StringV("e2"), StringV("e4"), StringV("e5"), StringV("e6"))
      )
    },
    test("index-served results equal full-scan results for every pushable shape") {
      val shapes = List(
        "SELECT _key FROM events WHERE priority = 5",
        "SELECT _key FROM events WHERE tag = 'cold'",
        "SELECT _key FROM events WHERE ts >= '2024-02-01T00:00:00Z'",
        "SELECT _key FROM events WHERE ts <= '2024-02-01T00:00:00Z'",
        "SELECT _key FROM events WHERE ts BETWEEN '2023-01-01' AND '2024-03-06'",
        "SELECT _key FROM events WHERE _key <= 'e3'"
      )
      for {
        _       <- seed
        results <- ZIO.foreach(shapes)(s => query(s + " ORDER BY _key").map(keysOf))
        // The same predicates made non-pushable by an OR with a false branch (forces a full scan).
        control <- ZIO.foreach(shapes)(s => query(s.replace("WHERE ", "WHERE label = 'no-such-label' OR ") + " ORDER BY _key").map(keysOf))
      } yield assertTrue(results == control)
    }
  ).provide(lmdbLayer) @@ TestAspect.sequential
}
