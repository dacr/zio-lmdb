/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package zio.lmdb

import zio._
import zio.test._
import zio.test.Assertion._
import zio.lmdb.json.LMDBCodecJson
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.KeyCodec.given

import java.util.UUID

object LMDBStreamPrefixSpec extends ZIOSpecDefault with Commons {

  case class Edge(weight: Int) derives LMDBCodecJson

  // Mirror L3's expected key shape: (SrcId, EdgeLabelId, TgtId) — all fixed-width.
  type EdgeKey = (UUID, Int, UUID)

  val spec = suite("LMDBStreamPrefixSpec")(
    test("streamPrefix returns only entries whose key starts with the prefix bytes") {
      for {
        col <- LMDB.collectionCreate[EdgeKey, Edge]("edges_by_src")
        srcA  = new UUID(1L, 1L)
        srcB  = new UUID(2L, 2L)
        tgt1  = new UUID(10L, 10L)
        tgt2  = new UUID(11L, 11L)
        tgt3  = new UUID(12L, 12L)
        _   <- col.upsertOverwrite((srcA, 1, tgt1), Edge(weight = 100))
        _   <- col.upsertOverwrite((srcA, 1, tgt2), Edge(weight = 200))
        _   <- col.upsertOverwrite((srcA, 2, tgt3), Edge(weight = 300))
        _   <- col.upsertOverwrite((srcB, 1, tgt1), Edge(weight = 999))
        // Prefix on srcA only — should match the 3 srcA edges, regardless of label
        allFromA <- col.streamPrefixWithKeys[UUID](srcA).runCollect
        // Prefix on (srcA, 1) — should match only the 2 (srcA, label=1, *) edges
        labelOne <- col.streamPrefixWithKeys[(UUID, Int)]((srcA, 1)).runCollect
      } yield assert(allFromA.toList.map(_._1))(
        hasSameElements(List((srcA, 1, tgt1), (srcA, 1, tgt2), (srcA, 2, tgt3)))
      ) && assert(labelOne.toList.map(_._1))(
        hasSameElements(List((srcA, 1, tgt1), (srcA, 1, tgt2)))
      ) && assert(labelOne.toList.map(_._2))(
        hasSameElements(List(Edge(100), Edge(200)))
      )
    },
    test("streamPrefix terminates immediately when no key matches the prefix") {
      for {
        col <- LMDB.collectionCreate[EdgeKey, Edge]("edges_by_src_empty")
        unknown = new UUID(999L, 999L)
        result <- col.streamPrefix[UUID](unknown).runCollect
      } yield assert(result.toList)(isEmpty)
    },
    test("streamPrefix is ordered by the suffix that follows the prefix") {
      for {
        col <- LMDB.collectionCreate[EdgeKey, Edge]("edges_by_src_ordered")
        src   = new UUID(42L, 42L)
        // Insert in scrambled order; expect tuple-key lexicographic emission
        _   <- col.upsertOverwrite((src, 3, new UUID(0L, 3L)), Edge(weight = 3))
        _   <- col.upsertOverwrite((src, 1, new UUID(0L, 1L)), Edge(weight = 1))
        _   <- col.upsertOverwrite((src, 2, new UUID(0L, 2L)), Edge(weight = 2))
        out <- col.streamPrefixWithKeys[UUID](src).runCollect
      } yield assert(out.toList.map(_._2.weight))(equalTo(List(1, 2, 3)))
    },
    test("transactional streamPrefix") {
      for {
        col <- LMDB.collectionCreate[EdgeKey, Edge]("edges_by_src_txn")
        src   = new UUID(7L, 7L)
        _   <- col.upsertOverwrite((src, 1, new UUID(0L, 1L)), Edge(weight = 11))
        _   <- col.upsertOverwrite((src, 1, new UUID(0L, 2L)), Edge(weight = 22))
        out <- col.readOnly { ops =>
                 ops.streamPrefixWithKeys[(UUID, Int)]((src, 1)).runCollect
               }
      } yield assert(out.toList.map(_._2.weight))(hasSameElements(List(11, 22)))
    }
  ).provideLayerShared(lmdbLayer)
}
