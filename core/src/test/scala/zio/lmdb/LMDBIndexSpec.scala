/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio.lmdb

import zio.*
import zio.test.*
import zio.test.TestAspect.*
import zio.stream.*

object LMDBIndexSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDBIndex features")(
    test("basic index operations") {
      for {
        indexName <- Random.nextUUID.map(_.toString)
        index     <- LMDB.indexCreate[String, String](indexName)
        
        key1 = "group1"
        id1  = "item1"
        id2  = "item2"
        id3  = "item3"

        _ <- index.index(key1, id1)
        _ <- index.index(key1, id2)
        
        contains1 <- index.indexContains(key1, id1)
        contains2 <- index.indexContains(key1, id2)
        contains3 <- index.indexContains(key1, id3)
        
        items <- index.indexed(key1).runCollect
        
        _ <- index.unindex(key1, id1)
        
        itemsAfterUnindex <- index.indexed(key1).runCollect
        contains1After    <- index.indexContains(key1, id1)

      } yield assertTrue(
        contains1,
        contains2,
        !contains3,
        items == Chunk((key1, id1), (key1, id2)),
        itemsAfterUnindex == Chunk((key1, id2)),
        !contains1After
      )
    },
    test("index navigation and key check") {
      for {
        indexName <- Random.nextUUID.map(_.toString)
        index     <- LMDB.indexCreate[String, String](indexName)

        key1 = "group1"
        key2 = "group2"
        key3 = "group3"
        id1  = "item1"
        id2  = "item2"
        id3  = "item3"

        _ <- index.index(key1, id1)
        _ <- index.index(key1, id2)
        _ <- index.index(key2, id3)

        hasKey1 <- index.hasKey(key1)
        hasKey2 <- index.hasKey(key2)
        hasKey3 <- index.hasKey(key3)

        head <- index.head()
        last <- index.last()

        nextKey1 <- index.next(key1)
        prevKey2 <- index.previous(key2)
        prevKey1 <- index.previous(key1)
        nextKey2 <- index.next(key2) // Should be None as it is the last one

      } yield assertTrue(
        hasKey1,
        hasKey2,
        !hasKey3,
        head.contains((key1, id1)),
        last.contains((key2, id3)),
        nextKey1.contains((key1, id2)), // Next after first entry of key1 is second entry of key1
        prevKey2.contains((key1, id2)), // Previous before first entry of key2 is last entry of key1
        prevKey1.isEmpty,               // Previous before first entry of key1 is None
        nextKey2.isEmpty
      )
    },
    test("index duplicate values") {
      for {
        indexName <- Random.nextUUID.map(_.toString)
        index     <- LMDB.indexCreate[String, String](indexName)

        key = "key"
        otherKey = "id1"

        // LMDB with MDB_DUPSORT does NOT store duplicate (key, value) pairs. 
        // Putting the same (key, value) again is a no-op or overwrites.
        _ <- index.index(key, otherKey)
        _ <- index.index(key, otherKey)

        items <- index.indexed(key).runCollect
      } yield assertTrue(
        items.size == 1,
        items.head == (key, otherKey)
      )
    },
    test("index iteration logic") {
      for {
        indexName <- Random.nextUUID.map(_.toString)
        index     <- LMDB.indexCreate[String, String](indexName)

        key1 = "group1"
        key2 = "group2"
        id1  = "item1"
        id2  = "item2"
        id3  = "item3"

        _ <- index.index(key1, id1)
        _ <- index.index(key1, id2)
        _ <- index.index(key2, id3)

        // limitToKey = true (default)
        items1 <- index.indexed(key1).runCollect
        items2 <- index.indexed(key2).runCollect

        // limitToKey = false
        items1Full <- index.indexed(key1, limitToKey = false).runCollect

      } yield assertTrue(
        items1 == Chunk((key1, id1), (key1, id2)),
        items2 == Chunk((key2, id3)),
        items1Full == Chunk((key1, id1), (key1, id2), (key2, id3))
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
