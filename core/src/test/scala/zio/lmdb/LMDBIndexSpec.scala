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
        items == Chunk(id1, id2),
        itemsAfterUnindex == Chunk(id2),
        !contains1After
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
        items.head == otherKey
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
