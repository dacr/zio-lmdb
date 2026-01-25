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
import zio.test.Assertion.*
import zio.test.TestAspect.*
import zio.stream.*
import zio.lmdb.json.stringCodec

object LMDBLockingSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Locking Behavior")(
    test("stream should release read lock allowing subsequent writes") {
      for {
        collection <- LMDB.collectionCreate[String, String]("locking-test")
        _          <- collection.upsert("key1", _ => "val1")
        
        // consume stream (read lock acquired)
        _          <- collection.stream().runCollect
        
        // try write (should succeed if lock was released)
        fiber      <- collection.upsert("key2", _ => "val2").fork
        _          <- TestClock.adjust(1.second)
        res        <- fiber.join.timeout(2.seconds)
      } yield assertTrue(res.isDefined)
    },
    test("concurrent reads should work") {
      for {
        collection <- LMDB.collectionCreate[String, String]("concurrent-read-test")
        _          <- collection.upsert("key1", _ => "val1")
        
        f1 <- collection.fetch("key1").repeatN(100).fork
        f2 <- collection.stream().runDrain.repeatN(100).fork
        
        _  <- f1.join
        _  <- f2.join
      } yield assertTrue(true)
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
