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
import zio.lmdb.protobuf.*

import zio.lmdb.protobuf.user_profile.UserProfile

object LMDBProtobufSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("Protobuf serialization codec")(
    test("support scalapb generated classes") {
      for {
        collection    <- LMDB.collectionCreate[String, UserProfile]("user_profiles")
        recordId      <- Random.nextUUID.map(_.toString)
        record         = UserProfile(id = recordId, name = "Alice", email = "alice@example.com", age = 30)
        _             <- collection.upsert(recordId, _ => record)
        exists        <- collection.contains(recordId)
        gotten        <- collection.fetch(recordId).some
        deletedRecord <- collection.delete(recordId)
        gotNothing    <- collection.fetch(recordId)
      } yield assertTrue(
        gotten == record,
        deletedRecord.contains(record),
        gotNothing.isEmpty,
        exists
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
