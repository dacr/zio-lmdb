/*
 * Copyright 2025 David Crosson
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
import zio.json.*
import zio.nio.file.Files
import zio.lmdb.json.*

case class User(firstName:String, lastName:String, age:Option[Int]) derives LMDBCodecJson
case class Login(username:String, user:User) derives LMDBCodecJson

object LMDBDataClassSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("Json serialization codec")(
    test("support product type")(
      for {
        collection    <- LMDB.collectionCreate[Login]("logins")
        user           = User("John", "Doe", Some(42))
        record         = Login("joe", user)
        recordId      <- Random.nextUUID.map(_.toString)
        _             <- collection.upsert(recordId, previousRecord => record)
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
    )
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
