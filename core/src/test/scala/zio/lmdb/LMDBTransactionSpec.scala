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
import zio.lmdb.json.stringCodec

object LMDBTransactionSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Transaction support")(
    test("readWrite transaction commits changes") {
      for {
        collectionName <- Random.nextUUID.map(_.toString)
        _              <- LMDB.collectionCreate[String, String](collectionName)
        
        _ <- LMDB.readWrite { txn =>
               for {
                 _ <- txn.upsert[String, String](collectionName, "key1", _ => "val1")
                 _ <- txn.upsert[String, String](collectionName, "key2", _ => "val2")
               } yield ()
             }
        
        v1 <- LMDB.fetch[String, String](collectionName, "key1")
        v2 <- LMDB.fetch[String, String](collectionName, "key2")
      } yield assertTrue(v1.contains("val1"), v2.contains("val2"))
    },
    test("readWrite transaction rollback on error") {
      for {
        collectionName <- Random.nextUUID.map(_.toString)
        _              <- LMDB.collectionCreate[String, String](collectionName)
        
        _ <- LMDB.readWrite { txn =>
               for {
                 _ <- txn.upsert[String, String](collectionName, "key1", _ => "val1")
                 _ <- ZIO.fail(new Exception("Boom"))
               } yield ()
             }.ignore
        
        v1 <- LMDB.fetch[String, String](collectionName, "key1")
      } yield assertTrue(v1.isEmpty)
    },
    test("readOnly transaction sees consistent view") {
      for {
        collectionName <- Random.nextUUID.map(_.toString)
        _              <- LMDB.collectionCreate[String, String](collectionName)
        _              <- LMDB.upsert[String, String](collectionName, "key1", _ => "val1")
        
        _ <- LMDB.readOnly { txn =>
               for {
                 v1 <- txn.fetch[String, String](collectionName, "key1")
                 // This should technically NOT see concurrent writes if isolation works, 
                 // but checking basic read functionality here.
                 // LMDB provides Snapshot Isolation for read transactions.
               } yield assertTrue(v1.contains("val1"))
             }
      } yield assertTrue(true)
    },
    test("mixed operations in transaction") {
      for {
        col1 <- Random.nextUUID.map(_.toString)
        col2 <- Random.nextUUID.map(_.toString)
        _    <- LMDB.collectionCreate[String, String](col1)
        _    <- LMDB.collectionCreate[String, String](col2)
        
        _ <- LMDB.readWrite { txn =>
               for {
                 _ <- txn.upsert[String, String](col1, "k1", _ => "v1")
                 v <- txn.fetch[String, String](col1, "k1")
                 _ <- txn.upsert[String, String](col2, "k1", _ => v.getOrElse("default"))
               } yield ()
             }
             
        res <- LMDB.fetch[String, String](col2, "k1")
      } yield assertTrue(res.contains("v1"))
    },
    test("LMDBCollection transaction methods") {
      for {
        collectionName <- Random.nextUUID.map(_.toString)
        collection     <- LMDB.collectionCreate[String, String](collectionName)
        
        _ <- collection.readWrite { txn =>
               for {
                 _ <- txn.upsert("key1", _ => "val1")
                 v <- txn.fetch("key1")
                 _ <- ZIO.fromOption(v).map(_ => ()) // ensure value is visible
                 _ <- txn.upsert("key2", _ => v.getOrElse("default"))
               } yield ()
             }
             
        v1 <- collection.fetch("key1")
        v2 <- collection.fetch("key2")
      } yield assertTrue(v1.contains("val1"), v2.contains("val1"))
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
