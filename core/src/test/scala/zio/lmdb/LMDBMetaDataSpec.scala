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
import zio.lmdb.json.*

object LMDBMetaDataSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB MetaData Maintenance")(
    test("metadata collection is created automatically") {
      for {
        collections <- LMDB.collectionsAvailable()
        config      <- ZIO.config(LMDB.config)
      } yield assertTrue(collections.contains(config.metaDataCollectionName))
    },
    test("creating collection updates metadata") {
      for {
        config      <- ZIO.config(LMDB.config)
        colName      = "test-collection"
        _           <- LMDB.collectionAllocate(colName)
        metaCol     <- LMDB.collectionGet[String, MetaDataEntry](config.metaDataCollectionName)
        entry       <- metaCol.fetch(colName).some
      } yield assertTrue(
        entry.collectionName == colName,
        entry.collectionKind == CollectionKind.Regular
      )
    },
    test("creating index updates metadata") {
      for {
        config      <- ZIO.config(LMDB.config)
        indexName    = "test-index"
        _           <- LMDB.indexCreate[String, String](indexName)
        metaCol     <- LMDB.collectionGet[String, MetaDataEntry](config.metaDataCollectionName)
        entry       <- metaCol.fetch(indexName).some
      } yield assertTrue(
        entry.collectionName == indexName,
        entry.collectionKind == CollectionKind.Index
      )
    },
    test("dropping collection removes metadata") {
      for {
        config      <- ZIO.config(LMDB.config)
        colName      = "test-drop-col"
        _           <- LMDB.collectionAllocate(colName)
        _           <- LMDB.collectionDrop(colName)
        metaCol     <- LMDB.collectionGet[String, MetaDataEntry](config.metaDataCollectionName)
        entry       <- metaCol.fetch(colName)
      } yield assertTrue(entry.isEmpty)
    },
    test("dropping index removes metadata") {
      for {
        config      <- ZIO.config(LMDB.config)
        indexName    = "test-drop-index"
        _           <- LMDB.indexCreate[String, String](indexName)
        _           <- LMDB.indexDrop(indexName)
        metaCol     <- LMDB.collectionGet[String, MetaDataEntry](config.metaDataCollectionName)
        entry       <- metaCol.fetch(indexName)
      } yield assertTrue(entry.isEmpty)
    },
    test("failIfExists=false updates metadata for existing collection") {
      for {
        config      <- ZIO.config(LMDB.config)
        colName      = "test-migration"
        // 1. Manually create collection without metadata (simulating old version)
        // We can't easily bypass metadata update with public API anymore, 
        // but we can delete the metadata entry manually.
        _           <- LMDB.collectionAllocate(colName)
        metaCol     <- LMDB.collectionGet[String, MetaDataEntry](config.metaDataCollectionName)
        _           <- metaCol.delete(colName)
        entryBefore <- metaCol.fetch(colName)
        
        // 2. Call collectionCreate with failIfExists=false
        _           <- LMDB.collectionCreate[String, String](colName, failIfExists = false)
        entryAfter  <- metaCol.fetch(colName).some
      } yield assertTrue(
        entryBefore.isEmpty,
        entryAfter.collectionName == colName,
        entryAfter.collectionKind == CollectionKind.Regular
      )
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
