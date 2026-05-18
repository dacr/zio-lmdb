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

import java.io.File
import zio._

/** Configuration for LMDB
  *
  * @param databaseName
  *   Database name, which will be also used as the directory name
  * @param databasesHome
  *   Where to store the database directory
  * @param fileSystemSynchronized
  *   Synchronize the file system with all database write operations
  * @param maxReaders
  *   The maximum number of readers (LMDB-level reader-table size)
  * @param maxConcurrentReaders
  *   Upper bound on the number of read operations the library lets through to
  *   LMDB at the same time. Must be > 0 and should be <= `maxReaders`. Used to
  *   protect against unbounded fan-out of read transactions (which can exhaust
  *   the reader table and, under heavy host load, has been observed to trigger
  *   native crashes in `mdb_page_search` because of cursor lifecycle races).
  * @param readExecutorThreads
  *   Size of the dedicated thread pool that runs LMDB read operations. Keeping
  *   read work pinned to a small bounded pool stabilises the JNI side. Must be
  *   > 0.
  * @param maxCollections
  *   The maximum number of collections that can be created
  * @param mapSize
  *   The maximum size of the whole database including metadata
  * @param metaDataCollectionName
  *   The name of the collection used for storing collection metadata (Collection is regular or an index, typing information, ...)
  */
case class LMDBConfig(
  databaseName: String,
  databasesHome: Option[String],
  fileSystemSynchronized: Boolean,
  maxReaders: Int,
  maxConcurrentReaders: Int,
  readExecutorThreads: Int,
  maxCollections: Int,
  mapSize: BigInt,
  metaDataCollectionName: String
)

object LMDBConfig {
  val default =
    LMDBConfig(
      databaseName = "default",
      databasesHome = None,
      fileSystemSynchronized = false,
      maxReaders = 1_000,
      maxConcurrentReaders = 32,
      readExecutorThreads = math.max(2, math.min(8, java.lang.Runtime.getRuntime.availableProcessors())),
      mapSize = BigInt(100_000_000_000L),
      maxCollections = 10_000,
      metaDataCollectionName = "meta-data"
    )
}
