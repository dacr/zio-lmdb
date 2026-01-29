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
  *   The maximum number of readers
  * @param maxCollections
  *   The maximum number of collections which can be created
  * @param mapSize
  *   The maximum size of the whole database including metadata
  */
case class LMDBConfig(
  databaseName: String,
  databasesHome: Option[String],
  fileSystemSynchronized: Boolean,
  maxReaders: Int,
  maxCollections: Int,
  mapSize: BigInt
)

object LMDBConfig {
  val default =
    LMDBConfig(
      databaseName = "default",
      databasesHome = None,
      fileSystemSynchronized = false,
      maxReaders = 1_000,
      mapSize = BigInt(100_000_000_000L),
      maxCollections = 10_000
    )
}
