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

import zio._
import zio.stream.ZStream
import zio.config._

trait LMDB {

  def databasePath: String

  def platformCheck(): IO[StorageSystemError, Unit]

  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  def collectionCreate[T](name: CollectionName, failIfExists: Boolean = true)(implicit codec: LMDBCodec[T]): IO[CreateErrors, LMDBCollection[T]]

  def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit]

  def collectionGet[T](name: CollectionName)(implicit codec: LMDBCodec[T]): IO[GetErrors, LMDBCollection[T]]

  def collectionSize(name: CollectionName): IO[SizeErrors, Long]

  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]

  def collectionDrop(name: CollectionName): IO[DropErrors, Unit]

  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit codec: LMDBCodec[T]): IO[FetchErrors, Option[T]]

  def head[T](collectionName: CollectionName)(implicit codec: LMDBCodec[T]): IO[FetchErrors, Option[(RecordKey, T)]]

  def previous[T](collectionName: CollectionName, beforeThatKey: RecordKey)(implicit codec: LMDBCodec[T]): IO[FetchErrors, Option[(RecordKey, T)]]

  def next[T](collectionName: CollectionName, afterThatKey: RecordKey)(implicit codec: LMDBCodec[T]): IO[FetchErrors, Option[(RecordKey, T)]]

  def last[T](collectionName: CollectionName)(implicit codec: LMDBCodec[T]): IO[FetchErrors, Option[(RecordKey, T)]]

  def contains(collectionName: CollectionName, key: RecordKey): IO[ContainsErrors, Boolean]

  def update[T](collectionName: CollectionName, key: RecordKey, modifier: T => T)(implicit codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]]

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit codec: LMDBCodec[T]): IO[UpsertErrors, T]

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit codec: LMDBCodec[T]): IO[UpsertErrors, Unit]

  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]]

  def collect[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit codec: LMDBCodec[T]): IO[CollectErrors, List[T]]

  def stream[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  )(implicit codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T]

  def streamWithKeys[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  )(implicit codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (RecordKey, T)]
}

object LMDB {

  val config: Config[LMDBConfig] = ((Config.string("name").withDefault(LMDBConfig.default.databaseName)
    ?? "Database name, which will be also used as the directory name") ++
    (Config.string("home").optional.withDefault(LMDBConfig.default.databasesHome)
      ?? "Where to store the database directory") ++
    (Config.boolean("sync").withDefault(LMDBConfig.default.fileSystemSynchronized)
      ?? "Synchronize the file system with all database write operations") ++
    (Config.int("maxReaders").withDefault(LMDBConfig.default.maxReaders)
      ?? "The maximum number of readers") ++
    (Config.int("maxCollections").withDefault(LMDBConfig.default.maxCollections)
      ?? "The maximum number of collections which can be created") ++
    (Config.bigInt("mapSize").withDefault(LMDBConfig.default.mapSize)
      ?? "The maximum size of the whole database including metadata"))
    .to[LMDBConfig]
    .nested("lmdb")

  /** Default live implementation using the current configuration provider
    */
  val live: ZLayer[Scope, Any, LMDB] = ZLayer.fromZIO(
    for {
      config <- ZIO.config(LMDB.config)
      // doc     = generateDocs(LMDB.config).toTable.toGithubFlavouredMarkdown
      // _      <- ZIO.logInfo(s"Configuration documentation:\n$doc")
      _      <- ZIO.logInfo(s"Configuration : $config")
      lmdb   <- LMDBLive.setup(config)
    } yield lmdb
  )

  /** Default live implementation using the current configuration provider but overriding any configured database name with the provided one
    * @param name
    *   database name to use
    * @return
    */
  def liveWithDatabaseName(name: String): ZLayer[Scope, Any, LMDB] = ZLayer.fromZIO(
    for {
      config <- ZIO.config(LMDB.config).map(_.copy(databaseName = name))
      // doc     = generateDocs(LMDB.config).toTable.toGithubFlavouredMarkdown
      // _      <- Console.printLine(s"Configuration documentation:\n$doc")
      _      <- ZIO.logInfo(s"Configuration : $config")
      lmdb   <- LMDBLive.setup(config)
    } yield lmdb
  )

  /** Get the used storage directory in your file system.
    *
    * @return
    *   storage directory path
    */
  def databasePath: ZIO[LMDB, StorageSystemError, String] = ZIO.serviceWith(_.databasePath)

  /** Check LMDB server current configuration compatibility
    */
  def platformCheck(): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  /** List all available collections
    *
    * @return
    *   the list of collection names
    */
  def collectionsAvailable(): ZIO[LMDB, StorageSystemError, List[CollectionName]] = ZIO.serviceWithZIO(_.collectionsAvailable())

  /** check if a collection exists
    *
    * @param name
    *   the collection name
    * @return
    *   true if the collection exists
    */
  def collectionExists(name: CollectionName): ZIO[LMDB, StorageSystemError, Boolean] = ZIO.serviceWithZIO(_.collectionExists(name))

  /** Create a collection and return the collection helper facade. Use collection helper facade when all records are using the same json data type.
    *
    * @param name
    *   the collection name
    * @param failIfExists
    *   raise an error if the collection already exists, default to true
    * @tparam T
    *   the data type of the records which must be Json serializable
    * @return
    *   the collection helper facade
    */
  def collectionCreate[T](name: CollectionName, failIfExists: Boolean = true)(implicit codec: LMDBCodec[T]): ZIO[LMDB, CreateErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionCreate(name, failIfExists))

  /** Create a collection
    *
    * @param name
    *   the collection name
    */
  def collectionAllocate(name: CollectionName): ZIO[LMDB, CreateErrors, Unit] = ZIO.serviceWithZIO(_.collectionAllocate(name))

  /** Get a collection helper facade. Use collection helper facade when all records are using the same json data type.
    *
    * @param name
    *   the collection name
    * @return
    *   the collection helper facade
    */
  def collectionGet[T](name: CollectionName)(implicit codec: LMDBCodec[T]): ZIO[LMDB, GetErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionGet(name))

  /** Get how many items a collection contains
    *
    * @param name
    *   the collection name
    * @tparam T
    *   the data type of the records which must be Json serializable
    * @return
    *   the collection size
    */
  def collectionSize(name: CollectionName): ZIO[LMDB, SizeErrors, Long] = ZIO.serviceWithZIO(_.collectionSize(name))

  /** Remove all the content of a collection
    *
    * @param name
    *   the collection name
    */
  def collectionClear(name: CollectionName): ZIO[LMDB, ClearErrors, Unit] = ZIO.serviceWithZIO(_.collectionClear(name))

  /** Drop a collection
    *
    * @param name
    *   the collection name
    */
  def collectionDrop(name: CollectionName): ZIO[LMDB, DropErrors, Unit] = ZIO.serviceWithZIO(_.collectionDrop(name))

  /** Get a collection record
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to get
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[T]] = ZIO.serviceWithZIO(_.fetch(collectionName, key))

  /** Get collection first record
    *
    * @param collectionName
    *   the collection name
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head[T](collectionName: CollectionName)(implicit codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(RecordKey, T)]] = ZIO.serviceWithZIO(_.head(collectionName))

  /** Get the previous record for the given key
    *
    * @param collectionName
    *   the collection name
    * @param beforeThatKey
    *   the key of the reference record
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the key is the first one
    */
  def previous[T](collectionName: CollectionName, beforeThatKey: RecordKey)(implicit codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(RecordKey, T)]] = ZIO.serviceWithZIO(_.previous(collectionName, beforeThatKey))

  /** Get the next record for the given key
    *
    * @param collectionName
    *   the collection name
    * @param afterThatKey
    *   the key of the reference record
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the key is the last one
    */
  def next[T](collectionName: CollectionName, afterThatKey: RecordKey)(implicit codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(RecordKey, T)]] = ZIO.serviceWithZIO(_.next(collectionName, afterThatKey))

  /** Get collection last record
    *
    * @param collectionName
    *   the collection name
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last[T](collectionName: CollectionName)(implicit codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(RecordKey, T)]] = ZIO.serviceWithZIO(_.last(collectionName))

  /** Check if a collection contains the given key
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to look for
    * @return
    *   true if the key is used by the given collection
    */
  def contains(collectionName: CollectionName, key: RecordKey): ZIO[LMDB, ContainsErrors, Boolean] = ZIO.serviceWithZIO(_.contains(collectionName, key))

  /** update atomically a record in a collection.
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @returns
    *   the updated record if a record exists for the given key
    */

  def update[T](collectionName: CollectionName, key: RecordKey, modifier: T => T)(implicit codec: LMDBCodec[T]): ZIO[LMDB, UpdateErrors, Option[T]] = {
    ZIO.serviceWithZIO(_.update[T](collectionName, key, modifier))
  }

  /** update or insert atomically a record in a collection.
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @returns
    *   the updated or inserted record
    */
  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit codec: LMDBCodec[T]): ZIO[LMDB, UpsertErrors, T] = {
    ZIO.serviceWithZIO(_.upsert[T](collectionName, key, modifier))
  }

  /** Overwrite or insert a record in a collection. If the key is already being used for a record then the previous record will be overwritten by the new one.
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    * @tparam T
    *   the data type of the record which must be Json serializable
    */
  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit codec: LMDBCodec[T]): ZIO[LMDB, UpsertErrors, Unit] =
    ZIO.serviceWithZIO(_.upsertOverwrite[T](collectionName, key, document))

  /** Delete a record in a collection
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to delete
    * @tparam T
    *   the data type of the record which must be Json serializable the deleted record
    * @return
    *   the deleted content
    */
  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit codec: LMDBCodec[T]): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[T](collectionName, key))

  /** Collect collection content into the memory, use keyFilter or valueFilter to limit the amount of loaded entries.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter, the value deserialization is done **after** the filtering step
    * @param valueFilter
    *   filter lambda to select only the record your want, default is no filter
    * @param startAfter
    *   start the stream after the given key, default is start from the beginning (when backward is false) or from end (when backward is true)
    * @param backward
    *   going in reverse key order, default is false
    * @param limit
    *   maximum number of item you want to get
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   All matching records
    */
  def collect[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit codec: LMDBCodec[T]): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[T](collectionName, keyFilter, valueFilter, startAfter, backward, limit))

  /** Stream collection records, use keyFilter to apply filtering before record deserialization.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter, the value deserialization is done **after** the filtering step
    * @param startAfter
    *   start the stream after the given key, default is start from the beginning (when backward is false) or from end (when backward is true)
    * @param backward
    *   going in reverse key order, default is false
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   the stream of records
    */
  def stream[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  )(implicit codec: LMDBCodec[T]): ZStream[LMDB, StreamErrors, T] =
    ZStream.serviceWithStream(_.stream(collectionName, keyFilter, startAfter, backward))

  /** stream collection Key/record tuples, use keyFilter to apply filtering before record deserialization.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter, the value deserialization is done **after** the filtering step
    * @param startAfter
    *   start the stream after the given key, default is start from the beginning (when backward is false) or from end (when backward is true)
    * @param backward
    *   going in reverse key order, default is false
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   the tuple of key and record stream
    */
  def streamWithKeys[T](
    collectionName: CollectionName,
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  )(implicit codec: LMDBCodec[T]): ZStream[LMDB, StreamErrors, (RecordKey, T)] =
    ZStream.serviceWithStream(_.streamWithKeys(collectionName, keyFilter, startAfter, backward))
}
