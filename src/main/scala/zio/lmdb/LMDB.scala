/*
 * Copyright 2023 David Crosson
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
import zio.json._
import zio.stream.ZStream
import zio.config._

trait LMDB {

  def databasePath: String

  def platformCheck(): IO[StorageSystemError, Unit]

  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  def collectionCreate[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CreateErrors, LMDBCollection[T]]

  def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit]

  def collectionGet[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[GetErrors, LMDBCollection[T]]

  def collectionSize(name: CollectionName): IO[SizeErrors, Long]

  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]

  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[FetchErrors, Option[T]]

  def contains(collectionName: CollectionName, key: RecordKey): IO[ContainsErrors, Boolean]

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, Unit]

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, Unit]

  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[DeleteErrors, Option[T]]

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CollectErrors, List[T]]

  def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[Any, StreamErrors, T]

  def streamWithKeys[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[Any, StreamErrors, (RecordKey, T)]
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
    * @tparam T
    *   the data type of the records which must be Json serializable
    * @return
    *   the collection helper facade
    */
  def collectionCreate[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, CreateErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionCreate(name))

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
  def collectionGet[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, GetErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionGet(name))

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
  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, FetchErrors, Option[T]] = ZIO.serviceWithZIO(_.fetch(collectionName, key))

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
    */
  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, UpsertErrors, Unit] =
    ZIO.serviceWithZIO(_.upsert[T](collectionName, key, modifier))

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
  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, UpsertErrors, Unit] =
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
  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[T](collectionName, key))

  /** Collect collection content into the memory, use keyFilter or valueFilter to limit the amount of loaded entries.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter
    * @param valueFilter
    *   filter lambda to select only the record your want, default is no filter
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   All matching records
    */
  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[T](collectionName, keyFilter, valueFilter))

  /** Stream collection records, use keyFilter to apply filtering before record deserialization.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   the stream of records
    */
  def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[LMDB, StreamErrors, T] =
    ZStream.serviceWithStream(_.stream(collectionName, keyFilter))

  /** stream collection Key/record tuples, use keyFilter to apply filtering before record deserialization.
    *
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   the tuple of key and record stream
    */
  def streamWithKeys[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[LMDB, StreamErrors, (RecordKey, T)] =
    ZStream.serviceWithStream(_.streamWithKeys(collectionName, keyFilter))
}
