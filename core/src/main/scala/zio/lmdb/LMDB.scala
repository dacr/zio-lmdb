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
import zio.lmdb.keycodecs.KeyCodec

import zio._
import zio.stream.ZStream
import zio.config._

/** Lightning Memory-Mapped Database (LMDB) abstraction layer for ZIO. */
trait LMDB {

  /** Get the used storage directory in your file system.
    * @return
    *   storage directory path
    */
  def databasePath: String

  /** Check LMDB server current configuration compatibility */
  def platformCheck(): IO[StorageSystemError, Unit]

  /** List all available collections
    * @return
    *   the list of collection names
    */
  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  /** check if a collection exists
    * @param name
    *   the collection name
    * @return
    *   true if the collection exists
    */
  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  /** Create a collection and return the collection helper facade.
    * @param name
    *   the collection name
    * @param failIfExists
    *   raise an error if the collection already exists, default to true
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the records
    * @return
    *   the collection helper facade
    */
  def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CreateErrors, LMDBCollection[K, T]]

  /** Create a collection
    * @param name
    *   the collection name
    */
  def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit]

  /** Get a collection helper facade.
    * @param name
    *   the collection name
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the records
    * @return
    *   the collection helper facade
    */
  def collectionGet[K, T](name: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[GetErrors, LMDBCollection[K, T]]

  /** Get how many items a collection contains
    * @param name
    *   the collection name
    * @return
    *   the collection size
    */
  def collectionSize(name: CollectionName): IO[SizeErrors, Long]

  /** Remove all the content of a collection
    * @param name
    *   the collection name
    */
  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]

  /** Drop a collection
    * @param name
    *   the collection name
    */
  def collectionDrop(name: CollectionName): IO[DropErrors, Unit]

  /** Get a collection record
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to get
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]]

  /** Fetches an optional value from the specified collection at the given index.
    * @param collectionName
    *   the collection name
    * @param index
    *   the index within the collection to fetch the value
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some (key,record) tuple or none if index is out of bounds
    */
  def fetchAt[K, T](collectionName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get collection first record
    * @param collectionName
    *   the collection name
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get the previous record for the given key
    * @param collectionName
    *   the collection name
    * @param beforeThatKey
    *   the key of the reference record
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some (key,record) tuple or none if the key is the first one
    */
  def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get the next record for the given key
    * @param collectionName
    *   the collection name
    * @param afterThatKey
    *   the key of the reference record
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some (key,record) tuple or none if the key is the last one
    */
  def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get collection last record
    * @param collectionName
    *   the collection name
    * @tparam K
    *   key type
    * @tparam T
    *   the data type of the record
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Check if a collection contains the given key
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to look for
    * @tparam K
    *   key type
    * @return
    *   true if the key is used by the given collection
    */
  def contains[K](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[ContainsErrors, Boolean]

  /** atomically update a record in a collection.
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record update
    * @param modifier
    *   the lambda used to update the record content
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   the updated record if a record exists for the given key
    */
  def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]]

  /** update or insert atomically a record in a collection.
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   the updated or inserted record
    */
  def upsert[K, T](collectionName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T]

  /** Overwrite or insert a record in a collection.
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    */
  def upsertOverwrite[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit]

  /** Delete a record in a collection
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to delete
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   the deleted content
    */
  def delete[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]]

  /** Collect collection content into the memory.
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want
    * @param valueFilter
    *   filter lambda to select only the record your want
    * @param startAfter
    *   start the stream after the given key
    * @param backward
    *   going in reverse key order
    * @param limit
    *   maximum number of item you want to get
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   All matching records
    */
  def collect[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CollectErrors, List[T]]

  /** Stream collection records.
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want
    * @param startAfter
    *   start the stream after the given key
    * @param backward
    *   going in reverse key order
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   the stream of records
    */
  def stream[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T]

  /** stream collection Key/record tuples.
    * @param collectionName
    *   the collection name
    * @param keyFilter
    *   filter lambda to select only the keys you want
    * @param startAfter
    *   start the stream after the given key
    * @param backward
    *   going in reverse key order
    * @tparam K
    *   key type
    * @tparam T
    *   record type
    * @return
    *   the tuple of key and record stream
    */
  def streamWithKeys[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)]

  /** Create an index
    * @param name
    *   the index name
    * @param failIfExists
    *   raise an error if the index already exists, default to true
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   the index helper facade
    */
  def indexCreate[FROM_KEY, TO_KEY](name: IndexName, failIfExists: Boolean = true)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]]

  /** Get an index helper facade.
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   the index helper facade
    */
  def indexGet[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]]

  /** Check if an index exists
    * @param name
    *   the index name
    * @return
    *   true if the index exists
    */
  def indexExists(name: IndexName): IO[IndexErrors, Boolean]

  /** Drop an index
    * @param name
    *   the index name
    */
  def indexDrop(name: IndexName): IO[IndexErrors, Unit]

  /** List all available indexes
    * @return
    *   the list of index names
    */
  def indexes(): IO[IndexErrors, List[IndexName]]

  /** Add a mapping to an index
    * @param name
    *   the index name
    * @param key
    *   the key to index
    * @param targetKey
    *   the target key to map to
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    */
  def index[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit]

  /** Check if an index contains the given mapping
    * @param name
    *   the index name
    * @param key
    *   the key to check
    * @param targetKey
    *   the target key to check
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   true if the index contains the mapping
    */
  def indexContains[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean]

  /** Check if an index contains the given key
    * @param name
    *   the index name
    * @param key
    *   the key to check
    * @tparam FROM_KEY
    *   the type of the key
    * @return
    *   true if the index contains the key
    */
  def indexHasKey[FROM_KEY](
    name: IndexName,
    key: FROM_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY]): IO[IndexErrors, Boolean]

  /** Get index first record
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def indexHead[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]

  /** Get the previous record for the given key in an index
    * @param name
    *   the index name
    * @param beforeThatKey
    *   the key of the reference record
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the key is the first one
    */
  def indexPrevious[FROM_KEY, TO_KEY](name: IndexName, beforeThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]

  /** Get the next record for the given key in an index
    * @param name
    *   the index name
    * @param afterThatKey
    *   the key of the reference record
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the key is the last one
    */
  def indexNext[FROM_KEY, TO_KEY](name: IndexName, afterThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]

  /** Get index last record
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def indexLast[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]]

  /** Remove a mapping from an index
    * @param name
    *   the index name
    * @param key
    *   the key to unindex
    * @param targetKey
    *   the target key to unmap
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   true if the mapping was found and removed
    */
  def unindex[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean]

  /** Get a stream of target keys for a given key in an index
    * @param name
    *   the index name
    * @param key
    *   the key to look for
    * @param limitToKey
    *   limit results to values belonging to the given key only (no key jump), default is true
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   a stream of target keys
    */
  def indexed[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    limitToKey: Boolean = true
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)]

  /** Execute a series of read operations within a single read-only transaction.
    * @param f
    *   function using read-only operations
    * @return
    *   result of the function
    */
  def readOnly[R, E, A](f: LMDBReadOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A]

  /** Execute a series of read and write operations within a single read-write transaction.
    * @param f
    *   function using read-write operations
    * @return
    *   result of the function
    */
  def readWrite[R, E, A](f: LMDBWriteOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A]

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
    *   the data type of the records which must be LMDB serializable
    * @return
    *   the collection helper facade
    */
  def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, CreateErrors, LMDBCollection[K, T]] = ZIO.serviceWithZIO(_.collectionCreate(name, failIfExists))

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
  def collectionGet[K, T](name: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, GetErrors, LMDBCollection[K, T]] = ZIO.serviceWithZIO(_.collectionGet(name))

  /** Get how many items a collection contains
    *
    * @param name
    *   the collection name
    * @tparam T
    *   the data type of the records which must be LMDB serializable
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
  def fetch[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[T]] = ZIO.serviceWithZIO(_.fetch(collectionName, key))

  /** Fetches an optional value of type `T` from the specified collection at the given index. This is non optimal feature that requires walking through available records using the default ordering until the given index is reached.
    *
    * @param collectionName
    *   the name of the collection from which the value will be fetched
    * @param index
    *   the index within the collection to fetch the value
    * @param codec
    *   an implicit codec used to encode and decode the value of type `T`
    * @return
    *   a ZIO effect that, when executed, may produce either a FetchErrors error or an Option containing the fetched value of type `T`
    */
  def fetchAt[K, T](collectionName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(K, T)]] = ZIO.serviceWithZIO(_.fetchAt(collectionName, index))

  /** Get collection first record
    *
    * @param collectionName
    *   the collection name
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(K, T)]] = ZIO.serviceWithZIO(_.head(collectionName))

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
  def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(K, T)]] = ZIO.serviceWithZIO(_.previous(collectionName, beforeThatKey))

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
  def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(K, T)]] = ZIO.serviceWithZIO(_.next(collectionName, afterThatKey))

  /** Get collection last record
    *
    * @param collectionName
    *   the collection name
    * @tparam T
    *   the data type of the record which must be Json serializable
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, FetchErrors, Option[(K, T)]] = ZIO.serviceWithZIO(_.last(collectionName))

  /** Check if a collection contains the given key
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to look for
    * @return
    *   true if the key is used by the given collection
    */
  def contains[K](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): ZIO[LMDB, ContainsErrors, Boolean] = ZIO.serviceWithZIO(_.contains(collectionName, key))

  /** atomically update a record in a collection.
    *
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @tparam T
    *   the data type of the record which must be LMDB serializable
    * @return
    *   the updated record if a record exists for the given key
    */

  def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, UpdateErrors, Option[T]] = {
    ZIO.serviceWithZIO(_.update[K, T](collectionName, key, modifier))
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
    * @return
    *   the updated or inserted record
    */
  def upsert[K, T](collectionName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, UpsertErrors, T] = {
    ZIO.serviceWithZIO(_.upsert[K, T](collectionName, key, modifier))
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
  def upsertOverwrite[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, UpsertErrors, Unit] =
    ZIO.serviceWithZIO(_.upsertOverwrite[K, T](collectionName, key, document))

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
  def delete[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[K, T](collectionName, key))

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
  def collect[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[K, T](collectionName, keyFilter, valueFilter, startAfter, backward, limit))

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
  def stream[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[LMDB, StreamErrors, T] =
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
  def streamWithKeys[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[LMDB, StreamErrors, (K, T)] =
    ZStream.serviceWithStream(_.streamWithKeys(collectionName, keyFilter, startAfter, backward))

  /** Create an index
    * @param name
    *   the index name
    * @param failIfExists
    *   raise an error if the index already exists, default to true
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   the index helper facade
    */
  def indexCreate[FROM_KEY, TO_KEY](name: IndexName, failIfExists: Boolean = true)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] =
    ZIO.serviceWithZIO(_.indexCreate(name, failIfExists))

  /** Get an index helper facade.
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   the index helper facade
    */
  def indexGet[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] =
    ZIO.serviceWithZIO(_.indexGet(name))

  /** Check if an index exists
    * @param name
    *   the index name
    * @return
    *   true if the index exists
    */
  def indexExists(name: IndexName): ZIO[LMDB, IndexErrors, Boolean] =
    ZIO.serviceWithZIO(_.indexExists(name))

  /** Drop an index
    * @param name
    *   the index name
    */
  def indexDrop(name: IndexName): ZIO[LMDB, IndexErrors, Unit] =
    ZIO.serviceWithZIO(_.indexDrop(name))

  /** List all available indexes
    * @return
    *   the list of index names
    */
  def indexes(): ZIO[LMDB, IndexErrors, List[IndexName]] =
    ZIO.serviceWithZIO(_.indexes())

  /** Add a mapping to an index
    * @param name
    *   the index name
    * @param key
    *   the key to index
    * @param targetKey
    *   the target key to map to
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    */
  def index[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, IndexErrors, Unit] =
    ZIO.serviceWithZIO(_.index(name, key, targetKey))

  /** Check if an index contains the given mapping
    * @param name
    *   the index name
    * @param key
    *   the key to check
    * @param targetKey
    *   the target key to check
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   true if the index contains the mapping
    */
  def indexContains[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, IndexErrors, Boolean] =
    ZIO.serviceWithZIO(_.indexContains(name, key, targetKey))

  /** Check if an index contains the given key
    * @param name
    *   the index name
    * @param key
    *   the key to check
    * @tparam FROM_KEY
    *   the type of the key
    * @return
    *   true if the index contains the key
    */
  def indexHasKey[FROM_KEY](
    name: IndexName,
    key: FROM_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY]): ZIO[LMDB, IndexErrors, Boolean] =
    ZIO.serviceWithZIO(_.indexHasKey(name, key))

  /** Get index first record
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def indexHead[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ZIO.serviceWithZIO(_.indexHead(name))

  /** Get the previous record for the given key in an index
    * @param name
    *   the index name
    * @param beforeThatKey
    *   the key of the reference record
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the key is the first one
    */
  def indexPrevious[FROM_KEY, TO_KEY](name: IndexName, beforeThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ZIO.serviceWithZIO(_.indexPrevious(name, beforeThatKey))

  /** Get the next record for the given key in an index
    * @param name
    *   the index name
    * @param afterThatKey
    *   the key of the reference record
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the key is the last one
    */
  def indexNext[FROM_KEY, TO_KEY](name: IndexName, afterThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ZIO.serviceWithZIO(_.indexNext(name, afterThatKey))

  /** Get index last record
    * @param name
    *   the index name
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def indexLast[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ZIO.serviceWithZIO(_.indexLast(name))

  /** Remove a mapping from an index
    * @param name
    *   the index name
    * @param key
    *   the key to unindex
    * @param targetKey
    *   the target key to unmap
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   true if the mapping was found and removed
    */
  def unindex[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[LMDB, IndexErrors, Boolean] =
    ZIO.serviceWithZIO(_.unindex(name, key, targetKey))

  /** Get a stream of target keys for a given key in an index
    * @param name
    *   the index name
    * @param key
    *   the key to look for
    * @param limitToKey
    *   limit results to values belonging to the given key only (no key jump), default is true
    * @tparam FROM_KEY
    *   the type of the key
    * @tparam TO_KEY
    *   the type of the target key
    * @return
    *   a stream of target keys
    */
  def indexed[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    limitToKey: Boolean = true
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZStream[LMDB, IndexErrors, (FROM_KEY, TO_KEY)] =
    ZStream.serviceWithStream(_.indexed(name, key, limitToKey))

  /** Execute a series of read operations within a single read-only transaction.
    * @param f
    *   function using read-only operations
    * @return
    *   result of the function
    */
  def readOnly[R, E, A](f: LMDBReadOps => ZIO[R, E, A]): ZIO[LMDB & R, E | StorageSystemError, A] =
    ZIO.serviceWithZIO[LMDB](_.readOnly(f))

  /** Execute a series of read and write operations within a single read-write transaction.
    * @param f
    *   function using read-write operations
    * @return
    *   result of the function
    */
  def readWrite[R, E, A](f: LMDBWriteOps => ZIO[R, E, A]): ZIO[LMDB & R, E | StorageSystemError, A] =
    ZIO.serviceWithZIO[LMDB](_.readWrite(f))
}
