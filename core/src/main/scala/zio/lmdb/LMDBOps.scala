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

/** LMDB operations available within a read-only transaction context. */
trait LMDBReadOps {

  /** check if a collection exists
    * @param name
    *   the collection name
    * @return
    *   true if the collection exists
    */
  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  /** Get how many items a collection contains
    * @param name
    *   the collection name
    * @return
    *   the collection size
    */
  def collectionSize(name: CollectionName): IO[SizeErrors, Long]

  /** Get a collection record
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to get
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]]

  /** Fetches an optional value of type `T` from the specified collection at the given index. This is non optimal feature that requires walking through available records using the default ordering until the given index is reached.
    * @param collectionName
    *   the collection name
    * @param index
    *   the index within the collection to fetch the value
    * @return
    *   some record or none if index is out of bounds
    */
  def fetchAt[K, T](collectionName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get collection first record
    * @param collectionName
    *   the collection name
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get the previous record for the given key
    * @param collectionName
    *   the collection name
    * @param beforeThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the first one
    */
  def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get the next record for the given key
    * @param collectionName
    *   the collection name
    * @param afterThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the last one
    */
  def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Get collection last record
    * @param collectionName
    *   the collection name
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]

  /** Check if a collection contains the given key
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to look for
    * @return
    *   true if the key is used by the given collection
    */
  def contains[K](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[ContainsErrors, Boolean]

  /** Collect collection content into the memory, use keyFilter or valueFilter to limit the amount of loaded entries.
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

  /** check if an index exists
    * @param name
    *   the index name
    * @return
    *   true if the index exists
    */
  def indexExists(name: IndexName): IO[IndexErrors, Boolean]

  /** Check if an index contains the given key and target key
    * @param name
    *   the index name
    * @param key
    *   the key to check
    * @param targetKey
    *   the target key to check
    * @return
    *   true if the index contains the mapping
    */
  def indexContains[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean]
}

/** LMDB operations available within a read-write transaction context. */
trait LMDBWriteOps extends LMDBReadOps {

  /** Remove all the content of a collection
    * @param name
    *   the collection name
    */
  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]

  /** atomically update a record in a collection.
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record update
    * @param modifier
    *   the lambda used to update the record content
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
    * @return
    *   the updated or inserted record
    */
  def upsert[K, T](collectionName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T]

  /** Overwrite or insert a record in a collection. If the key is already being used for a record then the previous record will be overwritten by the new one.
    * @param collectionName
    *   the collection name
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    */
  def upsertOverwrite[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit]

  /** Delete a record in a collection
    * @param collectionName
    *   the collection name
    * @param key
    *   the key of the record to delete
    * @return
    *   the deleted content
    */
  def delete[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]]

  /** Add a mapping to an index
    * @param name
    *   the index name
    * @param key
    *   the key to index
    * @param targetKey
    *   the target key to map to
    */
  def index[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit]

  /** Remove a mapping from an index
    * @param name
    *   the index name
    * @param key
    *   the key to unindex
    * @param targetKey
    *   the target key to unmap
    * @return
    *   true if the mapping was found and removed
    */
  def unindex[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean]
}
