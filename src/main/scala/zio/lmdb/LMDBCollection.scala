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
import zio.lmdb.StorageUserError._
import zio.lmdb.StorageSystemError
import zio.stream._

/** A helper class to simplify user experience by avoiding repeating collection name and data types
  *
  * @param name
  *   collection name
  * @param JsonEncoder[T]
  * @param JsonDecoder[T]
  * @tparam T
  *   the data class type for collection content
  */
case class LMDBCollection[T](name: String, lmdb: LMDB)(implicit codec:LMDBCodec[T]) {

  /** Get how many items a collection contains
    *
    * @return
    *   the collection size
    */
  def size(): IO[SizeErrors, Long] = lmdb.collectionSize(name)

  /** Remove all the content
    */
  def clear(): IO[ClearErrors, Unit] = lmdb.collectionClear(name)

  /** Get a collection record
    *
    * @param key
    *   the key of the record to get
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch(key: RecordKey): IO[FetchErrors, Option[T]] = lmdb.fetch(name, key)

  /** Get collection first record
    *
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head(): IO[FetchErrors, Option[(RecordKey, T)]] = lmdb.head(name)

  /** Get the previous record for the given key
    *
    * @param beforeThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the first one
    */
  def previous(beforeThatKey: RecordKey): IO[FetchErrors, Option[(RecordKey, T)]] = lmdb.previous(name, beforeThatKey)

  /** Get the next record for the given key
    *
    * @param afterThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the last one
    */
  def next(afterThatKey: RecordKey): IO[FetchErrors, Option[(RecordKey, T)]] = lmdb.next(name, afterThatKey)

  /** Get collection last record
    *
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last(): IO[FetchErrors, Option[(RecordKey, T)]] = lmdb.last(name)

  /** Check if a collection contains the given key
    *
    * @param key
    *   the key of the record to look for
    * @return
    *   true if the key is used by the given collection
    */
  def contains(key: RecordKey): IO[ContainsErrors, Boolean] = lmdb.contains(name, key)

  /** update atomically a record in a collection.
   *
   * @param key
   *   the key for the record upsert
   * @param modifier
   *   the lambda used to update the record content
   * @returns
   *   the updated record if a record exists for the given key
   */
  def update(key: RecordKey, modifier: T => T): IO[UpdateErrors, Option[T]] = lmdb.update(name, key, modifier)

  /** update or insert atomically a record in a collection.
    *
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @returns
    *   the updated or inserted record
    */
  def upsert(key: RecordKey, modifier: Option[T] => T): IO[UpsertErrors, T] =
    lmdb.upsert[T](name, key, modifier)

  /** Overwrite or insert a record in a collection. If the key is already being used for a record then the previous record will be overwritten by the new one.
    *
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    */
  def upsertOverwrite(key: RecordKey, document: T): IO[UpsertErrors, Unit] =
    lmdb.upsertOverwrite[T](name, key, document)

  /** Delete a record in a collection
    *
    * @param key
    *   the key of the record to delete
    * @return
    *   the deleted content
    */
  def delete(key: RecordKey): IO[DeleteErrors, Option[T]] =
    lmdb.delete[T](name, key)

  /** Collect collection content into the memory, use keyFilter or valueFilter to limit the amount of loaded entries.
    *
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
  def collect(
    keyFilter: RecordKey => Boolean = _ => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  ): IO[CollectErrors, List[T]] =
    lmdb.collect[T](name, keyFilter, valueFilter, startAfter, backward, limit)

  /** Stream collection records, use keyFilter to apply filtering before record deserialization.
    *
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter, the value deserialization is done **after** the filtering step
    * @param startAfter
    *   start the stream after the given key, default is start from the beginning (when backward is false) or from end (when backward is true)
    * @param backward
    *   going in reverse key order, default is false
    * @return
    *   the stream of records
    */
  def stream(
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  ): ZStream[Any, StreamErrors, T] =
    lmdb.stream(name, keyFilter, startAfter, backward)

  /** stream collection Key/record tuples, use keyFilter to apply filtering before record deserialization.
    *
    * @param keyFilter
    *   filter lambda to select only the keys you want, default is no filter, the value deserialization is done **after** the filtering step
    * @param startAfter
    *   start the stream after the given key, default is start from the beginning (when backward is false) or from end (when backward is true)
    * @param backward
    *   going in reverse key order, default is false
    * @return
    *   the tuple of key and record stream
    */
  def streamWithKeys(
    keyFilter: RecordKey => Boolean = _ => true,
    startAfter: Option[RecordKey] = None,
    backward: Boolean = false
  ): ZStream[Any, StreamErrors, (RecordKey, T)] =
    lmdb.streamWithKeys(name, keyFilter, startAfter, backward)
}
