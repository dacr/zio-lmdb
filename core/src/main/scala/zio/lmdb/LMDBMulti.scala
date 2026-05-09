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
import zio.lmdb.StorageUserError._
import zio.lmdb.StorageSystemError
import zio.stream._

/** A helper class to simplify user experience by avoiding repeating collection name and data types for multi-value collections
  *
  * @param name
  *   collection name
  * @param lmdb
  *   LMDB instance
  * @tparam K
  *   the key type
  * @tparam T
  *   the data class type for collection content
  */
case class LMDBMulti[K, T](name: CollectionName, lmdb: LMDB)(implicit val kodec: KeyCodec[K], val codec: LMDBCodec[T]) {

  /** Get how many items a collection contains (total number of key-value pairs)
    *
    * @return
    *   the collection size
    */
  def size(): IO[SizeErrors, Long] = lmdb.multiSize(name)

  def clear(): IO[ClearErrors, Unit] = lmdb.multiClear(name)

  /** Get all records for a given key
    *
    * @param key
    *   the key of the records to get
    * @return
    *   a list of records
    */
  def fetch(key: K): IO[FetchErrors, List[T]] = lmdb.multiFetch(name, key)

  /** Insert a record in a multi-collection.
    *
    * @param key
    *   the key for the record
    * @param document
    *   the record content to insert
    */
  def put(key: K, document: T): IO[UpsertErrors, Unit] = lmdb.multiPut(name, key, document)

  /** Delete a specific record in a multi-collection
    *
    * @param key
    *   the key of the record to delete
    * @param document
    *   the specific document to delete
    * @return
    *   true if the record was deleted
    */
  def delete(key: K, document: T): IO[DeleteErrors, Boolean] = lmdb.multiDelete(name, key, document)

  /** Delete all records for a given key in a multi-collection
    *
    * @param key
    *   the key of the records to delete
    * @return
    *   true if any records were deleted
    */
  def deleteAll(key: K): IO[DeleteErrors, Boolean] = lmdb.multiDeleteAll(name, key)

  /** Execute a series of read operations on this collection within a single read-only transaction.
    * @param f
    *   function using collection read operations
    * @return
    *   result of the function
    */
  def readOnly[R, E, A](f: LMDBMultiReadOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] =
    lmdb.readOnly { ops =>
      f(LMDBMultiReadOps(this, ops))
    }

  /** Execute a series of read and write operations on this collection within a single read-write transaction.
    * @param f
    *   function using collection write operations
    * @return
    *   result of the function
    */
  def readWrite[R, E, A](f: LMDBMultiWriteOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | StorageUserError.NestedWriteTransactionError, A] =
    lmdb.readWrite { ops =>
      f(LMDBMultiWriteOps(this, ops))
    }

  /** Create a collection-specific read-only operations facade from a global transaction.
    * @param ops
    *   The global read-only operations
    * @return
    *   The collection-specific facade
    */
  def lift(ops: LMDBReadOps): LMDBMultiReadOps[K, T] =
    LMDBMultiReadOps(this, ops)(kodec, codec)

  /** Create a collection-specific read-write operations facade from a global transaction.
    * @param ops
    *   The global read-write operations
    * @return
    *   The collection-specific facade
    */
  def lift(ops: LMDBWriteOps): LMDBMultiWriteOps[K, T] =
    LMDBMultiWriteOps(this, ops)(kodec, codec)
}

/** Collection-specific read operations available within a transaction.
  * @tparam K
  *   key type
  * @tparam T
  *   value type
  */
case class LMDBMultiReadOps[K, T](
  collection: LMDBMulti[K, T],
  ops: LMDBReadOps
)(implicit val keyCodec: KeyCodec[K], val valueCodec: LMDBCodec[T]) {

  /** check if the collection exists */
  def exists(): IO[StorageSystemError, Boolean] = ops.multiExists(collection.name)

  /** Get how many items the collection contains */
  def size(): IO[SizeErrors, Long] = ops.multiSize(collection.name)

  /** Get all records for a given key
    * @param key
    *   the key of the records to get
    * @return
    *   a list of records
    */
  def fetch(key: K): IO[FetchErrors, List[T]] = ops.multiFetch(collection.name, key)
}

/** Collection-specific read-write operations available within a transaction.
  * @tparam K
  *   key type
  * @tparam T
  *   value type
  */
case class LMDBMultiWriteOps[K, T](
  collection: LMDBMulti[K, T],
  ops: LMDBWriteOps
)(implicit val keyCodec: KeyCodec[K], val valueCodec: LMDBCodec[T]) {

  // Delegate read operations
  private val readOps = LMDBMultiReadOps(collection, ops)
  export readOps.{collection as _, ops as _, keyCodec as _, valueCodec as _, _}

  /** Remove all the content of the collection */
  def clear(): IO[ClearErrors, Unit] = ops.multiClear(collection.name)

  /** Insert a record in the collection.
    * @param key
    *   the key for the record
    * @param document
    *   the record content to insert
    */
  def put(key: K, document: T): IO[UpsertErrors, Unit] = ops.multiPut(collection.name, key, document)

  /** Delete a specific record in the collection
    * @param key
    *   the key of the record to delete
    * @param document
    *   the specific document to delete
    * @return
    *   true if the record was deleted
    */
  def delete(key: K, document: T): IO[DeleteErrors, Boolean] = ops.multiDelete(collection.name, key, document)

  /** Delete all records for a given key in the collection
    * @param key
    *   the key of the records to delete
    * @return
    *   true if any records were deleted
    */
  def deleteAll(key: K): IO[DeleteErrors, Boolean] = ops.multiDeleteAll(collection.name, key)
}