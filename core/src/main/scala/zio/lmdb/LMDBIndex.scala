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

import zio.*
import zio.stream.*

/** A helper class to simplify user experience by avoiding repeating index name and data types
  *
  * @param name
  *   index name
  * @param description
  *   index description
  * @param lmdb
  *   LMDB service
  * @tparam FROM_KEY
  *   the type of the key
  * @tparam TO_KEY
  *   the type of the value (target key)
  */
case class LMDBIndex[FROM_KEY, TO_KEY](
  name: IndexName,
  description: Option[String],
  lmdb: LMDB
)(implicit val keyCodec: KeyCodec[FROM_KEY], val toKeyCodec: KeyCodec[TO_KEY]) {

  /** Add a mapping to the index
    * @param key
    *   the key to index
    * @param toKey
    *   the target key to map to
    */
  def index(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Unit] = lmdb.index[FROM_KEY, TO_KEY](name, key, toKey)

  /** Check if the index contains the given mapping
    * @param key
    *   the key to check
    * @param toKey
    *   the target key to check
    * @return
    *   true if the index contains the mapping
    */
  def indexContains(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.indexContains[FROM_KEY, TO_KEY](name, key, toKey)

  /** Check if the index contains the given key
    * @param key
    *   the key to check
    * @return
    *   true if the index contains the key
    */
  def hasKey(
    key: FROM_KEY
  ): IO[IndexErrors, Boolean] = lmdb.indexHasKey[FROM_KEY](name, key)

  /** Get index first record
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def head(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = lmdb.indexHead[FROM_KEY, TO_KEY](name)

  /** Get the previous record for the given key in an index
    * @param beforeThatKey
    *   the key of the reference record
    * @return
    *   some (key,targetKey) tuple or none if the key is the first one
    */
  def previous(beforeThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = lmdb.indexPrevious[FROM_KEY, TO_KEY](name, beforeThatKey)

  /** Get the next record for the given key in an index
    * @param afterThatKey
    *   the key of the reference record
    * @return
    *   some (key,targetKey) tuple or none if the key is the last one
    */
  def next(afterThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = lmdb.indexNext[FROM_KEY, TO_KEY](name, afterThatKey)

  /** Get index last record
    * @return
    *   some (key,targetKey) tuple or none if the index is empty
    */
  def last(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = lmdb.indexLast[FROM_KEY, TO_KEY](name)

  /** Remove a mapping from the index
    * @param key
    *   the key to unindex
    * @param toKey
    *   the target key to unmap
    * @return
    *   true if the mapping was found and removed
    */
  def unindex(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.unindex[FROM_KEY, TO_KEY](name, key, toKey)

  /** Get a stream of target keys for a given key in the index
    * @param key
    *   the key to look for
    * @param limitToKey
    *   limit results to values belonging to the given key only (no key jump), default is true
    * @return
    *   a stream of target keys
    */
  def indexed(
    key: FROM_KEY,
    limitToKey: Boolean = true
  ): ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)] = lmdb.indexed[FROM_KEY, TO_KEY](name, key, limitToKey)

  /** Execute a series of read operations on this index within a single read-only transaction.
    * @param f
    *   function using index read operations
    * @return
    *   result of the function
    */
  def readOnly[R, E, A](f: LMDBIndexReadOps[FROM_KEY, TO_KEY] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] =
    lmdb.readOnly { ops =>
      f(LMDBIndexReadOps(this, ops))
    }

  /** Execute a series of read and write operations on this index within a single read-write transaction.
    * @param f
    *   function using index write operations
    * @return
    *   result of the function
    */
  def readWrite[R, E, A](f: LMDBIndexWriteOps[FROM_KEY, TO_KEY] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] =
    lmdb.readWrite { ops =>
      f(LMDBIndexWriteOps(this, ops))
    }

  /** Create an index-specific read-only operations facade from a global transaction.
    * @param ops
    *   The global read-only operations
    * @return
    *   The index-specific facade
    */
  def lift(ops: LMDBReadOps): LMDBIndexReadOps[FROM_KEY, TO_KEY] =
    LMDBIndexReadOps(this, ops)(keyCodec, toKeyCodec)

  /** Create an index-specific read-write operations facade from a global transaction.
    * @param ops
    *   The global read-write operations
    * @return
    *   The index-specific facade
    */
  def lift(ops: LMDBWriteOps): LMDBIndexWriteOps[FROM_KEY, TO_KEY] =
    LMDBIndexWriteOps(this, ops)(keyCodec, toKeyCodec)

}

/** Index-specific read operations available within a transaction.
  * @tparam FROM_KEY
  *   source key type
  * @tparam TO_KEY
  *   target key type
  */
case class LMDBIndexReadOps[FROM_KEY, TO_KEY](
  index: LMDBIndex[FROM_KEY, TO_KEY],
  ops: LMDBReadOps
)(implicit val keyCodec: KeyCodec[FROM_KEY], val toKeyCodec: KeyCodec[TO_KEY]) {

  /** Check if the index exists */
  def exists(): IO[IndexErrors, Boolean] = ops.indexExists(index.name)

  /** Check if the index contains the given mapping */
  def contains(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Boolean] =
    ops.indexContains(index.name, key, toKey)

  /** Check if the index contains the given key */
  def hasKey(key: FROM_KEY): IO[IndexErrors, Boolean] =
    ops.indexHasKey(index.name, key)

  /** Get index first record */
  def head(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ops.indexHead(index.name)

  /** Get the previous record for the given key in an index */
  def previous(beforeThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ops.indexPrevious(index.name, beforeThatKey)

  /** Get the next record for the given key in an index */
  def next(afterThatKey: FROM_KEY): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ops.indexNext(index.name, afterThatKey)

  /** Get index last record */
  def last(): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    ops.indexLast(index.name)
}

/** Index-specific read-write operations available within a transaction.
  * @tparam FROM_KEY
  *   source key type
  * @tparam TO_KEY
  *   target key type
  */
case class LMDBIndexWriteOps[FROM_KEY, TO_KEY](
  index: LMDBIndex[FROM_KEY, TO_KEY],
  ops: LMDBWriteOps
)(implicit val keyCodec: KeyCodec[FROM_KEY], val toKeyCodec: KeyCodec[TO_KEY]) {

  // Delegate read operations
  private val readOps = LMDBIndexReadOps(index, ops)
  export readOps.{index as _, ops as _, keyCodec as _, toKeyCodec as _, _}

  /** Add a mapping to the index */
  def index(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Unit] =
    ops.index(index.name, key, toKey)

  /** Remove a mapping from the index */
  def unindex(key: FROM_KEY, toKey: TO_KEY): IO[IndexErrors, Boolean] =
    ops.unindex(index.name, key, toKey)
}
