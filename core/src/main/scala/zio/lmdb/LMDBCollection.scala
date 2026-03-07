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

/** A helper class to simplify user experience by avoiding repeating collection name and data types
  *
  * @param name
  *   collection name
  * @param codec
  * @tparam T
  *   the data class type for collection content
  */
case class LMDBCollection[K, T](name: CollectionName, lmdb: LMDB, indexUpdaters: List[IndexUpdater[K, T]] = Nil)(implicit val kodec: KeyCodec[K], val codec: LMDBCodec[T]) {

  /** Link an index to this collection, so that it is automatically updated when the collection is modified.
    * @param index The index to link
    * @param extractor A function to extract the index keys from a collection record
    * @tparam IK The index key type
    * @return A new collection facade with the index updater attached
    */
  def withIndex[IK](index: LMDBIndex[IK, K])(extractor: T => Iterable[IK]): LMDBCollection[K, T] = {
    val updater = new IndexUpdater[K, T] {
      override def onInsert(ops: LMDBWriteOps, key: K, newValue: T): IO[IndexErrors, Unit] = {
        ZIO.foreachDiscard(extractor(newValue)) { ik =>
          ops.index(index.name, ik, key)(index.keyCodec, index.toKeyCodec)
        }
      }

      override def onDelete(ops: LMDBWriteOps, key: K, oldValue: T): IO[IndexErrors, Unit] = {
        ZIO.foreachDiscard(extractor(oldValue)) { ik =>
          ops.unindex(index.name, ik, key)(index.keyCodec, index.toKeyCodec)
        }
      }

      override def onUpdate(ops: LMDBWriteOps, key: K, oldValue: T, newValue: T): IO[IndexErrors, Unit] = {
        val oldKeys = extractor(oldValue).toSet
        val newKeys = extractor(newValue).toSet
        val toRemove = oldKeys -- newKeys
        val toAdd = newKeys -- oldKeys

        for {
          _ <- ZIO.foreachDiscard(toRemove) { ik =>
                 ops.unindex(index.name, ik, key)(index.keyCodec, index.toKeyCodec)
               }
          _ <- ZIO.foreachDiscard(toAdd) { ik =>
                 ops.index(index.name, ik, key)(index.keyCodec, index.toKeyCodec)
               }
        } yield ()
      }

      override def onClear(ops: LMDBWriteOps): IO[IndexErrors, Unit] = {
        ops.indexClear(index.name)
      }
    }
    this.copy(indexUpdaters = indexUpdaters :+ updater)
  }

  /** Get how many items a collection contains
    *
    * @return
    *   the collection size
    */
  def size(): IO[SizeErrors, Long] = lmdb.collectionSize(name)

  /** Rebuild all attached indices by clearing them and re-indexing all existing records in the collection.
    * This is useful when indices are added after the collection has already been populated.
    */
  def rebuildIndexes(): IO[IndexErrors | StreamErrors | ClearErrors, Unit] = {
    if (indexUpdaters.isEmpty) ZIO.unit
    else {
      for {
        _ <- ZIO.foreachDiscard(indexUpdaters) { updater =>
               lmdb.readWrite(ops => updater.onClear(ops))
             }
        _ <- streamWithKeys().runForeach { case (key, value) =>
               ZIO.foreachDiscard(indexUpdaters) { updater =>
                 lmdb.readWrite(ops => updater.onInsert(ops, key, value))
               }
             }
      } yield ()
    }
  }
  def clear(): IO[ClearErrors | IndexErrors, Unit] = {
    if (indexUpdaters.isEmpty) lmdb.collectionClear(name)
    else readWrite(_.clear())
  }

  /** Get a collection record
    *
    * @param key
    *   the key of the record to get
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch(key: K): IO[FetchErrors, Option[T]] = lmdb.fetch(name, key)

  /** Fetches an optional value of type `T` from the specified collection at the given index. This is non optimal feature that requires walking through available records using the default ordering until the given index is reached.
    *
    * @param index
    *   the index within the collection to fetch the value
    * @param codec
    *   an implicit codec used to encode and decode the value of type `T`
    * @return
    *   a ZIO effect that, when executed, may produce either a FetchErrors error or an Option containing the fetched value of type `T`
    */
  def fetchAt(index: Long): ZIO[LMDB, FetchErrors, Option[(K, T)]] = lmdb.fetchAt(name, index)

  /** Get collection first record
    *
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def head(): IO[FetchErrors, Option[(K, T)]] = lmdb.head(name)

  /** Get the previous record for the given key
    *
    * @param beforeThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the first one
    */
  def previous(beforeThatKey: K): IO[FetchErrors, Option[(K, T)]] = lmdb.previous(name, beforeThatKey)

  /** Get the next record for the given key
    *
    * @param afterThatKey
    *   the key of the reference record
    * @return
    *   some (key,record) tuple or none if the key is the last one
    */
  def next(afterThatKey: K): IO[FetchErrors, Option[(K, T)]] = lmdb.next(name, afterThatKey)

  /** Get collection last record
    *
    * @return
    *   some (key,record) tuple or none if the collection is empty
    */
  def last(): IO[FetchErrors, Option[(K, T)]] = lmdb.last(name)

  /** Check if a collection contains the given key
    *
    * @param key
    *   the key of the record to look for
    * @return
    *   true if the key is used by the given collection
    */
  def contains(key: K): IO[ContainsErrors, Boolean] = lmdb.contains(name, key)

  /** update atomically a record in a collection.
    *
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @returns
    *   the updated record if a record exists for the given key
    */
  def update(key: K, modifier: T => T): IO[UpdateErrors | IndexErrors, Option[T]] = {
    if (indexUpdaters.isEmpty) lmdb.update(name, key, modifier)
    else readWrite(_.update(key, modifier))
  }

  /** update or insert atomically a record in a collection.
    *
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @returns
    *   the updated or inserted record
    */
  def upsert(key: K, modifier: Option[T] => T): IO[UpsertErrors | IndexErrors, T] = {
    if (indexUpdaters.isEmpty) lmdb.upsert[K, T](name, key, modifier)
    else readWrite(_.upsert(key, modifier))
  }

  /** Overwrite or insert a record in a collection. If the key is already being used for a record then the previous record will be overwritten by the new one.
    *
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    */
  def upsertOverwrite(key: K, document: T): IO[UpsertErrors | IndexErrors, Unit] = {
    if (indexUpdaters.isEmpty) lmdb.upsertOverwrite[K, T](name, key, document)
    else readWrite(_.upsertOverwrite(key, document))
  }

  /** Delete a record in a collection
    *
    * @param key
    *   the key of the record to delete
    * @return
    *   the deleted content
    */
  def delete(key: K): IO[DeleteErrors | IndexErrors, Option[T]] = {
    if (indexUpdaters.isEmpty) lmdb.delete[K, T](name, key)
    else readWrite(_.delete(key))
  }

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
    keyFilter: K => Boolean = _ => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  ): IO[CollectErrors, List[T]] =
    lmdb.collect[K, T](name, keyFilter, valueFilter, startAfter, backward, limit)

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
    keyFilter: K => Boolean = _ => true,
    startAfter: Option[K] = None,
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
    keyFilter: K => Boolean = _ => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  ): ZStream[Any, StreamErrors, (K, T)] =
    lmdb.streamWithKeys(name, keyFilter, startAfter, backward)

  /** Execute a series of read operations on this collection within a single read-only transaction.
    * @param f
    *   function using collection read operations
    * @return
    *   result of the function
    */
  def readOnly[R, E, A](f: LMDBCollectionReadOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] =
    lmdb.readOnly { ops =>
      f(LMDBCollectionReadOps(this, ops))
    }

  /** Execute a series of read and write operations on this collection within a single read-write transaction.
    * @param f
    *   function using collection write operations
    * @return
    *   result of the function
    */
  def readWrite[R, E, A](f: LMDBCollectionWriteOps[K, T] => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] =
    lmdb.readWrite { ops =>
      f(LMDBCollectionWriteOps(this, ops))
    }

  /** Create a collection-specific read-only operations facade from a global transaction.
    * @param ops
    *   The global read-only operations
    * @return
    *   The collection-specific facade
    */
  def lift(ops: LMDBReadOps): LMDBCollectionReadOps[K, T] =
    LMDBCollectionReadOps(this, ops)(kodec, codec)

  /** Create a collection-specific read-write operations facade from a global transaction.
    * @param ops
    *   The global read-write operations
    * @return
    *   The collection-specific facade
    */
  def lift(ops: LMDBWriteOps): LMDBCollectionWriteOps[K, T] =
    LMDBCollectionWriteOps(this, ops)(kodec, codec)
}

/** Collection-specific read operations available within a transaction.
  * @tparam K
  *   key type
  * @tparam T
  *   value type
  */
case class LMDBCollectionReadOps[K, T](
  collection: LMDBCollection[K, T],
  ops: LMDBReadOps
)(implicit val keyCodec: KeyCodec[K], val valueCodec: LMDBCodec[T]) {

  /** check if the collection exists */
  def exists(): IO[StorageSystemError, Boolean] = ops.collectionExists(collection.name)

  /** Get how many items the collection contains */
  def size(): IO[SizeErrors, Long] = ops.collectionSize(collection.name)

  /** Get a collection record
    * @param key
    *   the key of the record to get
    * @return
    *   some record or none if no record has been found for the given key
    */
  def fetch(key: K): IO[FetchErrors, Option[T]] = ops.fetch(collection.name, key)

  /** Fetches an optional value from the specified collection at the given index.
    * @param index
    *   the index within the collection to fetch the value
    * @return
    *   some record or none if index is out of bounds
    */
  def fetchAt(index: Long): IO[FetchErrors, Option[(K, T)]] = ops.fetchAt(collection.name, index)

  /** Get collection first record */
  def head(): IO[FetchErrors, Option[(K, T)]] = ops.head(collection.name)

  /** Get the previous record for the given key */
  def previous(beforeThatKey: K): IO[FetchErrors, Option[(K, T)]] = ops.previous(collection.name, beforeThatKey)

  /** Get the next record for the given key */
  def next(afterThatKey: K): IO[FetchErrors, Option[(K, T)]] = ops.next(collection.name, afterThatKey)

  /** Get collection last record */
  def last(): IO[FetchErrors, Option[(K, T)]] = ops.last(collection.name)

  /** Check if the collection contains the given key */
  def contains(key: K): IO[ContainsErrors, Boolean] = ops.contains(collection.name, key)

  /** Collect collection content into the memory.
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
    * @return
    *   All matching records
    */
  def collect(
    keyFilter: K => Boolean = _ => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  ): IO[CollectErrors, List[T]] =
    ops.collect(collection.name, keyFilter, valueFilter, startAfter, backward, limit)
}

/** Collection-specific read-write operations available within a transaction.
  * @tparam K
  *   key type
  * @tparam T
  *   value type
  */
case class LMDBCollectionWriteOps[K, T](
  collection: LMDBCollection[K, T],
  ops: LMDBWriteOps
)(implicit val keyCodec: KeyCodec[K], val valueCodec: LMDBCodec[T]) {

  // Delegate read operations
  private val readOps = LMDBCollectionReadOps(collection, ops)
  export readOps.{collection as _, ops as _, keyCodec as _, valueCodec as _, _}

  /** Remove all the content of the collection */
  def clear(): IO[ClearErrors | IndexErrors, Unit] = {
    for {
      _ <- ops.collectionClear(collection.name)
      _ <- ZIO.foreachDiscard(collection.indexUpdaters)(_.onClear(ops))
    } yield ()
  }

  /** update atomically a record in the collection.
    * @param key
    *   the key for the record update
    * @param modifier
    *   the lambda used to update the record content
    * @return
    *   the updated record if a record exists for the given key
    */
  def update(key: K, modifier: T => T): IO[UpdateErrors | IndexErrors, Option[T]] = {
    for {
      oldValueOpt <- ops.fetch(collection.name, key)
      newValueOpt <- ops.update(collection.name, key, modifier)
      _ <- ZIO.foreachDiscard(collection.indexUpdaters) { updater =>
             (oldValueOpt, newValueOpt) match {
               case (Some(oldVal), Some(newVal)) => updater.onUpdate(ops, key, oldVal, newVal)
               case (None, Some(newVal))         => updater.onInsert(ops, key, newVal)
               case (Some(oldVal), None)         => updater.onDelete(ops, key, oldVal)
               case (None, None)                 => ZIO.unit
             }
           }
    } yield newValueOpt
  }

  /** update or insert atomically a record in the collection.
    * @param key
    *   the key for the record upsert
    * @param modifier
    *   the lambda used to update the record content
    * @return
    *   the updated or inserted record
    */
  def upsert(key: K, modifier: Option[T] => T): IO[UpsertErrors | IndexErrors, T] = {
    for {
      oldValueOpt <- ops.fetch(collection.name, key)
      newValue    <- ops.upsert(collection.name, key, modifier)
      _ <- ZIO.foreachDiscard(collection.indexUpdaters) { updater =>
             oldValueOpt match {
               case Some(oldVal) => updater.onUpdate(ops, key, oldVal, newValue)
               case None         => updater.onInsert(ops, key, newValue)
             }
           }
    } yield newValue
  }

  /** Overwrite or insert a record in the collection.
    * @param key
    *   the key for the record upsert
    * @param document
    *   the record content to upsert
    */
  def upsertOverwrite(key: K, document: T): IO[UpsertErrors | IndexErrors, Unit] = {
    for {
      oldValueOpt <- ops.fetch(collection.name, key)
      _           <- ops.upsertOverwrite(collection.name, key, document)
      _ <- ZIO.foreachDiscard(collection.indexUpdaters) { updater =>
             oldValueOpt match {
               case Some(oldVal) => updater.onUpdate(ops, key, oldVal, document)
               case None         => updater.onInsert(ops, key, document)
             }
           }
    } yield ()
  }

  /** Delete a record in the collection
    * @param key
    *   the key of the record to delete
    * @return
    *   the deleted content
    */
  def delete(key: K): IO[DeleteErrors | IndexErrors, Option[T]] = {
    for {
      deletedOpt <- ops.delete(collection.name, key)
      _ <- ZIO.foreachDiscard(collection.indexUpdaters) { updater =>
             deletedOpt match {
               case Some(deleted) => updater.onDelete(ops, key, deleted)
               case None          => ZIO.unit
             }
           }
    } yield deletedOpt
  }
}
