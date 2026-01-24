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

trait LMDBReadOps {
  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]
  def collectionSize(name: CollectionName): IO[SizeErrors, Long]
  
  def fetch[K, T](collectionName: CollectionName, key: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]]
  def fetchAt[K, T](collectionName: CollectionName, index: Long)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]
  
  def head[K, T](collectionName: CollectionName)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]
  def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]
  def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]
  def last[K, T](collectionName: CollectionName)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]]
  
  def contains[K](collectionName: CollectionName, key: K)(implicit kodec: LMDBKodec[K]): IO[ContainsErrors, Boolean]
  
  def collect[K, T](
    collectionName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[CollectErrors, List[T]]

  def indexExists(name: IndexName): IO[IndexErrors, Boolean]
  def indexContains[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Boolean]
}

trait LMDBWriteOps extends LMDBReadOps {
  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]
  
  def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]]
  def upsert[K, T](collectionName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T]
  def upsertOverwrite[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit]
  
  def delete[K, T](collectionName: CollectionName, key: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]]

  def index[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Unit]

  def unindex[FROM_KEY, TO_KEY](
    name: IndexName,
    key: FROM_KEY,
    targetKey: TO_KEY
  )(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Boolean]
}
