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
import zio.json.{JsonDecoder, JsonEncoder}
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
case class LMDBCollection[T](name: String, lmdb: LMDB)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]) {

  def size(): IO[SizeErrors, Long] = lmdb.collectionSize(name)

  def clear(): IO[ClearErrors, Unit] = lmdb.collectionClear(name)

  def fetch(key: RecordKey): IO[FetchErrors, Option[T]] = lmdb.fetch(name, key)

  def contains(key: RecordKey): IO[ContainsErrors, Boolean] = lmdb.contains(name, key)

  def upsert(key: RecordKey, modifier: Option[T] => T): IO[UpsertErrors, Unit] =
    lmdb.upsert[T](name, key, modifier)

  def upsertOverwrite(key: RecordKey, document: T): IO[UpsertErrors, Unit] =
    lmdb.upsertOverwrite[T](name, key, document)

  def delete(key: RecordKey): IO[DeleteErrors, Option[T]] =
    lmdb.delete[T](name, key)

  def collect(keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true): IO[CollectErrors, List[T]] =
    lmdb.collect[T](name, keyFilter, valueFilter)

//  def stream(keyFilter: RecordKey => Boolean = _ => true): ZStream[Scope, StreamErrors, T] =
//    lmdb.stream(name, keyFilter)
}
