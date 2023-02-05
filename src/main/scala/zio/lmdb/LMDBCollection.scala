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

import zio.ZIO
import zio.json.{JsonDecoder, JsonEncoder}
import zio.lmdb.StorageUserError.*
import zio.lmdb.StorageSystemError

/**
 * A helper class to simplify user experience by avoiding repeating collection name and data types
 *
 * @param name collection name
 * @param JsonEncoder[T]
 * @param JsonDecoder[T]
 * @tparam T the data class type for collection content
 */
case class LMDBCollection[T](name: String)(using JsonEncoder[T], JsonDecoder[T]) {

  def size(): ZIO[LMDB, CollectionNotFound | StorageSystemError, Long] = ZIO.serviceWithZIO(_.collectionSize(name))

  def fetch(key: RecordKey): ZIO[LMDB, FetchErrors, Option[T]] = LMDB.fetch(name, key)

  def upsert(key: RecordKey, modifier: Option[T] => T): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsert[T](name, key, modifier))

  def upsertOverwrite(key: RecordKey, document: T): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsertOverwrite[T](name, key, document))

  def delete(key: RecordKey): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[T](name, key))

  def collect(keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[T](name, keyFilter, valueFilter))

  // def stream(keyFilter: RecordKey => Boolean = _ => true): ZStream[Scope, DatabaseNotFound | LMDBError, T] = //TODO implement stream in LMDB service
  //    ZStream.serviceWithZIO(_.stream(colName, keyFilter))
}
