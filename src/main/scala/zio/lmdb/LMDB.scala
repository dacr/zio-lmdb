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

import zio.*
import zio.json.*
import zio.stream.ZStream
import zio.lmdb.StorageUserError.*
import zio.lmdb.StorageSystemError

trait LMDB {
  def platformCheck(): IO[StorageSystemError, Unit]

  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  def collectionCreate[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectionAlreadExists | StorageSystemError, LMDBCollection[T]]

  def collectionGet[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectionNotFound | StorageSystemError, LMDBCollection[T]]

  def collectionSize(name: CollectionName): IO[CollectionNotFound | StorageSystemError, Long]

  def collectionClear(name: CollectionName): IO[CollectionNotFound | StorageSystemError, Unit]

  def fetch[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): IO[FetchErrors, Option[T]]

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(using JsonEncoder[T], JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def delete[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): IO[DeleteErrors, Option[T]]

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectErrors, List[T]]

  //def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(using JsonEncoder[T], JsonDecoder[T]): ZStream[Scope, CollectErrors, T]
}

object LMDB {

  def platformCheck(): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def collectionsAvailable(): ZIO[LMDB, StorageSystemError, List[CollectionName]] = ZIO.serviceWithZIO(_.collectionsAvailable())

  def collectionExists(name: CollectionName): ZIO[LMDB, StorageSystemError, Boolean] = ZIO.serviceWithZIO(_.collectionExists(name))

  def collectionCreate[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, CollectionAlreadExists | StorageSystemError, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionCreate(name))

  def collectionGet[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, CollectionNotFound | StorageSystemError, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionGet(name))

  def collectionSize(name: CollectionName): ZIO[LMDB, CollectionNotFound | StorageSystemError, Long] = ZIO.serviceWithZIO(_.collectionSize(name))

  def collectionClear(name: CollectionName): ZIO[LMDB, CollectionNotFound | StorageSystemError, Unit] = ZIO.serviceWithZIO(_.collectionClear(name))

  def fetch[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, FetchErrors, Option[T]] = ZIO.serviceWithZIO(_.fetch(collectionName, key))

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsert[T](collectionName, key, modifier))

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsertOverwrite[T](collectionName, key, document))

  def delete[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[T](collectionName, key))

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[T](collectionName, keyFilter, valueFilter))

  //def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(using JsonEncoder[T], JsonDecoder[T]): ZStream[Scope & LMDB, CollectErrors, T] = // TODO implement stream in LMDB service
  //  ZStream.serviceWithZIO(_.stream(collectionName, keyFilter))

  val live: ZLayer[Scope & LMDBConfig, Any, LMDB] = ZLayer(ZIO.service[LMDBConfig].flatMap(LMDBLive.setup)).orDie
}
