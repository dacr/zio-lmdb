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
import zio.json._
import zio.stream.ZStream

trait LMDB {
  def platformCheck(): IO[StorageSystemError, Unit]

  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  def collectionCreate[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CreateErrors, LMDBCollection[T]]

  def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit]

  def collectionGet[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[GetErrors, LMDBCollection[T]]

  def collectionSize(name: CollectionName): IO[SizeErrors, Long]

  def collectionClear(name: CollectionName): IO[ClearErrors, Unit]

  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[FetchErrors, Option[T]]

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[DeleteErrors, Option[T]]

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CollectErrors, List[T]]

//  def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[Scope, StreamErrors, T]
}

object LMDB {

  val config: Config[LMDBConfig] = (
    Config.string("databaseName").withDefault("default") ++
      Config.string("databasesHome").optional ++
      Config.boolean("fileSystemSynchronized").withDefault(false) ++
      Config.int("maxReaders").withDefault(100) ++
      Config.bigInt("mapSize").withDefault(BigInt(100_000_000_000L)) ++
      Config.int("maxCollections").withDefault(10_000)
  ).map { case (databaseName, databasesHome, fileSystemSynchronized, maxReaders, mapSize, maxCollections) =>
    LMDBConfig(
      databaseName = databaseName,
      databasesHome = databasesHome,
      fileSystemSynchronized = fileSystemSynchronized,
      maxReaders = maxReaders,
      mapSize = mapSize,
      maxCollections = maxCollections
    )
  }

  val live: ZLayer[Scope, Any, LMDB] = ZLayer.fromZIO(
    for {
      config <- ZIO.config(LMDB.config)
      lmdb   <- LMDBLive.setup(config)
    } yield lmdb
  )

  def platformCheck(): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def collectionsAvailable(): ZIO[LMDB, StorageSystemError, List[CollectionName]] = ZIO.serviceWithZIO(_.collectionsAvailable())

  def collectionExists(name: CollectionName): ZIO[LMDB, StorageSystemError, Boolean] = ZIO.serviceWithZIO(_.collectionExists(name))

  def collectionCreate[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, CreateErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionCreate(name))

  def collectionAllocate(name: CollectionName): ZIO[LMDB, CreateErrors, Unit] = ZIO.serviceWithZIO(_.collectionAllocate(name))

  def collectionGet[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, GetErrors, LMDBCollection[T]] = ZIO.serviceWithZIO(_.collectionGet(name))

  def collectionSize(name: CollectionName): ZIO[LMDB, SizeErrors, Long] = ZIO.serviceWithZIO(_.collectionSize(name))

  def collectionClear(name: CollectionName): ZIO[LMDB, ClearErrors, Unit] = ZIO.serviceWithZIO(_.collectionClear(name))

  def fetch[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, FetchErrors, Option[T]] = ZIO.serviceWithZIO(_.fetch(collectionName, key))

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsert[T](collectionName, key, modifier))

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, UpsertErrors, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsertOverwrite[T](collectionName, key, document))

  def delete[T](collectionName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, DeleteErrors, Option[T]] =
    ZIO.serviceWithZIO(_.delete[T](collectionName, key))

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZIO[LMDB, CollectErrors, List[T]] =
    ZIO.serviceWithZIO(_.collect[T](collectionName, keyFilter, valueFilter))

//  def stream[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[Scope & LMDB, StreamErrors, T] =
//    ZStream.serviceWithZIO(_.stream(collectionName, keyFilter))

}
