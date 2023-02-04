package zio.lmdb

import zio.*
import zio.json.*
import zio.stream.ZStream
import zio.lmdb.StorageUserError.*
import zio.lmdb.StorageSystemError

type FetchErrors   = OverSizedKey | CollectionNotFound | JsonFailure | StorageSystemError
type UpsertErrors  = OverSizedKey | CollectionNotFound | JsonFailure | StorageSystemError
type DeleteErrors  = OverSizedKey | CollectionNotFound | JsonFailure | StorageSystemError
type CollectErrors = CollectionNotFound | JsonFailure | StorageSystemError

case class Collection[T](name: String)(using JsonEncoder[T], JsonDecoder[T]) {
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

trait LMDB {
  def platformCheck(): IO[StorageSystemError, Unit]

  def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]]

  def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean]

  def collectionCreate[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectionAlreadExists | StorageSystemError, Collection[T]]

  def collectionGet[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectionNotFound | StorageSystemError, Collection[T]]

  def collectionClear(name: CollectionName): IO[CollectionNotFound | StorageSystemError, Unit]


  def fetch[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): IO[FetchErrors, Option[T]]

  def upsertOverwrite[T](collectionName: CollectionName, key: RecordKey, document: T)(using JsonEncoder[T], JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def upsert[T](collectionName: CollectionName, key: RecordKey, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): IO[UpsertErrors, UpsertState[T]]

  def delete[T](collectionName: CollectionName, key: RecordKey)(using JsonEncoder[T], JsonDecoder[T]): IO[DeleteErrors, Option[T]]

  def collect[T](collectionName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonEncoder[T], JsonDecoder[T]): IO[CollectErrors, List[T]]
}

object LMDB {

  def platformCheck(): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def collectionsAvailable(): ZIO[LMDB, StorageSystemError, List[CollectionName]] = ZIO.serviceWithZIO(_.collectionsAvailable())

  def collectionExists(name: CollectionName): ZIO[LMDB, StorageSystemError, Boolean] = ZIO.serviceWithZIO(_.collectionExists(name))

  def collectionCreate[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, CollectionAlreadExists | StorageSystemError, Collection[T]] = ZIO.serviceWithZIO(_.collectionCreate(name))

  def collectionGet[T](name: CollectionName)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, CollectionNotFound | StorageSystemError, Collection[T]] = ZIO.serviceWithZIO(_.collectionGet(name))

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

  // def stream(keyFilter: RecordKey => Boolean = _ => true): ZStream[Scope, DatabaseNotFound | LMDBError, T] = //TODO implement stream in LMDB service
  //    ZStream.serviceWithZIO(_.stream(colName, keyFilter))

  val live: ZLayer[Scope & LMDBConfig, Any, LMDB] = ZLayer(ZIO.service[LMDBConfig].flatMap(LMDBOperations.setup)).orDie
}
