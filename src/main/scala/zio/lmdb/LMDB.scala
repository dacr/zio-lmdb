package zio.lmdb

import zio.*
import zio.json.*
import zio.stream.ZStream
import zio.lmdb.StorageUserError.*
import zio.lmdb.StorageSystemError

trait LMDB {
  def platformCheck(): IO[StorageSystemError, Unit]

  def databases(): IO[StorageSystemError, List[String]]
  def databaseExists(dbName: String): IO[StorageSystemError, Boolean]
  def databaseCreate(dbName: String): IO[StorageSystemError, Unit]
  def databaseClear(dbName: String): IO[DatabaseNotFound | StorageSystemError, Unit]

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]]
  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T], JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]]
  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]]
  def delete[T](dbName: String, id: String)(using JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]]

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): IO[DatabaseNotFound | JsonFailure | StorageSystemError, List[T]]
  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[Scope, DatabaseNotFound | LMDBError, T] TODO implement stream in LMDB service
}

object LMDB {
  def platformCheck(): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def databases(): ZIO[LMDB, StorageSystemError, List[String]] = ZIO.serviceWithZIO(_.databases())

  def databaseExists(dbName: String): ZIO[LMDB, StorageSystemError, Boolean] = ZIO.serviceWithZIO(_.databaseExists(dbName))

  def databaseCreate(dbName: String): ZIO[LMDB, StorageSystemError, Unit] = ZIO.serviceWithZIO(_.databaseCreate(dbName))

  def databaseClear(dbName: String): ZIO[LMDB, DatabaseNotFound | StorageSystemError, Unit] = ZIO.serviceWithZIO(_.databaseClear(dbName))

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): ZIO[LMDB, OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]] = ZIO.serviceWithZIO(_.fetch(dbName, id))

  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsertOverwrite(dbName, id, document))

  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]] =
    ZIO.serviceWithZIO(_.upsert(dbName, id, modifier))

  def delete[T](dbName: String, id: String)(using JsonDecoder[T]): ZIO[LMDB, OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]] = ZIO.serviceWithZIO(_.delete(dbName, id))

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): ZIO[LMDB, DatabaseNotFound | JsonFailure | StorageSystemError, List[T]] =
    ZIO.serviceWithZIO(_.collect(dbName, keyFilter, valueFilter))

  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[LMDB & Scope, DatabaseNotFound | LMDBError, T] =
  //  ZStream.serviceWithZIO(_.stream(dbName, keyFilter))

  val live: ZLayer[Scope & LMDBConfig, Any, LMDB] = ZLayer(ZIO.service[LMDBConfig].flatMap(LMDBOperations.setup)).orDie
}
