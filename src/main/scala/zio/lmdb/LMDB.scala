package zio.lmdb

import zio.*
import zio.json.{JsonEncoder, *}
import zio.stream.ZStream

trait LMDB {
  def platformCheck(): IO[LMDBError, Unit]

  def databases(): IO[LMDBError, List[String]]
  def databaseExists(dbName: String): IO[LMDBError, Boolean]
  def databaseCreate(dbName: String): IO[LMDBError, Unit]
  def databaseClear(dbName: String): IO[DatabaseNotFound | LMDBError, Unit]

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): IO[DatabaseNotFound | JsonFailure | LMDBError, Option[T]]
  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T]): IO[DatabaseNotFound | LMDBError, T]
  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): IO[DatabaseNotFound | JsonFailure | LMDBError, T]
  def delete(dbName: String, id: String): IO[DatabaseNotFound | LMDBError, Boolean]

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): IO[DatabaseNotFound | JsonFailure | LMDBError, List[T]]
  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[Scope, DatabaseNotFound | LMDBError, T] TODO implement stream in LMDB service
}

object LMDB {
  def platformCheck(): ZIO[LMDB, LMDBError, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def databases(): ZIO[LMDB, LMDBError, List[String]]                              = ZIO.serviceWithZIO(_.databases())
  def databaseExists(dbName: String): ZIO[LMDB, LMDBError, Boolean]                = ZIO.serviceWithZIO(_.databaseExists(dbName))
  def databaseCreate(dbName: String): ZIO[LMDB, LMDBError, Unit]                   = ZIO.serviceWithZIO(_.databaseCreate(dbName))
  def databaseClear(dbName: String): ZIO[LMDB, DatabaseNotFound | LMDBError, Unit] = ZIO.serviceWithZIO(_.databaseClear(dbName))

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): ZIO[LMDB, DatabaseNotFound | JsonFailure | LMDBError, Option[T]]                                    = ZIO.serviceWithZIO(_.fetch(dbName, id))
  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T]): ZIO[LMDB, DatabaseNotFound | LMDBError, T]                                   = ZIO.serviceWithZIO(_.upsertOverwrite(dbName, id, document))
  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): ZIO[LMDB, DatabaseNotFound | JsonFailure | LMDBError, T] = ZIO.serviceWithZIO(_.upsert(dbName, id, modifier))
  def delete(dbName: String, id: String): ZIO[LMDB, DatabaseNotFound | LMDBError, Boolean]                                                                            = ZIO.serviceWithZIO(_.delete(dbName, id))

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): ZIO[LMDB, DatabaseNotFound | JsonFailure | LMDBError, List[T]] =
    ZIO.serviceWithZIO(_.collect(dbName, keyFilter, valueFilter))

  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[LMDB & Scope, DatabaseNotFound | LMDBError, T] =
  //  ZStream.serviceWithZIO(_.stream(dbName, keyFilter))

  val live: ZLayer[Scope & LMDBConfig, Any, LMDB] = ZLayer(ZIO.service[LMDBConfig].flatMap(LMDBOperations.setup)).orDie
}
