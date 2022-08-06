package zio.lmdb

import zio.*
import zio.json.{JsonEncoder, *}
import zio.stream.ZStream

trait LMDB {
  def platformCheck(): Task[Unit]

  def databases(): Task[List[String]]
  def databaseExists(dbName: String): Task[Boolean]
  def databaseCreate(dbName: String): Task[Unit]
  def databaseClear(dbName: String): Task[Unit]

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): Task[Option[T]]
  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T]): Task[T]
  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): Task[T]
  def delete(dbName: String, id: String): Task[Boolean]

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): Task[List[T]]
  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[Scope, Throwable, T] TODO implement stream in LMDB service
}

object LMDB {
  def platformCheck(): RIO[LMDB, Unit] = ZIO.serviceWithZIO(_.platformCheck())

  def databases(): RIO[LMDB, List[String]]               = ZIO.serviceWithZIO(_.databases())
  def databaseExists(dbName: String): RIO[LMDB, Boolean] = ZIO.serviceWithZIO(_.databaseExists(dbName))
  def databaseCreate(dbName: String): RIO[LMDB, Unit]    = ZIO.serviceWithZIO(_.databaseCreate(dbName))
  def databaseClear(dbName: String): RIO[LMDB, Unit]     = ZIO.serviceWithZIO(_.databaseClear(dbName))

  def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): RIO[LMDB, Option[T]]                                    = ZIO.serviceWithZIO(_.fetch(dbName, id))
  def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T]): RIO[LMDB, T]                     = ZIO.serviceWithZIO(_.upsertOverwrite(dbName, id, document))
  def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): RIO[LMDB, T] = ZIO.serviceWithZIO(_.upsert(dbName, id, modifier))
  def delete(dbName: String, id: String): RIO[LMDB, Boolean]                                                              = ZIO.serviceWithZIO(_.delete(dbName, id))

  def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): RIO[LMDB, List[T]] =
    ZIO.serviceWithZIO(_.collect(dbName, keyFilter, valueFilter))

  // def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[LMDB & Scope, Throwable, T] =
  //  ZStream.serviceWithZIO(_.stream(dbName, keyFilter))

  val live: ZLayer[Scope & LMDBConfig, Any, LMDB] = ZLayer(ZIO.service[LMDBConfig].flatMap(LMDBOperations.setup)).orDie
}
