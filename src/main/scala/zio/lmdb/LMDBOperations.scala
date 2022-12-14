/*
 * Copyright 2022 David Crosson
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
import zio.stm.*
import zio.json.*
import zio.stream.*

import java.io.File
import org.lmdbjava.{Cursor, Dbi, DbiFlags, Env, EnvFlags, KeyRange, Txn, Verifier}
import org.lmdbjava.SeekOp.*
import org.lmdbjava.CursorIterable.KeyVal

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import zio.lmdb.StorageUserError.*
import zio.lmdb.StorageSystemError.*

/** LMDB ZIO abstraction layer, provides standard atomic operations implementations
  * @param env
  * @param openedDatabasesRef
  * @param reentrantLock
  */
class LMDBOperations(
  env: Env[ByteBuffer],
  openedDatabasesRef: Ref[Map[String, Dbi[ByteBuffer]]],
  reentrantLock: TReentrantLock
) extends LMDB {
  val charset = StandardCharsets.UTF_8

  private def makeKeyByteBuffer(id: String): IO[OverSizedKey | StorageSystemError, ByteBuffer] = {
    val keyBytes = id.getBytes(charset)
    if (keyBytes.length > env.getMaxKeySize) ZIO.fail(OverSizedKey(id, keyBytes.length, env.getMaxKeySize))
    else
      for {
        key <- ZIO.attempt(ByteBuffer.allocateDirect(env.getMaxKeySize)).mapError(err => InternalError(s"Couldn't allocate byte buffer for key", Some(err)))
        _   <- ZIO.attempt(key.put(keyBytes).flip).mapError(err => InternalError(s"Couldn't copy key bytes to buffer", Some(err)))
      } yield key
  }

  /** return an existing database
    * @param dbName
    * @return
    */
  private def getDatabase(dbName: String): IO[DatabaseNotFound, Dbi[ByteBuffer]] = {
    val alreadyHereLogic = for {
      databases <- openedDatabasesRef.get
    } yield databases.get(dbName)

    val addAndGetLogic = for {
      databases <- openedDatabasesRef.updateAndGet(before =>
                     if (before.contains(dbName)) before
                     else before + (dbName -> env.openDbi(dbName))
                   )
    } yield databases.get(dbName)

    alreadyHereLogic.some
      .orElse(addAndGetLogic.some)
      .mapError(err => DatabaseNotFound(dbName))
  }

  /** check if a database exists
    * @param dbName
    * @return
    */
  override def databaseExists(dbName: String): IO[StorageSystemError, Boolean] = {
    for {
      openedDatabases <- openedDatabasesRef.get
      found           <- if (openedDatabases.contains(dbName)) ZIO.succeed(true)
                         else databases().map(_.contains(dbName))
    } yield found
  }

  /** create the database (or does nothing if it already exists)
    * @param dbName
    * @return
    */
  override def databaseCreate(dbName: String): IO[StorageSystemError, Unit] = {
    databaseExists(dbName)
      .filterOrElse(identity)(
        databaseCreateLogic(dbName)
      )
      .unit
  }

  private def databaseCreateLogic(dbName: String) = reentrantLock.withWriteLock {
    for {
      databases <- openedDatabasesRef.updateAndGet(before =>
                     if (before.contains(dbName)) before
                     else before + (dbName -> env.openDbi(dbName, DbiFlags.MDB_CREATE)) // TODO
                   )
      db        <- ZIO
                     .from(databases.get(dbName))
                     .mapError(err => InternalError(s"Couldn't create DB $dbName"))
    } yield ()
  }

  private def withWriteTransaction(dbName: String): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO
        .attemptBlocking(env.txnWrite())
        .mapError(err => InternalError(s"Couldn't acquire write transaction on $dbName", Some(err)))
    )(txn =>
      ZIO
        .attemptBlocking(txn.close())
        .ignoreLogged
    )

  private def withReadTransaction(dbName: String) =
    ZIO.acquireReleaseWith(
      ZIO
        .attemptBlocking(env.txnRead())
        .mapError(err => InternalError(s"Couldn't acquire read transaction on $dbName", Some(err)))
    )(txn =>
      ZIO
        .attemptBlocking(txn.close())
        .ignoreLogged
    )

  /** Remove all the content of a database
    * @param dbName
    * @return
    */
  override def databaseClear(dbName: String): IO[DatabaseNotFound | StorageSystemError, Unit] = {
    def databaseClearLogic(db: Dbi[ByteBuffer]): ZIO[Any, StorageSystemError, Unit] = {
      withWriteTransaction(dbName) { txn =>
        ZIO
          .attemptBlocking(db.drop(txn))
          .mapError(err => InternalError(s"Couldn't clear $dbName", Some(err)))
      }
    }
    reentrantLock.withWriteLock(
      for {
        db <- getDatabase(dbName)
        _  <- databaseClearLogic(db)
      } yield ()
    )
  }

  /** Check server current configuration compatibility
    */
  override def platformCheck(): IO[StorageSystemError, Unit] = reentrantLock.withWriteLock {
    ZIO
      .attemptBlockingIO(new Verifier(env).runFor(5, TimeUnit.SECONDS))
      .mapError(err => InternalError(err.getMessage, Some(err)))
      .unit
  }

  /** list databases
    */
  override def databases(): IO[StorageSystemError, List[String]] = {
    reentrantLock.withWriteLock(
      for {
        databases <- ZIO
                       .attempt(
                         env
                           .getDbiNames()
                           .asScala
                           .map(bytes => new String(bytes))
                           .toList
                       )
                       .mapError(err => InternalError("Couldn't list databases", Some(err)))
      } yield databases
    )
  }

  /** delete record
    * @param id
    * @return
    */
  override def delete[T](dbName: String, id: String)(using JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]] = {
    type DeleteLogicErrorsType = OverSizedKey | StorageSystemError | JsonFailure
    def deleteLogic(db: Dbi[ByteBuffer]): IO[DeleteLogicErrorsType, Option[T]] = {
      withWriteTransaction(dbName) { txn =>
        for {
          key           <- makeKeyByteBuffer(id)
          found         <- ZIO.attemptBlocking(Option(db.get(txn, key))).mapError[DeleteLogicErrorsType](err => InternalError(s"Couldn't fetch $id for delete on $dbName", Some(err)))
          mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
          mayBeDoc      <- ZIO.foreach(mayBeRawValue) { rawValue =>
                             ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[DeleteLogicErrorsType](msg => JsonFailure(msg))
                           }
          keyFound      <- ZIO.attemptBlocking(db.delete(txn, key)).mapError[DeleteLogicErrorsType](err => InternalError(s"Couldn't delete $id from $dbName", Some(err)))
          _             <- ZIO.attemptBlocking(txn.commit()).mapError[DeleteLogicErrorsType](err => InternalError(s"Couldn't commit transaction", Some(err)))
        } yield mayBeDoc
      }
    }
    reentrantLock.withWriteLock(
      for {
        db     <- getDatabase(dbName)
        status <- deleteLogic(db)
      } yield status
    )
  }

  /** fetch a record
    * @param id
    * @return
    */
  override def fetch[T](dbName: String, id: String)(using JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, Option[T]] = {
    type FetchLogicErrorTypes = OverSizedKey | JsonFailure | StorageSystemError
    def fetchLogic(db: Dbi[ByteBuffer]): ZIO[Any, FetchLogicErrorTypes, Option[T]] = {
      withReadTransaction(dbName) { txn =>
        for {
          key           <- makeKeyByteBuffer(id)
          found         <- ZIO.attemptBlocking(Option(db.get(txn, key))).mapError[FetchLogicErrorTypes](err => InternalError(s"Couldn't fetch $id on $dbName", Some(err)))
          mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
          document      <- ZIO
                             .foreach(mayBeRawValue) { rawValue =>
                               ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[FetchLogicErrorTypes](msg => JsonFailure(msg))
                             }
        } yield document
      }
    }
    for {
      db     <- reentrantLock.withWriteLock(getDatabase(dbName))
      result <- reentrantLock.withReadLock(fetchLogic(db))
    } yield result
  }

  /** overwrite or insert a document
    * @param id
    * @param document
    * @tparam T
    * @return
    */
  override def upsertOverwrite[T](dbName: String, id: String, document: T)(using JsonEncoder[T], JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]] = {
    upsert(dbName, id, _ => document)
  }

  /** atomic document update/insert throw a lambda
    * @param id
    * @param modifier
    * @return
    */
  override def upsert[T](dbName: String, id: String, modifier: Option[T] => T)(using JsonEncoder[T], JsonDecoder[T]): IO[OverSizedKey | DatabaseNotFound | JsonFailure | StorageSystemError, UpsertState[T]] = {
    type UpsertLogicErrorTypes = OverSizedKey | JsonFailure | StorageSystemError
    def upsertLogic(db: Dbi[ByteBuffer]): IO[UpsertLogicErrorTypes, UpsertState[T]] = {
      withWriteTransaction(dbName) { txn =>
        for {
          key            <- makeKeyByteBuffer(id)
          found          <- ZIO.attemptBlocking(Option(db.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $id for upsert on $dbName", Some(err)))
          mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
          mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                              ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[UpsertLogicErrorTypes](msg => JsonFailure(msg))
                            }
          docAfter        = modifier(mayBeDocBefore)
          jsonDocBytes    = docAfter.toJson.getBytes(charset)
          valueBuffer    <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(jsonDocBytes.size)).mapError(err => InternalError(s"Couldn't allocate byte buffer for json value", Some(err)))
          _              <- ZIO.attemptBlocking(valueBuffer.put(jsonDocBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer", Some(err)))
          _              <- ZIO.attemptBlocking(db.put(txn, key, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $id into $dbName", Some(err)))
          _              <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError(s"Couldn't commit upsertOverwrite $id into $dbName", Some(err)))
        } yield UpsertState(previous = mayBeDocBefore, current = docAfter)
      }
    }
    reentrantLock.withWriteLock(
      for {
        db     <- getDatabase(dbName)
        result <- upsertLogic(db)
      } yield result
    )
  }

  /** Dangerous collect method as it loads everything in memory, use keyFilter or valueFilter to limit loaded entries. Use stream method instead
    * @return
    */
  override def collect[T](dbName: String, keyFilter: String => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(using JsonDecoder[T]): IO[DatabaseNotFound | JsonFailure | InternalError, List[T]] = {
    type CollectLogicErrorTypes = JsonFailure | InternalError
    def collectLogic(db: Dbi[ByteBuffer]): ZIO[Scope, CollectLogicErrorTypes, List[T]] = for {
      txn       <- ZIO.acquireRelease(
                     ZIO
                       .attemptBlocking(env.txnRead())
                       .mapError[CollectLogicErrorTypes](err => InternalError(s"Couldn't acquire read transaction on $dbName", Some(err)))
                   )(txn =>
                     ZIO
                       .attemptBlocking(txn.close())
                       .ignoreLogged
                   )
      iterable  <- ZIO.acquireRelease(
                     ZIO
                       .attemptBlocking(db.iterate(txn, KeyRange.all()))
                       .mapError[CollectLogicErrorTypes](err => InternalError(s"Couldn't acquire iterable on $dbName", Some(err)))
                   )(cursor =>
                     ZIO
                       .attemptBlocking(cursor.close())
                       .ignoreLogged
                   )
      collected <- ZIO
                     .attempt {
                       Chunk
                         .fromIterator(EncapsulatedIterator(iterable.iterator()))
                         .filter((key, value) => keyFilter(key))
                         .flatMap((key, value) => value.fromJson[T].toOption) // TODO error are hidden !!!
                         .filter(valueFilter)
                         .toList
                     }
                     .mapError[CollectLogicErrorTypes](err => InternalError(s"Couldn't collect documents stored in $dbName", Some(err)))
    } yield collected

    for {
      db        <- reentrantLock.withWriteLock(getDatabase(dbName))
      collected <- reentrantLock.withReadLock(ZIO.scoped(collectLogic(db)))
    } yield collected
  }

  private def extractKeyVal(keyval: KeyVal[ByteBuffer]): (String, String) = {
    val key          = keyval.key()
    val value        = keyval.`val`()
    val decodedKey   = charset.decode(key).toString
    val decodedValue = charset.decode(value).toString
    decodedKey -> decodedValue
  }

  // Encapsulation mandatory in order to make the stream work fine, without the behavior is very stange and not yet understood
  case class EncapsulatedIterator(jiterator: java.util.Iterator[KeyVal[ByteBuffer]]) extends Iterator[(String, String)] {
    override def hasNext: Boolean = jiterator.hasNext()

    override def next(): (String, String) = {
      val (key, value) = extractKeyVal(jiterator.next())
      key -> value
    }
  }
  /*
  def stream[T](dbName: String, keyFilter: String => Boolean = _ => true)(using JsonDecoder[T]): ZStream[Scope, DatabaseNotFound | JsonFailure | InternalError, T] = {
    def streamLogic(db: Dbi[ByteBuffer]): ZIO[Scope, InternalError, ZStream[Any, JsonFailure | InternalError, T]] = for {
      txn      <- ZIO.acquireRelease(
                    ZIO
                      .attemptBlocking(env.txnRead())
                      .mapError(err => InternalError(s"Couldn't acquire read transaction on $dbName", Some(err)))
                  )(txn =>
                    ZIO
                      .attemptBlocking(txn.close())
                      .ignoreLogged
                  )
      iterable <- ZIO.acquireRelease(
                    ZIO
                      .attemptBlocking(db.iterate(txn, KeyRange.all()))
                      .mapError(err => InternalError(s"Couldn't acquire iterable on $dbName", Some(err)))
                  )(cursor =>
                    ZIO
                      .attemptBlocking(cursor.close())
                      .ignoreLogged
                  )
    } yield ZStream
      .fromIterator(EncapsulatedIterator(iterable.iterator()))
      .filter((key, value) => keyFilter(key))
      .mapZIO((key, value) => ZIO.from(value.fromJson[T]).mapError(err => JsonFailure(err)))
      .mapError[JsonFailure | InternalError] {
        case err: Throwable   => InternalError(s"Couldn't stream from $dbName", Some(err))
        case err: JsonFailure => err
      }

    val result =
        for {
          db     <- reentrantLock.withWriteLock(getDatabase(dbName))
          _      <- reentrantLock.readLock
          stream <- streamLogic(db)
        } yield stream

    ZStream.unwrap(result)
  }
   */

}

object LMDBOperations {

  private def lmdbCreateEnv(config: LMDBConfig) =
    Env
      .create()
      .setMapSize(config.mapSize)
      .setMaxDbs(config.maxDbs)
      .setMaxReaders(config.maxReaders)
      .open(
        config.databasesPath,
        EnvFlags.MDB_NOTLS,
        // MDB_NOLOCK : the caller must enforce single-writer semantics
        // MDB_NOLOCK : the caller must ensure that no readers are using old transactions while a writer is active
        EnvFlags.MDB_NOLOCK, // Locks managed using ZIO ReentrantLock
        EnvFlags.MDB_NOSYNC  // Acceptable, in particular because EXT4 is used
      )

  def setup(config: LMDBConfig): ZIO[Scope, Throwable, LMDBOperations] = {
    for {
      environment     <- ZIO.acquireRelease(
                           ZIO.attemptBlocking(lmdbCreateEnv(config))
                         )(env => ZIO.attemptBlocking(env.close).ignoreLogged)
      openedDatabases <- Ref.make[Map[String, Dbi[ByteBuffer]]](Map.empty)
      reentrantLock   <- TReentrantLock.make.commit
    } yield new LMDBOperations(environment, openedDatabases, reentrantLock)
  }
}
