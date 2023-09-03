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
import zio.stm._
import zio.json._
import zio.stream._

import java.io.File
import org.lmdbjava.{Cursor, Dbi, DbiFlags, Env, EnvFlags, KeyRange, Txn, Verifier}
import org.lmdbjava.SeekOp._
import org.lmdbjava.CursorIterable.KeyVal

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import zio.lmdb.StorageUserError._
import zio.lmdb.StorageSystemError._

/** LMDB ZIO abstraction layer, provides standard atomic operations implementations
  * @param env
  * @param openedCollectionDbisRef
  * @param reentrantLock
  */
class LMDBLive(
  env: Env[ByteBuffer],
  openedCollectionDbisRef: Ref[Map[String, Dbi[ByteBuffer]]],
  reentrantLock: TReentrantLock,
  val databasePath: String
) extends LMDB {
  val charset = StandardCharsets.UTF_8

  private def makeKeyByteBuffer(id: String): IO[KeyErrors, ByteBuffer] = {
    val keyBytes = id.getBytes(charset)
    if (keyBytes.length > env.getMaxKeySize) ZIO.fail(OverSizedKey(id, keyBytes.length, env.getMaxKeySize))
    else
      for {
        key <- ZIO.attempt(ByteBuffer.allocateDirect(env.getMaxKeySize)).mapError(err => InternalError("Couldn't allocate byte buffer for key", Some(err)))
        _   <- ZIO.attempt(key.put(keyBytes).flip).mapError(err => InternalError("Couldn't copy key bytes to buffer", Some(err)))
      } yield key
  }

  private def getCollectionDbi(name: CollectionName): IO[CollectionNotFound, Dbi[ByteBuffer]] = {
    val alreadyHereLogic = for {
      openedCollectionDbis <- openedCollectionDbisRef.get
    } yield openedCollectionDbis.get(name)

    val openAndRememberLogic = for {
      openedCollectionDbis <- reentrantLock.withWriteLock( // See https://github.com/lmdbjava/lmdbjava/issues/195
                                openedCollectionDbisRef.updateAndGet(before =>
                                  if (before.contains(name)) before
                                  else before + (name -> env.openDbi(name))
                                )
                              )
    } yield openedCollectionDbis.get(name)

    alreadyHereLogic.some
      .orElse(openAndRememberLogic.some)
      .mapError(err => CollectionNotFound(name))
  }

  override def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean] = {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      found                <- if (openedCollectionDbis.contains(name)) ZIO.succeed(true)
                              else collectionsAvailable().map(_.contains(name))
    } yield found
  }

  override def collectionGet[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[GetErrors, LMDBCollection[T]] = {
    for {
      exists     <- collectionExists(name)
      collection <- ZIO.cond[CollectionNotFound, LMDBCollection[T]](exists, LMDBCollection[T](name, this), CollectionNotFound(name))
    } yield collection
  }

  override def collectionSize(name: CollectionName): IO[SizeErrors, Long] = {
    for {
      collectionDbi <- getCollectionDbi(name)
      stats         <- withReadTransaction(name) { txn =>
                         ZIO
                           .attempt(collectionDbi.stat(txn))
                           .mapError(err => InternalError(s"Couldn't get $name size", Some(err)))
                       }
    } yield stats.entries
  }

  override def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit] = {
    for {
      exists <- collectionExists(name)
      _      <- ZIO.cond[CollectionAlreadExists, Unit](!exists, (), CollectionAlreadExists(name))
      _      <- collectionCreateLogic(name)
    } yield ()
  }

  override def collectionCreate[T](name: CollectionName)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CreateErrors, LMDBCollection[T]] = {
    collectionAllocate(name) *> ZIO.succeed(LMDBCollection[T](name, this))
  }

  private def collectionCreateLogic(name: CollectionName): ZIO[Any, StorageSystemError, Unit] = reentrantLock.withWriteLock {
    for {
      openedCollectionDbis <- reentrantLock.withWriteLock( // See https://github.com/lmdbjava/lmdbjava/issues/195
                                openedCollectionDbisRef.updateAndGet(before =>
                                  if (before.contains(name)) before
                                  else before + (name -> env.openDbi(name, DbiFlags.MDB_CREATE)) // TODO
                                )
                              )
      collectionDbi        <- ZIO
                                .from(openedCollectionDbis.get(name))
                                .mapError(err => InternalError(s"Couldn't create DB $name"))
    } yield ()
  }

  private def withWriteTransaction(colName: CollectionName): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO
        .attemptBlocking(env.txnWrite())
        .mapError(err => InternalError(s"Couldn't acquire write transaction on $colName", Some(err)))
    )(txn =>
      ZIO
        .attemptBlocking(txn.close())
        .ignoreLogged
    )

  private def withReadTransaction(colName: CollectionName): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO
        .attemptBlocking(env.txnRead())
        .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
    )(txn =>
      ZIO
        .attemptBlocking(txn.close())
        .ignoreLogged
    )

  override def collectionClear(colName: CollectionName): IO[ClearErrors, Unit] = {
    def collectionClearLogic(colDbi: Dbi[ByteBuffer]): ZIO[Any, ClearErrors, Unit] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            _ <- ZIO
                   .attemptBlocking(colDbi.drop(txn))
                   .mapError(err => InternalError(s"Couldn't clear $colName", Some(err)))
            _ <- ZIO
                   .attemptBlocking(txn.commit())
                   .mapError[ClearErrors](err => InternalError("Couldn't commit transaction", Some(err)))
          } yield ()
        }
      )
    }
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- collectionClearLogic(collectionDbi)
    } yield ()
  }

  /** @inheritdoc */
  override def platformCheck(): IO[StorageSystemError, Unit] = reentrantLock.withWriteLock {
    ZIO
      .attemptBlockingIO(new Verifier(env).runFor(5, TimeUnit.SECONDS))
      .mapError(err => InternalError(err.getMessage, Some(err)))
      .unit
  }

  override def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]] = {
    reentrantLock.withWriteLock( // See https://github.com/lmdbjava/lmdbjava/issues/195
      for {
        collectionNames <- ZIO
                             .attempt(
                               env
                                 .getDbiNames()
                                 .asScala
                                 .map(bytes => new String(bytes))
                                 .toList
                             )
                             .mapError(err => InternalError("Couldn't list collections", Some(err)))
      } yield collectionNames
    )
  }

  override def delete[T](colName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[DeleteErrors, Option[T]] = {
    def deleteLogic(colDbi: Dbi[ByteBuffer]): IO[DeleteErrors, Option[T]] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key           <- makeKeyByteBuffer(key)
            found         <- ZIO.attemptBlocking(Option(colDbi.get(txn, key))).mapError[DeleteErrors](err => InternalError(s"Couldn't fetch $key for delete on $colName", Some(err)))
            mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            mayBeDoc      <- ZIO.foreach(mayBeRawValue) { rawValue =>
                               ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[DeleteErrors](msg => JsonFailure(msg))
                             }
            keyFound      <- ZIO.attemptBlocking(colDbi.delete(txn, key)).mapError[DeleteErrors](err => InternalError(s"Couldn't delete $key from $colName", Some(err)))
            _             <- ZIO.attemptBlocking(txn.commit()).mapError[DeleteErrors](err => InternalError("Couldn't commit transaction", Some(err)))
          } yield mayBeDoc
        }
      )
    }
    for {
      db     <- getCollectionDbi(colName)
      status <- deleteLogic(db)
    } yield status
  }

  override def fetch[T](colName: CollectionName, key: RecordKey)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[FetchErrors, Option[T]] = {
    def fetchLogic(colDbi: Dbi[ByteBuffer]): ZIO[Any, FetchErrors, Option[T]] = {
      withReadTransaction(colName) { txn =>
        for {
          key           <- makeKeyByteBuffer(key)
          found         <- ZIO.attemptBlocking(Option(colDbi.get(txn, key))).mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName", Some(err)))
          mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
          document      <- ZIO
                             .foreach(mayBeRawValue) { rawValue =>
                               ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[FetchErrors](msg => JsonFailure(msg))
                             }
        } yield document
      }
    }
    for {
      db     <- getCollectionDbi(colName)
      result <- fetchLogic(db)
    } yield result
  }

  override def contains(colName: CollectionName, key: RecordKey): IO[ContainsErrors, Boolean] = {
    def containsLogic(colDbi: Dbi[ByteBuffer]): ZIO[Any, ContainsErrors, Boolean] = {
      withReadTransaction(colName) { txn =>
        for {
          key   <- makeKeyByteBuffer(key)
          found <- ZIO.attemptBlocking(Option(colDbi.get(txn, key))).mapError[ContainsErrors](err => InternalError(s"Couldn't check $key on $colName", Some(err)))
        } yield found.isDefined
      }
    }
    for {
      db     <- getCollectionDbi(colName)
      result <- containsLogic(db)
    } yield result
  }

  override def upsertOverwrite[T](colName: CollectionName, key: RecordKey, document: T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, Unit] = {
    def upsertLogic(collectionDbi: Dbi[ByteBuffer]): IO[UpsertErrors, Unit] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key           <- makeKeyByteBuffer(key)
            found         <- ZIO.attemptBlocking(Option(collectionDbi.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $key for upsertOverwrite on $colName", Some(err)))
            mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            jsonDocBytes   = document.toJson.getBytes(charset)
            valueBuffer   <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(jsonDocBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for json value", Some(err)))
            _             <- ZIO.attemptBlocking(valueBuffer.put(jsonDocBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
            _             <- ZIO.attemptBlocking(collectionDbi.put(txn, key, valueBuffer)).mapError(err => InternalError(s"Couldn't upsertOverwrite $key into $colName", Some(err)))
            _             <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError(s"Couldn't commit upsertOverwrite $key into $colName", Some(err)))
          } yield ()
        }
      )
    }
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- upsertLogic(collectionDbi)
    } yield result
  }

  override def upsert[T](colName: CollectionName, key: RecordKey, modifier: Option[T] => T)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[UpsertErrors, Unit] = {
    def upsertLogic(collectionDbi: Dbi[ByteBuffer]): IO[UpsertErrors, Unit] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key            <- makeKeyByteBuffer(key)
            found          <- ZIO.attemptBlocking(Option(collectionDbi.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $key for upsert on $colName", Some(err)))
            mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                                ZIO.fromEither(charset.decode(rawValue).fromJson[T]).mapError[UpsertErrors](msg => JsonFailure(msg))
                              }
            docAfter        = modifier(mayBeDocBefore)
            jsonDocBytes    = docAfter.toJson.getBytes(charset)
            valueBuffer    <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(jsonDocBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for json value", Some(err)))
            _              <- ZIO.attemptBlocking(valueBuffer.put(jsonDocBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
            _              <- ZIO.attemptBlocking(collectionDbi.put(txn, key, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $key into $colName", Some(err)))
            _              <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError(s"Couldn't commit upsert $key into $colName", Some(err)))
          } yield ()
        }
      )
    }
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- upsertLogic(collectionDbi)
    } yield result
  }

  override def collect[T](colName: CollectionName, keyFilter: RecordKey => Boolean = _ => true, valueFilter: T => Boolean = (_: T) => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): IO[CollectErrors, List[T]] = {
    def collectLogic(collectionDbi: Dbi[ByteBuffer]): ZIO[Scope, CollectErrors, List[T]] = for {
      txn       <- ZIO.acquireRelease(
                     ZIO
                       .attemptBlocking(env.txnRead())
                       .mapError[CollectErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                   )(txn =>
                     ZIO
                       .attemptBlocking(txn.close())
                       .ignoreLogged
                   )
      iterable  <- ZIO.acquireRelease(
                     ZIO
                       .attemptBlocking(collectionDbi.iterate(txn, KeyRange.all()))
                       .mapError[CollectErrors](err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                   )(cursor =>
                     ZIO
                       .attemptBlocking(cursor.close())
                       .ignoreLogged
                   )
      collected <- ZIO
                     .attempt {
                       Chunk
                         .fromIterator(EncapsulatedIterator(iterable.iterator()))
                         .filter { case (key, value) => keyFilter(key) }
                         .flatMap { case (key, value) => value.fromJson[T].toOption } // TODO error are hidden !!!
                         .filter(valueFilter)
                         .toList
                     }
                     .mapError[CollectErrors](err => InternalError(s"Couldn't collect documents stored in $colName", Some(err)))
    } yield collected

    for {
      collectionDbi <- getCollectionDbi(colName)
      collected     <- ZIO.scoped(collectLogic(collectionDbi))
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

  def stream[T](colName: CollectionName, keyFilter: RecordKey => Boolean = _ => true)(implicit je: JsonEncoder[T], jd: JsonDecoder[T]): ZStream[Any, StreamErrors, T] = {
    def streamLogic(colDbi: Dbi[ByteBuffer]): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, T]] = for {
      txn      <- ZIO.acquireRelease(
                    ZIO
                      .attemptBlocking(env.txnRead())
                      .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                  )(txn =>
                    ZIO
                      .attemptBlocking(txn.close())
                      .ignoreLogged
                  )
      iterable <- ZIO.acquireRelease(
                    ZIO
                      .attemptBlocking(colDbi.iterate(txn, KeyRange.all()))
                      .mapError(err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                  )(cursor =>
                    ZIO
                      .attemptBlocking(cursor.close())
                      .ignoreLogged
                  )
    } yield ZStream
      .fromIterator(EncapsulatedIterator(iterable.iterator()))
      .filter { case (key, value) => keyFilter(key) }
      .mapZIO { case (key, value) => ZIO.from(value.fromJson[T]).mapError(err => JsonFailure(err)) }
      .mapError {
        case err: Throwable   => InternalError(s"Couldn't stream from $colName", Some(err))
        case err: JsonFailure => err
      }

    val result =
      for {
        db     <- getCollectionDbi(colName)
        _      <- reentrantLock.readLock
        stream <- streamLogic(db)
      } yield stream

    ZStream.unwrapScoped(result) // TODO not sure this is the good way ???
  }

}

object LMDBLive {

  private def lmdbCreateEnv(config: LMDBConfig, databasePath: File) = {
    val syncFlag = if (!config.fileSystemSynchronized) Some(EnvFlags.MDB_NOSYNC) else None

    val flags = Array(
      EnvFlags.MDB_NOTLS,
      // MDB_NOLOCK : the caller must enforce single-writer semantics
      // MDB_NOLOCK : the caller must ensure that no readers are using old transactions while a writer is active
      EnvFlags.MDB_NOLOCK // Locks managed using ZIO ReentrantLock
    ) ++ syncFlag

    Env
      .create()
      .setMapSize(config.mapSize.toLong)
      .setMaxDbs(config.maxCollections)
      .setMaxReaders(config.maxReaders)
      .open(
        databasePath,
        flags: _*
      )
  }

  def setup(config: LMDBConfig): ZIO[Scope, Throwable, LMDBLive] = {
    for {
      databasesHome        <- ZIO
                                .from(config.databasesHome)
                                .orElse(System.envOrElse("HOME", ".").map(home => home + File.separator + ".lmdb"))
      databasePath          = new File(databasesHome, config.databaseName)
      _                    <- ZIO.logInfo(s"LMDB databasePath=$databasePath")
      _                    <- ZIO.attemptBlockingIO(databasePath.mkdirs())
      environment          <- ZIO.acquireRelease(
                                ZIO.attemptBlocking(lmdbCreateEnv(config, databasePath))
                              )(env => ZIO.attemptBlocking(env.close).ignoreLogged)
      openedCollectionDbis <- Ref.make[Map[String, Dbi[ByteBuffer]]](Map.empty)
      reentrantLock        <- TReentrantLock.make.commit
    } yield new LMDBLive(environment, openedCollectionDbis, reentrantLock, databasePath.toString)
  }
}
