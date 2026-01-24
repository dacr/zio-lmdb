/*
 * Copyright 2025 David Crosson
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
import zio.stream._

import java.io.File
import org.lmdbjava.{Cursor, Dbi, DbiFlags, Env, EnvFlags, KeyRange, Txn, Verifier}
import org.lmdbjava.SeekOp._
import org.lmdbjava.CursorIterable.KeyVal
import org.lmdbjava.GetOp
import org.lmdbjava.SeekOp

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

  private def makeKeyByteBuffer[K](id: K)(implicit kodec: LMDBKodec[K]): IO[KeyErrors, ByteBuffer] = {
    val keyBytes: Array[Byte] = kodec.encode(id)
    if (keyBytes.length > env.getMaxKeySize) ZIO.fail(OverSizedKey(id.toString, keyBytes.length, env.getMaxKeySize)) // TODO id.toString probably not the best choice
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

  override def collectionGet[K, T](name: CollectionName)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[GetErrors, LMDBCollection[K, T]] = {
    for {
      exists     <- collectionExists(name)
      collection <- ZIO.cond[CollectionNotFound, LMDBCollection[K, T]](exists, LMDBCollection[K, T](name, this), CollectionNotFound(name))
    } yield collection
  }

  override def collectionSize(name: CollectionName): IO[SizeErrors, Long] = {
    for {
      collectionDbi <- getCollectionDbi(name)
      stats         <- reentrantLock.withReadLock(
                         withReadTransaction(name) { txn =>
                           ZIO
                             .attempt(collectionDbi.stat(txn))
                             .mapError(err => InternalError(s"Couldn't get $name size", Some(err)))
                         }
                       )
    } yield stats.entries
  }

  override def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit] = {
    for {
      exists <- collectionExists(name)
      _      <- ZIO.cond[CollectionAlreadExists, Unit](!exists, (), CollectionAlreadExists(name))
      _      <- collectionCreateLogic(name)
    } yield ()
  }

  override def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[CreateErrors, LMDBCollection[K, T]] = {
    val allocateLogic = if (failIfExists) collectionAllocate(name) else collectionAllocate(name).ignore
    allocateLogic.as(LMDBCollection[K, T](name, this))
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

  private def collectionClearOrDropLogic(colDbi: Dbi[ByteBuffer], collectionName: CollectionName, dropDatabase: Boolean): ZIO[Any, ClearErrors, Unit] = {
    reentrantLock.withWriteLock(
      withWriteTransaction(collectionName) { txn =>
        for {
          _ <- ZIO
                 .attemptBlocking(colDbi.drop(txn, dropDatabase))
                 .mapError(err => InternalError(s"Couldn't ${if (dropDatabase) "drop" else "clear"} $collectionName", Some(err)))
          _ <- ZIO
                 .attemptBlocking(txn.commit())
                 .mapError[ClearErrors](err => InternalError("Couldn't commit transaction", Some(err)))
        } yield ()
      }
    )
  }

  override def collectionClear(colName: CollectionName): IO[ClearErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- collectionClearOrDropLogic(collectionDbi, colName, false)
    } yield ()
  }

  override def collectionDrop(colName: CollectionName): IO[DropErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- collectionClearOrDropLogic(collectionDbi, colName, true)
      _             <- openedCollectionDbisRef.updateAndGet(_.removed(colName))
      _             <- reentrantLock.withWriteLock(
                         ZIO
                           .attemptBlocking(collectionDbi.close()) // TODO check close documentation more precisely at it states : It is very rare that closing a database handle is useful.
                           .mapError[DropErrors](err => InternalError("Couldn't close collection internal handler", Some(err)))
                       )
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

  override def delete[K, T](colName: CollectionName, key: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
    def deleteLogic(colDbi: Dbi[ByteBuffer]): IO[DeleteErrors, Option[T]] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key           <- makeKeyByteBuffer(key)
            found         <- ZIO.attemptBlocking(Option(colDbi.get(txn, key))).mapError[DeleteErrors](err => InternalError(s"Couldn't fetch $key for delete on $colName", Some(err)))
            mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            mayBeDoc      <- ZIO.foreach(mayBeRawValue) { rawValue =>
                               ZIO.fromEither(codec.decode(rawValue)).mapError[DeleteErrors](msg => CodecFailure(msg))
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

  override def fetch[K, T](colName: CollectionName, key: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]] = {
    def fetchLogic(colDbi: Dbi[ByteBuffer]): ZIO[Any, FetchErrors, Option[T]] = {
      withReadTransaction(colName) { txn =>
        for {
          key           <- makeKeyByteBuffer(key)
          found         <- ZIO.attemptBlocking(Option(colDbi.get(txn, key))).mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName", Some(err)))
          mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
          document      <- ZIO
                             .foreach(mayBeRawValue) { rawValue =>
                               ZIO.fromEither(codec.decode(rawValue)).mapError[FetchErrors](msg => CodecFailure(msg))
                             }
        } yield document
      }
    }

    for {
      db     <- getCollectionDbi(colName)
      result <- reentrantLock.withReadLock(fetchLogic(db))
    } yield result
  }

  override def fetchAt[K, T](colName: CollectionName, index: Long)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    def walkLogic(colDbi: Dbi[ByteBuffer]): ZIO[Scope, FetchErrors, Option[(K, T)]] = for {
      txn                <- ZIO.acquireRelease(
                              ZIO
                                .attemptBlocking(env.txnRead())
                                .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                            )(txn =>
                              ZIO
                                .attemptBlocking(txn.close())
                                .ignoreLogged
                            )
      cursor             <- ZIO.acquireRelease(
                              ZIO
                                .attemptBlocking(colDbi.openCursor(txn))
                                .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                            )(cursor =>
                              ZIO
                                .attemptBlocking(cursor.close())
                                .ignoreLogged
                            )
      seekFirstSuccess   <- ZIO
                              .attempt(cursor.seek(SeekOp.MDB_FIRST))
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName", Some(err)))
      seekAllNextSuccess <- ZIO
                              .attempt(cursor.seek(SeekOp.MDB_NEXT))
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName", Some(err)))
                              .repeatN(index.toInt) // TODO review and optimize (support long, start from first or from last
                              .when(seekFirstSuccess)
      seeksSuccess        = seekAllNextSuccess.contains(true) || (index == 0 && seekFirstSuccess)
      seekedKey          <- ZIO
                              .fromEither(kodec.decode(cursor.key()))
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $colName", None))
      valBuffer          <- ZIO
                              .attempt(cursor.`val`())
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $colName", Some(err)))
      seekedValue        <- ZIO
                              .foreach(valBuffer) { rawValue =>
                                ZIO
                                  .fromEither(codec.decode(rawValue))
                                  .mapError[FetchErrors](msg => CodecFailure(msg))
                              }
                              .when(seeksSuccess)
                              .map(_.flatten)

    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))
    for {
      db     <- getCollectionDbi(colName)
      result <- reentrantLock.withReadLock(ZIO.scoped(walkLogic(db)))
    } yield result
  }

  private def seek[K, T](colName: CollectionName, recordKey: Option[K], seekOperation: SeekOp)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    // TODO TOO COMPLEX !!!!
    def seekLogic(colDbi: Dbi[ByteBuffer]): ZIO[Scope, FetchErrors, Option[(K, T)]] = for {
      txn         <- ZIO.acquireRelease(
                       ZIO
                         .attemptBlocking(env.txnRead())
                         .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                     )(txn =>
                       ZIO
                         .attemptBlocking(txn.close())
                         .ignoreLogged
                     )
      cursor      <- ZIO.acquireRelease(
                       ZIO
                         .attemptBlocking(colDbi.openCursor(txn))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                     )(cursor =>
                       ZIO
                         .attemptBlocking(cursor.close())
                         .ignoreLogged
                     )
      key         <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk))
      _           <- ZIO.foreachDiscard(key) { k =>
                       ZIO
                         .attempt(cursor.get(k, GetOp.MDB_SET))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't set cursor at $recordKey for $colName", Some(err)))
                     }
      seekSuccess <- ZIO
                       .attempt(cursor.seek(seekOperation))
                       .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName", Some(err)))
      seekedKey   <- ZIO
                       .fromEither(kodec.decode(cursor.key()))
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $colName", None))
      valBuffer   <- ZIO
                       .attempt(cursor.`val`())
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $colName", Some(err)))
      seekedValue <- ZIO
                       .foreach(valBuffer) { rawValue =>
                         ZIO
                           .fromEither(codec.decode(rawValue))
                           .mapError[FetchErrors](msg => CodecFailure(msg))
                       }
                       .when(seekSuccess)
                       .map(_.flatten)
    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))

    for {
      db     <- getCollectionDbi(colName)
      result <- reentrantLock.withReadLock(ZIO.scoped(seekLogic(db)))
    } yield result
  }

  override def head[K, T](collectionName: CollectionName)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, None, SeekOp.MDB_FIRST)
  }

  override def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, Some(beforeThatKey), SeekOp.MDB_PREV)
  }

  override def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, Some(afterThatKey), SeekOp.MDB_NEXT)
  }

  override def last[K, T](collectionName: CollectionName)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, None, SeekOp.MDB_LAST)
  }

  override def contains[K](colName: CollectionName, key: K)(implicit kodec: LMDBKodec[K]): IO[ContainsErrors, Boolean] = {
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
      result <- reentrantLock.withReadLock(containsLogic(db))
    } yield result
  }

  override def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]] = {
    def updateLogic(collectionDbi: Dbi[ByteBuffer]): IO[UpdateErrors, Option[T]] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(collectionName) { txn =>
          for {
            key            <- makeKeyByteBuffer(key)
            found          <- ZIO.attemptBlocking(Option(collectionDbi.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $key for upsert on $collectionName", Some(err)))
            mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                                ZIO.fromEither(codec.decode(rawValue)).mapError[UpdateErrors](msg => CodecFailure(msg))
                              }
            mayBeDocAfter   = mayBeDocBefore.map(modifier)
            _              <- ZIO.foreachDiscard(mayBeDocAfter) { docAfter =>
                                val docBytes = codec.encode(docAfter)
                                for {
                                  valueBuffer <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for encoded value", Some(err)))
                                  _           <- ZIO.attemptBlocking(valueBuffer.put(docBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
                                  _           <- ZIO.attemptBlocking(collectionDbi.put(txn, key, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $key into $collectionName", Some(err)))
                                  _           <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError(s"Couldn't commit upsert $key into $collectionName", Some(err)))
                                } yield ()
                              }
          } yield mayBeDocAfter
        }
      )
    }

    for {
      collectionDbi <- getCollectionDbi(collectionName)
      result        <- updateLogic(collectionDbi)
    } yield result
  }

  override def upsertOverwrite[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    def upsertLogic(collectionDbi: Dbi[ByteBuffer]): IO[UpsertErrors, Unit] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key           <- makeKeyByteBuffer(key)
            found         <- ZIO.attemptBlocking(Option(collectionDbi.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $key for upsertOverwrite on $colName", Some(err)))
            mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            docBytes       = codec.encode(document)
            valueBuffer   <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for encoded value", Some(err)))
            _             <- ZIO.attemptBlocking(valueBuffer.put(docBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
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

  override def upsert[K, T](colName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T] = {
    def upsertLogic(collectionDbi: Dbi[ByteBuffer]): IO[UpsertErrors, T] = {
      reentrantLock.withWriteLock(
        withWriteTransaction(colName) { txn =>
          for {
            key            <- makeKeyByteBuffer(key)
            found          <- ZIO.attemptBlocking(Option(collectionDbi.get(txn, key))).mapError(err => InternalError(s"Couldn't fetch $key for upsert on $colName", Some(err)))
            mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
            mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                                ZIO.fromEither(codec.decode(rawValue)).mapError[UpsertErrors](msg => CodecFailure(msg))
                              }
            docAfter        = modifier(mayBeDocBefore)
            docBytes        = codec.encode(docAfter)
            valueBuffer    <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for encoded value", Some(err)))
            _              <- ZIO.attemptBlocking(valueBuffer.put(docBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
            _              <- ZIO.attemptBlocking(collectionDbi.put(txn, key, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $key into $colName", Some(err)))
            _              <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError(s"Couldn't commit upsert $key into $colName", Some(err)))
          } yield docAfter
        }
      )
    }

    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- upsertLogic(collectionDbi)
    } yield result
  }

  private def makeRange(
    startAfter: Option[ByteBuffer] = None,
    backward: Boolean = false
  ): KeyRange[ByteBuffer] = {
    startAfter match {
      case None      =>
        if (backward) KeyRange.allBackward()
        else KeyRange.all()
      case Some(key) =>
        if (backward) KeyRange.greaterThanBackward(key)
        else KeyRange.greaterThan(key)
    }
  }

  override def collect[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): IO[CollectErrors, List[T]] = {
    def collectLogic(collectionDbi: Dbi[ByteBuffer]): ZIO[Scope, CollectErrors, List[T]] = for {
      txn          <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(env.txnRead())
                          .mapError[CollectErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                      )(txn =>
                        ZIO
                          .attemptBlocking(txn.close())
                          .ignoreLogged
                      )
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(collectionDbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError[CollectErrors](err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
      collected    <- ZIO
                        .foreach {
                          def content =
                            LazyList
                              .from(KeyValueIterator[K, T](iterable.iterator()))
                              .map(kv => kv.key.flatMap(key => kv.value.map(value => (key, value))))
                              .collect { case either if either.isLeft || either.exists((k, v) => keyFilter(k) && valueFilter(v)) => either.map((k, v) => v) }
                          limit match {
                            case None    => content.toList
                            case Some(l) => content.take(l).toList
                          }
                        } { r => ZIO.from(r) }
                        .mapError[CollectErrors](err => InternalError(s"Couldn't collect documents stored in $colName : $err", None))
    } yield collected

    for {
      collectionDbi <- getCollectionDbi(colName)
      collected     <- reentrantLock.withReadLock(ZIO.scoped(collectLogic(collectionDbi)))
    } yield collected
  }

//  class LazyKeyValue[T](keyGetter: => K, valueGetter: => Either[String, T]) {
//    private var decodedKey: K           = null // hidden optim to avoid memory pressure
//    private var decodedValue: Either[String, T] = null
//
//    def key: K = {
//      if (decodedKey == null) {
//        decodedKey = keyGetter
//      }
//      decodedKey
//    }
//
//    def value: Either[String, T] = {
//      if (decodedValue == null) {
//        decodedValue = valueGetter
//      }
//      decodedValue
//    }
//  }

  case class KeyValue[K, T](key: Either[String, K], value: Either[String, T])

  case class KeyValueIterator[K, T](jiterator: java.util.Iterator[KeyVal[ByteBuffer]])(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]) extends Iterator[KeyValue[K, T]] {

    private def extractKeyVal[K, T](keyval: KeyVal[ByteBuffer])(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): KeyValue[K, T] = {
      val key   = keyval.key()
      val value = keyval.`val`()
      KeyValue(kodec.decode(key), codec.decode(value))
    }

    override def hasNext: Boolean = jiterator.hasNext()

    override def next(): KeyValue[K, T] = {
      extractKeyVal(jiterator.next())
    }
  }

  def stream[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
    def streamLogic(colDbi: Dbi[ByteBuffer]): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, T]] = for {
      txn          <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(env.txnRead())
                          .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                      )(txn =>
                        ZIO
                          .attemptBlocking(txn.close())
                          .ignoreLogged
                      )
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(colDbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
    } yield ZStream
      .fromIterator(KeyValueIterator[K, T](iterable.iterator()))
      .map(kv => kv.key.flatMap(key => kv.value.map(value => (key, value))))
      .collect { case either if either.isLeft || either.exists((k, v) => keyFilter(k)) => either.map((k, v) => v) }
      .mapZIO { valueEither => ZIO.fromEither(valueEither).mapError(err => CodecFailure(err)) }
      .mapError {
        case err: CodecFailure => err
        case err: Throwable    => InternalError(s"Couldn't stream from $colName", Some(err))
        case err               => InternalError(s"Couldn't stream from $colName : ${err.toString}", None)
      }

    val result =
      for {
        db     <- getCollectionDbi(colName)
        _      <- reentrantLock.readLock
        stream <- streamLogic(db)
      } yield stream

    ZStream.unwrapScoped(result) // TODO not sure this is the good way ???
  }

  def streamWithKeys[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: LMDBKodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
    def streamLogic(colDbi: Dbi[ByteBuffer]): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, (K, T)]] = for {
      txn          <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(env.txnRead())
                          .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                      )(txn =>
                        ZIO
                          .attemptBlocking(txn.close())
                          .ignoreLogged
                      )
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(colDbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
    } yield ZStream
      .fromIterator(KeyValueIterator[K, T](iterable.iterator()))
      .filter { entry => entry.key.exists(keyFilter) }
      .mapZIO { entry => ZIO.fromEither(entry.value.flatMap(value => entry.key.map(key => key -> value))).mapError(err => CodecFailure(err)) }
      .mapError {
        case err: CodecFailure => err
        case err: Throwable    => InternalError(s"Couldn't stream from $colName", Some(err))
        case err               => InternalError(s"Couldn't stream from $colName : ${err.toString}", None)
      }

    val result =
      for {
        db     <- getCollectionDbi(colName)
        _      <- reentrantLock.readLock
        stream <- streamLogic(db)
      } yield stream

    ZStream.unwrapScoped(result) // TODO not sure this is the good way ???
  }

  private def getIndexDbi(name: IndexName): IO[IndexNotFound, Dbi[ByteBuffer]] = {
    val alreadyHereLogic = for {
      openedCollectionDbis <- openedCollectionDbisRef.get
    } yield openedCollectionDbis.get(name)

    val openAndRememberLogic = for {
      openedCollectionDbis <- reentrantLock.withWriteLock(
                                openedCollectionDbisRef.updateAndGet(before =>
                                  if (before.contains(name)) before
                                  else before + (name -> env.openDbi(name, DbiFlags.MDB_DUPSORT))
                                )
                              )
    } yield openedCollectionDbis.get(name)

    alreadyHereLogic.some
      .orElse(openAndRememberLogic.some)
      .mapError(err => IndexNotFound(name))
  }

  private def indexCreateLogic(name: IndexName): ZIO[Any, StorageSystemError, Unit] = reentrantLock.withWriteLock {
    for {
      openedCollectionDbis <- reentrantLock.withWriteLock(
                                openedCollectionDbisRef.updateAndGet(before =>
                                  if (before.contains(name)) before
                                  else before + (name -> env.openDbi(name, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT))
                                )
                              )
    } yield ()
  }

  private def indexAllocate(name: IndexName): IO[IndexErrors, Unit] = {
    for {
      exists <- indexExists(name)
      _      <- ZIO.cond[IndexAlreadyExists, Unit](!exists, (), IndexAlreadyExists(name))
      _      <- indexCreateLogic(name)
    } yield ()
  }

  override def indexCreate[FROM_KEY, TO_KEY](name: IndexName, failIfExists: Boolean)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] = {
    val allocateLogic = if (failIfExists) indexAllocate(name) else indexAllocate(name).ignore
    allocateLogic.as(LMDBIndex[FROM_KEY, TO_KEY](name, None, this))
  }

  override def indexGet[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] = {
    for {
      exists <- indexExists(name)
      _      <- ZIO.cond[IndexNotFound, Unit](exists, (), IndexNotFound(name))
    } yield LMDBIndex[FROM_KEY, TO_KEY](name, None, this)
  }

  override def indexExists(name: IndexName): IO[IndexErrors, Boolean] = {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      found                <- if (openedCollectionDbis.contains(name)) ZIO.succeed(true)
                              else collectionsAvailable().map(_.contains(name)).mapError(e => e)
    } yield found
  }

  override def indexDrop(name: IndexName): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- collectionClearOrDropLogic(dbi, name, true)
               .mapError {
                 case CollectionNotFound(n) => IndexNotFound(n)
                 case e: StorageSystemError => e
               }
               .mapError(e => e.asInstanceOf[IndexErrors])
      _   <- openedCollectionDbisRef.updateAndGet(_.removed(name))
      _   <- reentrantLock.withWriteLock(
               ZIO
                 .attemptBlocking(dbi.close())
                 .mapError(err => InternalError("Couldn't close index handle", Some(err)))
             )
    } yield ()
  }

  override def indexes(): IO[IndexErrors, List[IndexName]] = {
    collectionsAvailable().mapError(e => e)
  }

  override def index[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- reentrantLock.withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   keyBuffer   <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
                   valueBuffer <- makeKeyByteBuffer(targetKey)(toKeyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
                   _           <- ZIO
                                    .attemptBlocking(dbi.put(txn, keyBuffer, valueBuffer))
                                    .mapError(err => InternalError(s"Couldn't index $key -> $targetKey in $name", Some(err)))
                   _           <- ZIO
                                    .attemptBlocking(txn.commit())
                                    .mapError(err => InternalError("Couldn't commit index transaction", Some(err)))
                 } yield ()
               }
             )
    } yield ()
  }

  override def indexContains[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    def containsLogic(dbi: Dbi[ByteBuffer]): ZIO[Scope, IndexErrors, Boolean] = {
      withReadTransaction(name) { txn =>
        for {
          keyBuffer   <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
          valueBuffer <- makeKeyByteBuffer(targetKey)(toKeyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
          cursor      <- ZIO.acquireRelease(
                           ZIO.attemptBlocking(dbi.openCursor(txn)).mapError(e => InternalError("Cursor error", Some(e)))
                         )(c => ZIO.attemptBlocking(c.close()).ignoreLogged)
          found       <- ZIO
                           .attemptBlocking {
                             @scala.annotation.tailrec
                             def findValue(): Boolean = {
                               if (cursor.`val`().compareTo(valueBuffer) == 0) true
                               else if (cursor.seek(SeekOp.MDB_NEXT_DUP)) findValue()
                               else false
                             }

                             if (cursor.get(keyBuffer, GetOp.MDB_SET)) findValue()
                             else false
                           }
                           .mapError(e => InternalError("Get error", Some(e)))
        } yield found
      }
    }

    for {
      dbi <- getIndexDbi(name)
      res <- reentrantLock.withReadLock(ZIO.scoped(containsLogic(dbi)))
    } yield res
  }

  override def unindex[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- reentrantLock.withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   keyBuffer   <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
                   valueBuffer <- makeKeyByteBuffer(targetKey).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
                   deleted     <- ZIO
                                    .attemptBlocking(dbi.delete(txn, keyBuffer, valueBuffer))
                                    .mapError(e => InternalError("Delete error", Some(e)))
                   _           <- ZIO
                                    .attemptBlocking(txn.commit())
                                    .mapError(e => InternalError("Commit error", Some(e)))
                 } yield deleted
               }
             )
    } yield res
  }

  override def indexed[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]): ZStream[Any, IndexErrors, TO_KEY] = {
    ZStream.unwrapScoped {
      for {
        db <- getIndexDbi(name)
        _  <- reentrantLock.readLock

        txn <- ZIO.acquireRelease(
                 ZIO
                   .attemptBlocking(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $name", Some(err)))
               )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)

        keyBuffer <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }

        cursor <- ZIO.acquireRelease(
                    ZIO
                      .attemptBlocking(db.openCursor(txn))
                      .mapError(err => InternalError(s"Couldn't acquire cursor on $name", Some(err)))
                  )(cursor => ZIO.attemptBlocking(cursor.close()).ignoreLogged)

        found <- ZIO
                   .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                   .mapError(err => InternalError("Seek error", Some(err)))

      } yield {
        if (!found) ZStream.empty
        else {
          ZStream.paginateChunkZIO(true) { isFirst =>
            ZIO
              .attemptBlocking {
                val valid = if (isFirst) true else cursor.seek(SeekOp.MDB_NEXT_DUP)
                if (valid) {
                  val v = cursor.`val`()
                  Some(v)
                } else None
              }
              .mapError(e => InternalError("Cursor iteration error", Some(e)): IndexErrors)
              .flatMap {
                case Some(v) =>
                  ZIO
                    .fromEither(toKeyCodec.decode(v))
                    .mapError(e => CodecFailure(e): IndexErrors)
                    .map(d => (Chunk(d), Some(false)))
                case None    =>
                  ZIO.succeed((Chunk.empty, None))
              }
          }
        }
      }
    }
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
