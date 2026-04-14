/*
 * Copyright 2026 David Crosson
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
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.KeyCodecError

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

  */
class LMDBLive(
  env: Env[ByteBuffer],
  openedCollectionDbisRef: Ref[Map[String, Dbi[ByteBuffer]]],
  writeMutex: TSemaphore,
  activeTransactionRef: FiberRef[Option[ActiveTransaction]],
  writeExecutor: Executor,
  /** @inheritdoc */
  val databasePath: String
) extends LMDB {

  private def withWriteLock[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.scoped(writeMutex.withPermit(effect)).onExecutor(writeExecutor)

  private def withReadLock[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    effect // MVCC: Readers run lock-free in ZIO

  /** Helper to create a direct ByteBuffer for a given key. */
  private def makeKeyByteBuffer[K](id: K)(implicit kodec: KeyCodec[K]): IO[KeyErrors, ByteBuffer] = {
    val keyBytes: Array[Byte] = kodec.encode(id)
    if (keyBytes.length > env.getMaxKeySize) ZIO.fail(OverSizedKey(id.toString, keyBytes.length, env.getMaxKeySize)) // TODO id.toString probably not the best choice
    else
      for {
        key <- ZIO.attempt(ByteBuffer.allocateDirect(keyBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for key: $err", Some(err)))
        _   <- ZIO.attempt(key.put(keyBytes).flip).mapError(err => InternalError(s"Couldn't copy key bytes to buffer: $err", Some(err)))
      } yield key
  }

  /** Gets or opens a collection DBI handle. */
  private def getCollectionDbi(name: CollectionName, txn: Option[Txn[ByteBuffer]] = None): IO[CollectionNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None    =>
          txn match {
            case Some(t) =>
              for {
                newDbi <- ZIO.attempt(env.openDbi(t, name.getBytes(StandardCharsets.UTF_8), null, false))
                _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
              } yield newDbi
            case None    =>
              withWriteLock {
                openedCollectionDbisRef.get.flatMap { openedAgain =>
                  openedAgain.get(name) match {
                    case Some(alreadyOpened) => ZIO.succeed(alreadyOpened)
                    case None                =>
                      for {
                        newDbi <- ZIO.attempt(env.openDbi(name))
                        _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                      } yield newDbi
                  }
                }
              }
          }
      }
    }
  }.mapError(_ => CollectionNotFound(name))

  /** @inheritdoc */
  override def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean] = {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      found                <- if (openedCollectionDbis.contains(name)) ZIO.succeed(true)
                              else collectionsAvailable().map(_.contains(name))
    } yield found
  }

  /** @inheritdoc */
  override def collectionGet[K, T](name: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[GetErrors, LMDBCollection[K, T]] = {
    for {
      exists     <- collectionExists(name)
      collection <- ZIO.cond[CollectionNotFound, LMDBCollection[K, T]](exists, LMDBCollection[K, T](name, this), CollectionNotFound(name))
    } yield collection
  }

  /** @inheritdoc */
  override def collectionSize(name: CollectionName): IO[SizeErrors, Long] = {
    for {
      collectionDbi <- getCollectionDbi(name)
      stats         <- withReadTransaction(name) { txn =>
                         collectionSizeLogic(txn, collectionDbi, name)
                       }
    } yield stats
  }

  private def collectionSizeLogic(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: CollectionName): IO[SizeErrors, Long] = {
    ZIO
      .attempt(dbi.stat(txn))
      .mapError(err => InternalError(s"Couldn't get $name size: $err", Some(err)))
      .map(_.entries)
  }

  /** @inheritdoc */
  override def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit] = {
    for {
      exists <- collectionExists(name)
      _      <- ZIO.cond[CollectionAlreadExists, Unit](!exists, (), CollectionAlreadExists(name))
      _      <- collectionCreateLogic(name)
    } yield ()
  }

  /** @inheritdoc */
  override def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CreateErrors, LMDBCollection[K, T]] = {
    val allocateLogic = if (failIfExists) {
      collectionAllocate(name)
    } else {
      collectionAllocate(name).catchSome { case CollectionAlreadExists(_) =>
        getCollectionDbi(name).ignore
      }
    }
    allocateLogic.as(LMDBCollection[K, T](name, this))
  }

  /** Internal logic to create a collection. */
  private def collectionCreateLogic(name: CollectionName): ZIO[Any, StorageSystemError, Unit] = withWriteLock {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      _                    <- ZIO.when(!openedCollectionDbis.contains(name)) {
                                for {
                                  newDbi <- ZIO
                                              .attempt(env.openDbi(name, DbiFlags.MDB_CREATE))
                                              .mapError(err => InternalError(s"Couldn't create DB $name: $err", Some(err)))
                                  _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                } yield ()
                              }
    } yield ()
  }

  /** Scoped write transaction. */
  private def withWriteTransaction[R, E >: StorageSystemError, A](colName: CollectionName)(use: Txn[ByteBuffer] => ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO
      .acquireReleaseWith(
        ZIO
          .attempt(env.txnWrite())
          .mapError(err => InternalError(s"Couldn't acquire write transaction on $colName: $err", Some(err)))
      )(txn =>
        ZIO
          .attempt(txn.close())
          .ignoreLogged
      )(use)
      .onExecutor(writeExecutor)

  /** Scoped read transaction. */
  private def withReadTransaction(colName: CollectionName): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO.attemptBlocking(env.txnRead())
        .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
    )(txn =>
      ZIO
        .attemptBlocking(txn.close())
        .ignoreLogged
    )

  /** Common logic for clear or drop collection. */
  private def collectionClearOrDropLogic(colDbi: Dbi[ByteBuffer], collectionName: CollectionName, dropDatabase: Boolean): ZIO[Any, StorageSystemError, Unit] = {
    withWriteLock(
      withWriteTransaction(collectionName) { txn =>
        for {
          _ <- ZIO
                 .attempt(colDbi.drop(txn, dropDatabase))
                 .mapError(err => InternalError(s"Couldn't ${if (dropDatabase) "drop" else "clear"} $collectionName: $err", Some(err)))
          _ <- ZIO
                 .attempt(txn.commit())
                 .mapError[StorageSystemError](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
        } yield ()
      }
    )
  }

  /** @inheritdoc */
  override def collectionClear(colName: CollectionName): IO[ClearErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- collectionClearLogic(txn, collectionDbi, colName)
                             _ <- ZIO.attempt(txn.commit()).mapError[ClearErrors](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield ()
                         }
                       )
    } yield ()
  }

  private def collectionClearLogic(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName): IO[ClearErrors, Unit] = {
    ZIO
      .attempt(dbi.drop(txn, false))
      .mapError(err => InternalError(s"Couldn't clear $colName: $err", Some(err)))
      .unit
  }

  /** @inheritdoc */
  override def collectionDrop(colName: CollectionName): IO[DropErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- collectionClearOrDropLogic(collectionDbi, colName, true)
      _             <- openedCollectionDbisRef.updateAndGet(_.removed(colName))
    } yield ()
  }

  /** @inheritdoc */
  override def platformCheck(): IO[StorageSystemError, Unit] = withReadLock {
    ZIO
      .attemptBlockingIO(new Verifier(env).runFor(5, TimeUnit.SECONDS))
      .mapError(err => InternalError(err.getMessage, Some(err)))
      .unit
  }

  /** @inheritdoc */
  override def collectionsAvailable(): IO[StorageSystemError, List[CollectionName]] = {
    withWriteLock( // See https://github.com/lmdbjava/lmdbjava/issues/195
      for {
        collectionNames <- ZIO
                             .attempt(
                               env
                                 .getDbiNames()
                                 .asScala
                                 .map(bytes => new String(bytes))
                                 .toList
                             )
                             .mapError(err => InternalError(s"Couldn't list collections: $err", Some(err)))
      } yield collectionNames
    )
  }

  /** @inheritdoc */
  override def delete[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withWriteLock(
                  withWriteTransaction(colName) { txn =>
                    for {
                      res <- deleteLogic(txn, db, colName, key)
                      _   <- ZIO.attempt(txn.commit()).mapError[DeleteErrors](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                    } yield res
                  }
                )
    } yield result
  }

  private def deleteLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
    for {
      keyBB         <- makeKeyByteBuffer(key)
      found         <- ZIO.attempt(Option(dbi.get(txn, keyBB))).mapError[DeleteErrors](err => InternalError(s"Couldn't fetch $key for delete on $colName: $err", Some(err)))
      mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      mayBeDoc      <- ZIO.foreach(mayBeRawValue) { rawValue =>
                         ZIO.fromEither(codec.decode(rawValue)).mapError[DeleteErrors](msg => CodecFailure(msg))
                       }
      _             <- ZIO.attempt(dbi.delete(txn, keyBB)).mapError[DeleteErrors](err => InternalError(s"Couldn't delete $key from $colName: $err", Some(err)))
    } yield mayBeDoc
  }

  /** @inheritdoc */
  override def fetch[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withReadTransaction(colName) { txn =>
                  fetchLogic(txn, db, colName, key)
                }
    } yield result
  }

  private def fetchLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Any, FetchErrors, Option[T]] = {
    for {
      keyBB         <- makeKeyByteBuffer(key)
      found         <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName: $err", Some(err)))
      mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      document      <- ZIO
                         .foreach(mayBeRawValue) { rawValue =>
                           ZIO.fromEither(codec.decode(rawValue)).mapError[FetchErrors](msg => CodecFailure(msg))
                         }
    } yield document
  }

  /** @inheritdoc */
  override def fetchAt[K, T](colName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- fetchAtLogic(txn, db, colName, index)
                  } yield res
                })
    } yield result
  }

  /** logic for fetching a record at a given index
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param index
    *   index to fetch
    * @return
    *   the record if found
    */
  private def fetchAtLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, FetchErrors, Option[(K, T)]] = {
    for {
      cursor             <- ZIO.acquireRelease(
                              ZIO
                                .attemptBlocking(dbi.openCursor(txn))
                                .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                            )(cursor =>
                              ZIO
                                .attemptBlocking(cursor.close())
                                .ignoreLogged
                            )
      seekFirstSuccess   <- ZIO
                              .attempt(cursor.seek(SeekOp.MDB_FIRST))
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName: $err", Some(err)))
      seekAllNextSuccess <- ZIO
                              .attempt(cursor.seek(SeekOp.MDB_NEXT))
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName: $err", Some(err)))
                              .repeatN(index.toInt) // TODO review and optimize (support long, start from first or from last
                              .when(seekFirstSuccess)
      seeksSuccess        = seekAllNextSuccess.contains(true) || (index == 0 && seekFirstSuccess)
      seekedKey          <- ZIO
                              .fromEither(kodec.decode(cursor.key()))
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $colName: $err", None))
      valBuffer          <- ZIO
                              .attempt(cursor.`val`())
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $colName: $err", Some(err)))
      seekedValue        <- ZIO
                              .foreach(valBuffer) { rawValue =>
                                ZIO
                                  .fromEither(codec.decode(rawValue))
                                  .mapError[FetchErrors](msg => CodecFailure(msg))
                              }
                              .when(seeksSuccess)
                              .map(_.flatten)

    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))
  }

  private def indexFetchLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, FetchErrors, Option[TO_KEY]] = {
    for {
      keyBuffer <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursor    <- ZIO.acquireRelease(
                     ZIO.attemptBlocking(dbi.openCursor(txn)).mapError[FetchErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                   )(c => ZIO.attemptBlocking(c.close()).ignoreLogged)
      found     <- ZIO
                     .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                     .mapError[FetchErrors](e => InternalError(s"Get error: $e", Some(e)))
      result    <- if (found) {
                     for {
                       valBuffer <- ZIO.attempt(cursor.`val`()).mapError[FetchErrors](e => InternalError(s"Val error: $e", Some(e)))
                       decoded   <- ZIO.fromEither(toKeyCodec.decode(valBuffer)).mapError[FetchErrors](e => CodecFailure(e))
                     } yield Some(decoded)
                   } else ZIO.succeed(None)
    } yield result
  }

  private def indexFetchAtLogic[FROM_KEY, TO_KEY](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    name: IndexName,
    position: Long
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      cursor             <- ZIO.acquireRelease(
                              ZIO
                                .attemptBlocking(dbi.openCursor(txn))
                                .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $name: $err", Some(err)))
                            )(cursor =>
                              ZIO
                                .attemptBlocking(cursor.close())
                                .ignoreLogged
                            )
      seekFirstSuccess   <- ZIO
                              .attempt(cursor.seek(SeekOp.MDB_FIRST))
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name: $err", Some(err)))
      seekAllNextSuccess <- if (position > 0) {
                              ZIO
                                .attempt(cursor.seek(SeekOp.MDB_NEXT))
                                .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name: $err", Some(err)))
                                .repeatN(position.toInt - 1)
                            } else ZIO.succeed(true)
      seeksSuccess        = seekAllNextSuccess && seekFirstSuccess
      seekedKey          <- ZIO
                              .fromEither(keyCodec.decode(cursor.key()))
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $name: $err", None))
      valBuffer          <- ZIO
                              .attempt(cursor.`val`())
                              .when(seeksSuccess)
                              .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $name: $err", Some(err)))
      seekedValue        <- ZIO
                              .foreach(valBuffer) { rawValue =>
                                ZIO
                                  .fromEither(toKeyCodec.decode(rawValue))
                                  .mapError[FetchErrors](msg => CodecFailure(msg))
                              }
                              .when(seeksSuccess)
                              .map(_.flatten)

    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))
  }

  private def seek[K, T](colName: CollectionName, recordKey: Option[K], seekOperation: SeekOp)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- seekLogic(txn, db, colName, recordKey, seekOperation)
                  } yield res
                })
    } yield result
  }

  /** logic for seeking a record
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param recordKey
    *   optional key to start from
    * @param seekOperation
    *   seek operation
    * @return
    *   the record if found
    */
  private def seekLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, recordKey: Option[K], seekOperation: SeekOp)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, FetchErrors, Option[(K, T)]] = {
    for {
      cursor      <- ZIO.acquireRelease(
                       ZIO
                         .attemptBlocking(dbi.openCursor(txn))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                     )(cursor =>
                       ZIO
                         .attemptBlocking(cursor.close())
                         .ignoreLogged
                     )
      key         <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk))
      _           <- ZIO.foreachDiscard(key) { k =>
                       ZIO
                         .attempt(cursor.get(k, GetOp.MDB_SET))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't set cursor at $recordKey for $colName: $err", Some(err)))
                     }
      seekSuccess <- ZIO
                       .attempt(cursor.seek(seekOperation))
                       .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName: $err", Some(err)))
      seekedKey   <- ZIO
                       .fromEither(kodec.decode(cursor.key()))
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $colName: $err", None))
      valBuffer   <- ZIO
                       .attempt(cursor.`val`())
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $colName: $err", Some(err)))
      seekedValue <- ZIO
                       .foreach(valBuffer) { rawValue =>
                         ZIO
                           .fromEither(codec.decode(rawValue))
                           .mapError[FetchErrors](msg => CodecFailure(msg))
                       }
                       .when(seekSuccess)
                       .map(_.flatten)
    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))
  }

  /** @inheritdoc */

  override def head[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, None, SeekOp.MDB_FIRST)
  }

  /** @inheritdoc */
  override def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, Some(beforeThatKey), SeekOp.MDB_PREV)
  }

  /** @inheritdoc */
  override def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, Some(afterThatKey), SeekOp.MDB_NEXT)
  }

  /** @inheritdoc */
  override def last[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    seek(collectionName, None, SeekOp.MDB_LAST)
  }

  /** @inheritdoc */
  override def contains[K](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[ContainsErrors, Boolean] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withReadTransaction(colName) { txn =>
                  containsLogic(txn, db, colName, key)
                }
    } yield result
  }

  private def containsLogic[K](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): ZIO[Any, ContainsErrors, Boolean] = {
    for {
      keyBB <- makeKeyByteBuffer(key)
      found <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError[ContainsErrors](err => InternalError(s"Couldn't check $key on $colName: $err", Some(err)))
    } yield found.isDefined
  }

  /** @inheritdoc */
  override def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]] = {
    for {
      collectionDbi <- getCollectionDbi(collectionName)
      result        <- withWriteLock(
                         withWriteTransaction(collectionName) { txn =>
                           for {
                             res <- updateLogic(txn, collectionDbi, collectionName, key, modifier)
                             _   <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield res
                         }
                       )
    } yield result
  }

  /** logic for updating a record
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param collectionName
    *   collection name
    * @param key
    *   key to update
    * @param modifier
    *   modifier lambda
    * @return
    *   the updated record if found
    */
  private def updateLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]] = {
    for {
      keyBB          <- makeKeyByteBuffer(key)
      found          <- ZIO.attempt(Option(dbi.get(txn, keyBB))).mapError(err => InternalError(s"Couldn't fetch $key for update on $collectionName: $err", Some(err)))
      mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                          ZIO.fromEither(codec.decode(rawValue)).mapError[UpdateErrors](msg => CodecFailure(msg))
                        }
      mayBeDocAfter   = mayBeDocBefore.map(modifier)
      _              <- ZIO.foreachDiscard(mayBeDocAfter) { docAfter =>
                          val docBytes = codec.encode(docAfter)
                          for {
                            valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
                            _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
                            _           <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't update $key into $collectionName: $err", Some(err)))
                          } yield ()
                        }
    } yield mayBeDocAfter
  }

  /** @inheritdoc */
  override def upsertOverwrite[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- upsertOverwriteLogic(txn, collectionDbi, colName, key, document)
                             _ <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield ()
                         }
                       )
    } yield result
  }

  /** logic for overwriting/inserting a record
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param key
    *   key to upsert
    * @param document
    *   record content
    */
  private def upsertOverwriteLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    for {
      keyBB       <- makeKeyByteBuffer(key)
      docBytes     = codec.encode(document)
      valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      _           <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't upsertOverwrite $key into $colName: $err", Some(err)))
    } yield ()
  }

  /** @inheritdoc */
  override def upsert[K, T](colName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             res <- upsertLogic(txn, collectionDbi, colName, key, modifier)
                             _   <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield res
                         }
                       )
    } yield result
  }

  /** logic for updating or inserting a record
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param key
    *   key to upsert
    * @param modifier
    *   modifier lambda
    * @return
    *   the updated or inserted record
    */
  private def upsertLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T] = {
    for {
      keyBB          <- makeKeyByteBuffer(key)
      found          <- ZIO.attempt(Option(dbi.get(txn, keyBB))).mapError(err => InternalError(s"Couldn't fetch $key for upsert on $colName: $err", Some(err)))
      mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                          ZIO.fromEither(codec.decode(rawValue)).mapError[UpsertErrors](msg => CodecFailure(msg))
                        }
      docAfter        = modifier(mayBeDocBefore)
      docBytes        = codec.encode(docAfter)
      valueBuffer    <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _              <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      _              <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $key into $colName: $err", Some(err)))
    } yield docAfter
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

  /** @inheritdoc */
  override def collect[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CollectErrors, List[T]] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      collected     <- withReadLock(ZIO.scoped {
                         for {
                           txn <- ZIO.acquireRelease(
                                    ZIO
                                      .attemptBlocking(env.txnRead())
                                      .mapError[CollectErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                                  )(txn =>
                                    ZIO
                                      .attemptBlocking(txn.close())
                                      .ignoreLogged
                                  )
                           res <- collectLogic(txn, collectionDbi, colName, keyFilter, valueFilter, startAfter, backward, limit)
                         } yield res
                       })
    } yield collected
  }

  /** logic for collecting records
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param keyFilter
    *   key filter
    * @param valueFilter
    *   value filter
    * @param startAfter
    *   optional key to start after
    * @param backward
    *   backward iteration
    * @param limit
    *   optional limit
    * @return
    *   the list of records
    */
  private def collectLogic[K, T](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    valueFilter: T => Boolean = (_: T) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false,
    limit: Option[Int] = None
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, CollectErrors, List[T]] = {
    for {
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError[CollectErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
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

  case class KeyValue[K, T](key: Either[KeyCodecError, K], value: Either[String, T])

  case class KeyValueIterator[K, T](jiterator: java.util.Iterator[KeyVal[ByteBuffer]])(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]) extends Iterator[KeyValue[K, T]] {

    private def extractKeyVal[K, T](keyval: KeyVal[ByteBuffer])(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): KeyValue[K, T] = {
      val key   = keyval.key()
      val value = keyval.`val`()
      KeyValue(kodec.decode(key), codec.decode(value))
    }

    override def hasNext: Boolean = jiterator.hasNext()

    override def next(): KeyValue[K, T] = {
      extractKeyVal(jiterator.next())
    }
  }

  override def stream[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
    val result =
      for {
        db  <- getCollectionDbi(colName)
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attemptBlocking(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attemptBlocking(txn.close())
                   .ignoreLogged
               )
        s   <- streamLogic(txn, db, colName, keyFilter, startAfter, backward)
      } yield s

    ZStream.unwrapScoped(result)
  }

  private def streamLogic[K, T](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    colName: CollectionName,
    keyFilter: K => Boolean,
    startAfter: Option[K],
    backward: Boolean
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, T]] = {
    for {
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
    } yield ZStream
      .fromIterator(KeyValueIterator[K, T](iterable.iterator()))
      .map(kv => kv.key.left.map(_.toString).flatMap(key => kv.value.map(value => (key, value))))
      .collect { case either if either.isLeft || either.exists((k, v) => keyFilter(k)) => either.map((k, v) => v) }
      .mapZIO { valueEither => ZIO.fromEither(valueEither).mapError(err => CodecFailure(err)) }
      .mapError {
        case err: CodecFailure => err
        case err: Throwable    => InternalError(s"Couldn't stream from $colName: $err", Some(err))
        case err               => InternalError(s"Couldn't stream from $colName : ${err.toString}", None)
      }
  }

  override def streamWithKeys[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
    val result =
      for {
        db  <- getCollectionDbi(colName)
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attemptBlocking(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attemptBlocking(txn.close())
                   .ignoreLogged
               )
        s   <- streamWithKeysLogic(txn, db, colName, keyFilter, startAfter, backward)
      } yield s

    ZStream.unwrapScoped(result)
  }

  private def streamWithKeysLogic[K, T](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    colName: CollectionName,
    keyFilter: K => Boolean,
    startAfter: Option[K],
    backward: Boolean
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, (K, T)]] = {
    for {
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attemptBlocking(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
    } yield ZStream
      .fromIterator(KeyValueIterator[K, T](iterable.iterator()))
      .filter { entry => entry.key.exists(keyFilter) }
      .mapZIO { entry => ZIO.fromEither(entry.value.flatMap(value => entry.key.left.map(_.toString).map(key => key -> value))).mapError(err => CodecFailure(err)) }
      .mapError {
        case err: CodecFailure => err
        case err: Throwable    => InternalError(s"Couldn't stream from $colName: $err", Some(err))
        case err               => InternalError(s"Couldn't stream from $colName : ${err.toString}", None)
      }
  }

  /** Gets or opens an index DBI handle. */
  private def getIndexDbi(name: IndexName, txn: Option[Txn[ByteBuffer]] = None): IO[IndexNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None    =>
          txn match {
            case Some(t) =>
              for {
                newDbi <- ZIO.attempt(env.openDbi(t, name.getBytes(StandardCharsets.UTF_8), null, false, DbiFlags.MDB_DUPSORT))
                _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
              } yield newDbi
            case None    =>
              withWriteLock {
                openedCollectionDbisRef.get.flatMap { openedAgain =>
                  openedAgain.get(name) match {
                    case Some(alreadyOpened) => ZIO.succeed(alreadyOpened)
                    case None                =>
                      for {
                        newDbi <- ZIO.attempt(env.openDbi(name, DbiFlags.MDB_DUPSORT))
                        _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                      } yield newDbi
                  }
                }
              }
          }
      }
    }
  }.mapError(_ => IndexNotFound(name))

  /** Internal logic to create an index. */
  private def indexCreateLogic(name: IndexName): ZIO[Any, StorageSystemError, Unit] = withWriteLock {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      _                    <- ZIO.when(!openedCollectionDbis.contains(name)) {
                                for {
                                  newDbi <- ZIO
                                              .attempt(env.openDbi(name, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT))
                                              .mapError(err => InternalError(s"Couldn't create Index $name: $err", Some(err)))
                                  _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                } yield ()
                              }
    } yield ()
  }

  /** Allocates an index if it doesn't exist. */
  private def indexAllocate(name: IndexName): IO[IndexErrors, Unit] = {
    for {
      exists <- indexExists(name)
      _      <- ZIO.cond[IndexAlreadyExists, Unit](!exists, (), IndexAlreadyExists(name))
      _      <- indexCreateLogic(name)
    } yield ()
  }

  /** @inheritdoc */
  override def indexCreate[FROM_KEY, TO_KEY](name: IndexName, failIfExists: Boolean)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] = {
    val allocateLogic = if (failIfExists) {
      indexAllocate(name)
    } else {
      indexAllocate(name).catchSome { case IndexAlreadyExists(_) =>
        getIndexDbi(name).ignore
      }
    }
    allocateLogic.as(LMDBIndex[FROM_KEY, TO_KEY](name, None, this))
  }

  /** @inheritdoc */
  override def indexGet[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] = {
    for {
      exists <- indexExists(name)
      _      <- ZIO.cond[IndexNotFound, Unit](exists, (), IndexNotFound(name))
    } yield LMDBIndex[FROM_KEY, TO_KEY](name, None, this)
  }

  /** @inheritdoc */
  override def indexExists(name: IndexName): IO[IndexErrors, Boolean] = {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      found                <- if (openedCollectionDbis.contains(name)) ZIO.succeed(true)
                              else collectionsAvailable().map(_.contains(name)).mapError(e => e)
    } yield found
  }

  /** @inheritdoc */
  override def indexDrop(name: IndexName): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- collectionClearOrDropLogic(dbi, name, true)
               .mapError(e => e: IndexErrors)
      _   <- openedCollectionDbisRef.updateAndGet(_.removed(name))
    } yield ()
  }

  /** @inheritdoc */
  override def indexes(): IO[IndexErrors, List[IndexName]] = {
    collectionsAvailable().mapError(e => e)
  }

  /** @inheritdoc */
  override def index[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   _ <- indexLogic(txn, dbi, name, key, targetKey)
                   _ <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit index transaction: $err", Some(err)))
                 } yield ()
               }
             )
    } yield ()
  }

  /** logic for adding a mapping to an index
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param name
    *   index name
    * @param key
    *   key to index
    * @param targetKey
    *   target key to map to
    */
  private def indexLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit] = {
    for {
      keyBuffer   <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      valueBuffer <- makeKeyByteBuffer(targetKey)(toKeyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      _           <- ZIO
                       .attempt(dbi.put(txn, keyBuffer, valueBuffer))
                       .mapError(err => InternalError(s"Couldn't index $key -> $targetKey in $name: $err", Some(err)))
    } yield ()
  }

  /** @inheritdoc */
  override def indexContains[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attemptBlocking(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                        )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
                 res <- indexContainsLogic(txn, dbi, name, key, targetKey)
               } yield res
             })
    } yield res
  }

  /** @inheritdoc */
  override def indexHasKey[FROM_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attemptBlocking(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                        )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
                 res <- indexHasKeyLogic(txn, dbi, name, key)
               } yield res
             })
    } yield res
  }

  private def indexHasKeyLogic[FROM_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): ZIO[Scope, IndexErrors, Boolean] = {
    for {
      keyBuffer <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursor    <- ZIO.acquireRelease(
                     ZIO.attemptBlocking(dbi.openCursor(txn)).mapError(e => InternalError(s"Cursor error: $e", Some(e)))
                   )(c => ZIO.attemptBlocking(c.close()).ignoreLogged)
      found     <- ZIO
                     .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                     .mapError(e => InternalError(s"Get error: $e", Some(e)))
    } yield found
  }

  private def indexSeek[FROM_KEY, TO_KEY](name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      db     <- getIndexDbi(name).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- indexSeekLogic(txn, db, name, recordKey, seekOperation)(keyCodec, toKeyCodec)
                  } yield res
                })
    } yield result
  }

  private def indexSeekLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit
    keyCodec: KeyCodec[FROM_KEY],
    toKeyCodec: KeyCodec[TO_KEY]
  ): ZIO[Scope, FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      cursor      <- ZIO.acquireRelease(
                       ZIO
                         .attemptBlocking(dbi.openCursor(txn))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $name: $err", Some(err)))
                     )(cursor =>
                       ZIO
                         .attemptBlocking(cursor.close())
                         .ignoreLogged
                     )
      key         <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e })
      _           <- ZIO.foreachDiscard(key) { k =>
                       ZIO
                         .attempt(cursor.get(k, GetOp.MDB_SET))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't set cursor at $recordKey for $name: $err", Some(err)))
                     }
      seekSuccess <- ZIO
                       .attempt(cursor.seek(seekOperation))
                       .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name: $err", Some(err)))
      seekedKey   <- ZIO
                       .fromEither(keyCodec.decode(cursor.key()))
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $name: $err", None))
      valBuffer   <- ZIO
                       .attempt(cursor.`val`())
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $name: $err", Some(err)))
      seekedValue <- ZIO
                       .foreach(valBuffer) { rawValue =>
                         ZIO
                           .fromEither(toKeyCodec.decode(rawValue))
                           .mapError[FetchErrors](msg => CodecFailure(msg))
                       }
                       .when(seekSuccess)
                       .map(_.flatten)
    } yield seekedValue.flatMap(v => seekedKey.map(k => k -> v))
  }

  /** @inheritdoc */
  override def indexHead[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    indexSeek(name, None, SeekOp.MDB_FIRST)

  /** @inheritdoc */
  override def indexLast[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    indexSeek(name, None, SeekOp.MDB_LAST)

  /** @inheritdoc */
  override def indexPrevious[FROM_KEY, TO_KEY](name: IndexName, beforeThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    indexSeek(name, Some(beforeThatKey), SeekOp.MDB_PREV)

  /** @inheritdoc */
  override def indexNext[FROM_KEY, TO_KEY](name: IndexName, afterThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
    indexSeek(name, Some(afterThatKey), SeekOp.MDB_NEXT)

  /** @inheritdoc */
  override def indexFetch[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[TO_KEY]] = {
    for {
      db     <- getIndexDbi(name).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- indexFetchLogic(txn, db, name, key)(keyCodec, toKeyCodec)
                  } yield res
                })
    } yield result
  }

  /** @inheritdoc */
  override def indexFetchAt[FROM_KEY, TO_KEY](name: IndexName, position: Long)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      db     <- getIndexDbi(name).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- indexFetchAtLogic(txn, db, name, position)(keyCodec, toKeyCodec)
                  } yield res
                })
    } yield result
  }

  /** logic for checking if an index contains a mapping
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param name
    *   index name
    * @param key
    *   key to check
    * @param targetKey
    *   target key to check
    * @return
    *   true if the mapping is found
    */
  private def indexContainsLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit
    keyCodec: KeyCodec[FROM_KEY],
    toKeyCodec: KeyCodec[TO_KEY]
  ): ZIO[Scope, IndexErrors, Boolean] = {
    for {
      keyBuffer   <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      valueBuffer <- makeKeyByteBuffer(targetKey)(toKeyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursor      <- ZIO.acquireRelease(
                       ZIO.attemptBlocking(dbi.openCursor(txn)).mapError(e => InternalError(s"Cursor error: $e", Some(e)))
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
                       .mapError(e => InternalError(s"Get error: $e", Some(e)))
    } yield found
  }

  /** @inheritdoc */
  override def unindex[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   res <- unindexLogic(txn, dbi, name, key, targetKey)
                   _   <- ZIO.attempt(txn.commit()).mapError(e => InternalError(s"Commit error: $e", Some(e)))
                 } yield res
               }
             )
    } yield res
  }

  /** @inheritdoc */
  override def indexClear(name: IndexName): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   _ <- ZIO.attempt(dbi.drop(txn, false)).mapError(e => InternalError(s"Couldn't clear index $name: $e", Some(e)))
                   _ <- ZIO.attempt(txn.commit()).mapError(e => InternalError(s"Commit error: $e", Some(e)))
                 } yield ()
               }
             )
    } yield ()
  }

  /** logic for removing a mapping from an index
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param name
    *   index name
    * @param key
    *   key to unindex
    * @param targetKey
    *   target key to unmap
    * @return
    *   true if the mapping was found and removed
    */
  private def unindexLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      keyBuffer   <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      valueBuffer <- makeKeyByteBuffer(targetKey).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      deleted     <- ZIO
                       .attempt(dbi.delete(txn, keyBuffer, valueBuffer))
                       .mapError(e => InternalError(s"Delete error: $e", Some(e)))
    } yield deleted
  }

  /** @inheritdoc */
  override def indexed[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, limitToKey: Boolean)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)] = {
    ZStream.unwrapScoped {
      for {
        db  <- getIndexDbi(name)
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attemptBlocking(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
               )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
        s   <- indexedLogic(txn, db, name, key, limitToKey)(keyCodec, toKeyCodec)
      } yield s
    }
  }

  private def indexedLogic[FROM_KEY, TO_KEY](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    name: IndexName,
    key: FROM_KEY,
    limitToKey: Boolean
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, IndexErrors, ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)]] = {
    for {
      keyBuffer <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }

      cursor <- ZIO.acquireRelease(
                  ZIO
                    .attemptBlocking(dbi.openCursor(txn))
                    .mapError(err => InternalError(s"Couldn't acquire cursor on $name: $err", Some(err)))
                )(cursor => ZIO.attemptBlocking(cursor.close()).ignoreLogged)

      found <- ZIO
                 .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                 .mapError(err => InternalError(s"Seek error: $err", Some(err)))

    } yield {
      if (!found) ZStream.empty
      else {
        ZStream.paginateChunkZIO(true) { isFirst =>
          ZIO
            .attemptBlocking {
              val valid =
                if (isFirst) true
                else if (limitToKey) cursor.seek(SeekOp.MDB_NEXT_DUP)
                else cursor.seek(SeekOp.MDB_NEXT)
              if (valid) {
                val v = cursor.`val`()
                val k = cursor.key()
                Some((k, v))
              } else None
            }
            .mapError(e => InternalError(s"Cursor iteration error: $e", Some(e)): IndexErrors)
            .flatMap {
              case Some((k, v)) =>
                val decoded = for {
                  key   <- keyCodec.decode(k)
                  value <- toKeyCodec.decode(v)
                } yield (key, value)

                ZIO
                  .fromEither(decoded)
                  .mapError(e => CodecFailure(e): IndexErrors)
                  .map(d => (Chunk(d), Some(false)))
              case None         =>
                ZIO.succeed((Chunk.empty, None))
            }
        }
      }
    }
  }

  /** @inheritdoc */
  override def readOnly[R, E, A](f: LMDBReadOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] = {
    withReadLock(
      ZIO.scoped(
        for {
          txn <- ZIO.acquireRelease(
                   ZIO
                     .attemptBlocking(env.txnRead())
                     .mapError(err => InternalError(s"Couldn't acquire read transaction: $err", Some(err)))
                 )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
          ops  = new LMDBReadOpsLive(txn)
          res <- f(ops)
        } yield res
      )
    )
  }

  /** @inheritdoc */
  override def readWrite[R, E, A](f: LMDBWriteOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | StorageUserError.NestedTransactionError, A] = {
    activeTransactionRef.get.flatMap {
      case Some(active) => ZIO.fail(NestedTransactionError(active))
      case None         =>
        Clock.currentDateTime.flatMap { now =>
          activeTransactionRef.locally(Some(ActiveTransaction(now))) {
            withWriteLock(
              ZIO
                .scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attempt(env.txnWrite())
                               .mapError(err => InternalError(s"Couldn't acquire write transaction: $err", Some(err)))
                           )(txn =>
                             ZIO
                               .attempt(txn.close())
                               .ignoreLogged
                           )
                    ops  = new LMDBWriteOpsLive(txn)
                    res <- f(ops)
                    _   <- ZIO
                             .attempt(txn.commit())
                             .mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                  } yield res
                }
            )
          }
        }
    }
  }

  /** Live implementation of read-only operations using a shared transaction. */
  private class LMDBReadOpsLive(txn: Txn[ByteBuffer]) extends LMDBReadOps {

    /** @inheritdoc */
    override def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean] = {
      getCollectionDbi(name, Some(txn)).as(true).catchAll(_ => ZIO.succeed(false))
    }

    /** @inheritdoc */
    override def collectionSize(name: CollectionName): IO[SizeErrors, Long] = {
      for {
        collectionDbi <- getCollectionDbi(name, Some(txn))
        size          <- collectionSizeLogic(txn, collectionDbi, name)
      } yield size
    }

    /** @inheritdoc */
    override def fetch[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[T]] = {
      for {
        db  <- getCollectionDbi(colName, Some(txn))
        res <- fetchLogic(txn, db, colName, key)
      } yield res
    }

    /** @inheritdoc */
    override def fetchAt[K, T](colName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
      for {
        db  <- getCollectionDbi(colName, Some(txn))
        res <- ZIO.scoped(fetchAtLogic(txn, db, colName, index))
      } yield res
    }

    /** @inheritdoc */
    override def head[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] =
      seek(collectionName, None, SeekOp.MDB_FIRST)

    /** @inheritdoc */
    override def previous[K, T](collectionName: CollectionName, beforeThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] =
      seek(collectionName, Some(beforeThatKey), SeekOp.MDB_PREV)

    /** @inheritdoc */
    override def next[K, T](collectionName: CollectionName, afterThatKey: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] =
      seek(collectionName, Some(afterThatKey), SeekOp.MDB_NEXT)

    /** @inheritdoc */
    override def last[K, T](collectionName: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] =
      seek(collectionName, None, SeekOp.MDB_LAST)

    /** logic for seeking a record within a transaction
      * @param colName
      *   collection name
      * @param recordKey
      *   optional key to start from
      * @param seekOperation
      *   seek operation
      * @return
      *   the record if found
      */
    private def seek[K, T](colName: CollectionName, recordKey: Option[K], seekOperation: SeekOp)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
      for {
        db  <- getCollectionDbi(colName, Some(txn))
        res <- ZIO.scoped(seekLogic(txn, db, colName, recordKey, seekOperation))
      } yield res
    }

    /** @inheritdoc */
    override def contains[K](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[ContainsErrors, Boolean] = {
      for {
        db  <- getCollectionDbi(colName, Some(txn))
        res <- containsLogic(txn, db, colName, key)
      } yield res
    }

    /** @inheritdoc */
    override def collect[K, T](
      colName: CollectionName,
      keyFilter: K => Boolean,
      valueFilter: T => Boolean,
      startAfter: Option[K],
      backward: Boolean,
      limit: Option[Int]
    )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CollectErrors, List[T]] = {
      for {
        collectionDbi <- getCollectionDbi(colName, Some(txn))
        res           <- ZIO.scoped(collectLogic(txn, collectionDbi, colName, keyFilter, valueFilter, startAfter, backward, limit))
      } yield res
    }

    /** @inheritdoc */
    override def indexExists(name: IndexName): IO[IndexErrors, Boolean] = {
      getIndexDbi(name, Some(txn)).as(true).catchAll(_ => ZIO.succeed(false))
    }

    /** @inheritdoc */
    override def indexContains[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
      for {
        dbi <- getIndexDbi(name, Some(txn))
        res <- ZIO.scoped(indexContainsLogic(txn, dbi, name, key, targetKey))
      } yield res
    }

    /** @inheritdoc */
    override def indexHasKey[FROM_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): IO[IndexErrors, Boolean] = {
      for {
        dbi <- getIndexDbi(name, Some(txn))
        res <- ZIO.scoped(indexHasKeyLogic(txn, dbi, name, key))
      } yield res
    }

    private def indexSeek[FROM_KEY, TO_KEY](name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
      for {
        db  <- getIndexDbi(name, Some(txn)).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
        res <- ZIO.scoped(indexSeekLogic(txn, db, name, recordKey, seekOperation)(keyCodec, toKeyCodec))
      } yield res
    }

    /** @inheritdoc */
    override def indexHead[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
      indexSeek(name, None, SeekOp.MDB_FIRST)

    /** @inheritdoc */
    override def indexLast[FROM_KEY, TO_KEY](name: IndexName)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
      indexSeek(name, None, SeekOp.MDB_LAST)

    /** @inheritdoc */
    override def indexPrevious[FROM_KEY, TO_KEY](name: IndexName, beforeThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
      indexSeek(name, Some(beforeThatKey), SeekOp.MDB_PREV)

    /** @inheritdoc */
    override def indexNext[FROM_KEY, TO_KEY](name: IndexName, afterThatKey: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] =
      indexSeek(name, Some(afterThatKey), SeekOp.MDB_NEXT)

    /** @inheritdoc */
    override def indexFetch[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[TO_KEY]] = {
      for {
        db  <- getIndexDbi(name, Some(txn)).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
        res <- ZIO.scoped(indexFetchLogic(txn, db, name, key)(keyCodec, toKeyCodec))
      } yield res
    }

    /** @inheritdoc */
    override def indexFetchAt[FROM_KEY, TO_KEY](name: IndexName, position: Long)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
      for {
        db  <- getIndexDbi(name, Some(txn)).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
        res <- ZIO.scoped(indexFetchAtLogic(txn, db, name, position)(keyCodec, toKeyCodec))
      } yield res
    }

    /** @inheritdoc */
    override def indexed[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, limitToKey: Boolean)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZStream[Any, IndexErrors, (FROM_KEY, TO_KEY)] = {
      val result = for {
        db     <- getIndexDbi(name, Some(txn))
        stream <- indexedLogic(txn, db, name, key, limitToKey)(keyCodec, toKeyCodec)
      } yield stream
      ZStream.unwrapScoped(result)
    }

    /** @inheritdoc */
    override def stream[K, T](
      collectionName: CollectionName,
      keyFilter: K => Boolean = (_: K) => true,
      startAfter: Option[K] = None,
      backward: Boolean = false
    )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
      val result = for {
        db     <- getCollectionDbi(collectionName, Some(txn))
        stream <- streamLogic(txn, db, collectionName, keyFilter, startAfter, backward)
      } yield stream
      ZStream.unwrapScoped(result)
    }

    /** @inheritdoc */
    override def streamWithKeys[K, T](
      collectionName: CollectionName,
      keyFilter: K => Boolean = (_: K) => true,
      startAfter: Option[K] = None,
      backward: Boolean = false
    )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
      val result = for {
        db     <- getCollectionDbi(collectionName, Some(txn))
        stream <- streamWithKeysLogic(txn, db, collectionName, keyFilter, startAfter, backward)
      } yield stream
      ZStream.unwrapScoped(result)
    }
  }

  /** Live implementation of read-write operations using a shared transaction. */
  private class LMDBWriteOpsLive(txn: Txn[ByteBuffer]) extends LMDBReadOpsLive(txn) with LMDBWriteOps {

    /** @inheritdoc */
    override def collectionClear(name: CollectionName): IO[ClearErrors, Unit] = {
      for {
        collectionDbi <- getCollectionDbi(name, Some(txn))
        _             <- collectionClearLogic(txn, collectionDbi, name)
      } yield ()
    }

    /** @inheritdoc */
    override def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]] = {
      for {
        collectionDbi <- getCollectionDbi(collectionName, Some(txn))
        res           <- updateLogic(txn, collectionDbi, collectionName, key, modifier)
      } yield res
    }

    /** @inheritdoc */
    override def upsert[K, T](collectionName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T] = {
      for {
        collectionDbi <- getCollectionDbi(collectionName, Some(txn))
        res           <- upsertLogic(txn, collectionDbi, collectionName, key, modifier)
      } yield res
    }

    /** @inheritdoc */
    override def upsertOverwrite[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
      for {
        collectionDbi <- getCollectionDbi(collectionName, Some(txn))
        _             <- upsertOverwriteLogic(txn, collectionDbi, collectionName, key, document)
      } yield ()
    }

    /** @inheritdoc */
    override def delete[K, T](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
      for {
        db  <- getCollectionDbi(collectionName, Some(txn))
        res <- deleteLogic(txn, db, collectionName, key)
      } yield res
    }

    /** @inheritdoc */
    override def index[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit] = {
      for {
        dbi <- getIndexDbi(name, Some(txn))
        _   <- indexLogic(txn, dbi, name, key, targetKey)
      } yield ()
    }

    /** @inheritdoc */
    override def unindex[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
      for {
        dbi <- getIndexDbi(name, Some(txn))
        res <- unindexLogic(txn, dbi, name, key, targetKey)
      } yield res
    }

    /** @inheritdoc */
    override def indexClear(name: IndexName): IO[IndexErrors, Unit] = {
      for {
        dbi <- getIndexDbi(name, Some(txn))
        _   <- ZIO.attempt(dbi.drop(txn, false)).mapError(e => InternalError(s"Couldn't clear index $name: $e", Some(e)))
      } yield ()
    }
  }
}

object LMDBLive {

  private def lmdbCreateEnv(config: LMDBConfig, databasePath: File) = {
    val syncFlag = if (!config.fileSystemSynchronized) Some(EnvFlags.MDB_NOSYNC) else None

    val flags = Array(
      EnvFlags.MDB_NOTLS // MVCC: readers run lock-free in ZIO across fibers
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
                              )(env => ZIO.attemptBlocking(env.close()).ignoreLogged)
      openedCollectionDbis <- Ref.make[Map[String, Dbi[ByteBuffer]]](Map.empty)
      writeMutex           <- TSemaphore.make(1).commit
      activeTransactionRef <- FiberRef.make[Option[ActiveTransaction]](None)
      executorService      <- ZIO.acquireRelease(
                                ZIO.attempt(java.util.concurrent.Executors.newSingleThreadExecutor())
                              )(es => ZIO.attempt(es.shutdown()).ignoreLogged)
      writeExecutor         = Executor.fromJavaExecutor(executorService)
    } yield new LMDBLive(environment, openedCollectionDbis, writeMutex, activeTransactionRef, writeExecutor, databasePath.toString)
  }
}
