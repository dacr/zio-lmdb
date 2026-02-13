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
  /** @inheritdoc */
  val databasePath: String
) extends LMDB {

  /** Helper to create a direct ByteBuffer for a given key. */
  private def makeKeyByteBuffer[K](id: K)(implicit kodec: KeyCodec[K]): IO[KeyErrors, ByteBuffer] = {
    val keyBytes: Array[Byte] = kodec.encode(id)
    if (keyBytes.length > env.getMaxKeySize) ZIO.fail(OverSizedKey(id.toString, keyBytes.length, env.getMaxKeySize)) // TODO id.toString probably not the best choice
    else
      for {
        key <- ZIO.attempt(ByteBuffer.allocateDirect(keyBytes.length)).mapError(err => InternalError("Couldn't allocate byte buffer for key", Some(err)))
        _   <- ZIO.attempt(key.put(keyBytes).flip).mapError(err => InternalError("Couldn't copy key bytes to buffer", Some(err)))
      } yield key
  }

  /** Gets or opens a collection DBI handle. */
  private def getCollectionDbi(name: CollectionName, txn: Option[Txn[ByteBuffer]] = None): IO[CollectionNotFound, Dbi[ByteBuffer]] = {
    val logic = for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      dbi                  <- ZIO
                                .fromOption(openedCollectionDbis.get(name))
                                .orElse {
                                  for {
                                    newDbi <- txn match {
                                                case Some(t) => ZIO.attempt(env.openDbi(t, name.getBytes(StandardCharsets.UTF_8), null, false))
                                                case None    => ZIO.attemptBlocking(env.openDbi(name))
                                              }
                                    _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                  } yield newDbi
                                }
    } yield dbi

    reentrantLock.withWriteLock(logic).mapError(_ => CollectionNotFound(name))
  }

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
      stats         <- reentrantLock.withReadLock(
                         withReadTransaction(name) { txn =>
                           collectionSizeLogic(txn, collectionDbi, name)
                         }
                       )
    } yield stats
  }

  private def collectionSizeLogic(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: CollectionName): IO[SizeErrors, Long] = {
    ZIO
      .attempt(dbi.stat(txn))
      .mapError(err => InternalError(s"Couldn't get $name size", Some(err)))
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
  private def collectionCreateLogic(name: CollectionName): ZIO[Any, StorageSystemError, Unit] = reentrantLock.withWriteLock {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      _                    <- ZIO.when(!openedCollectionDbis.contains(name)) {
                                for {
                                  newDbi <- ZIO
                                              .attemptBlocking(env.openDbi(name, DbiFlags.MDB_CREATE))
                                              .mapError(err => InternalError(s"Couldn't create DB $name", Some(err)))
                                  _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                } yield ()
                              }
    } yield ()
  }

  /** Scoped write transaction. */
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

  /** Scoped read transaction. */
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

  /** Common logic for clear or drop collection. */
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

  /** @inheritdoc */
  override def collectionClear(colName: CollectionName): IO[ClearErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      _             <- reentrantLock.withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- collectionClearLogic(txn, collectionDbi, colName)
                             _ <- ZIO.attemptBlocking(txn.commit()).mapError[ClearErrors](err => InternalError("Couldn't commit transaction", Some(err)))
                           } yield ()
                         }
                       )
    } yield ()
  }

  private def collectionClearLogic(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName): IO[ClearErrors, Unit] = {
    ZIO
      .attemptBlocking(dbi.drop(txn, false))
      .mapError(err => InternalError(s"Couldn't clear $colName", Some(err)))
      .unit
  }

  /** @inheritdoc */
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

  /** @inheritdoc */
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

  /** @inheritdoc */
  override def delete[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- reentrantLock.withWriteLock(
                  withWriteTransaction(colName) { txn =>
                    for {
                      res <- deleteLogic(txn, db, colName, key)
                      _   <- ZIO.attemptBlocking(txn.commit()).mapError[DeleteErrors](err => InternalError("Couldn't commit transaction", Some(err)))
                    } yield res
                  }
                )
    } yield result
  }

  private def deleteLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Option[T]] = {
    for {
      keyBB         <- makeKeyByteBuffer(key)
      found         <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError[DeleteErrors](err => InternalError(s"Couldn't fetch $key for delete on $colName", Some(err)))
      mayBeRawValue <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      mayBeDoc      <- ZIO.foreach(mayBeRawValue) { rawValue =>
                         ZIO.fromEither(codec.decode(rawValue)).mapError[DeleteErrors](msg => CodecFailure(msg))
                       }
      _             <- ZIO.attemptBlocking(dbi.delete(txn, keyBB)).mapError[DeleteErrors](err => InternalError(s"Couldn't delete $key from $colName", Some(err)))
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
      found         <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName", Some(err)))
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
      result <- ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- fetchAtLogic(txn, db, colName, index)
                  } yield res
                }
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
  }

  private def seek[K, T](colName: CollectionName, recordKey: Option[K], seekOperation: SeekOp)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- seekLogic(txn, db, colName, recordKey, seekOperation)
                  } yield res
                }
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
      found <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError[ContainsErrors](err => InternalError(s"Couldn't check $key on $colName", Some(err)))
    } yield found.isDefined
  }

  /** @inheritdoc */
  override def update[K, T](collectionName: CollectionName, key: K, modifier: T => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpdateErrors, Option[T]] = {
    for {
      collectionDbi <- getCollectionDbi(collectionName)
      result        <- reentrantLock.withWriteLock(
                         withWriteTransaction(collectionName) { txn =>
                           for {
                             res <- updateLogic(txn, collectionDbi, collectionName, key, modifier)
                             _   <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError("Couldn't commit transaction", Some(err)))
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
      found          <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError(err => InternalError(s"Couldn't fetch $key for update on $collectionName", Some(err)))
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
                            _           <- ZIO.attemptBlocking(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't update $key into $collectionName", Some(err)))
                          } yield ()
                        }
    } yield mayBeDocAfter
  }

  /** @inheritdoc */
  override def upsertOverwrite[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- reentrantLock.withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- upsertOverwriteLogic(txn, collectionDbi, colName, key, document)
                             _ <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError("Couldn't commit transaction", Some(err)))
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
      valueBuffer <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for encoded value", Some(err)))
      _           <- ZIO.attemptBlocking(valueBuffer.put(docBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
      _           <- ZIO.attemptBlocking(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't upsertOverwrite $key into $colName", Some(err)))
    } yield ()
  }

  /** @inheritdoc */
  override def upsert[K, T](colName: CollectionName, key: K, modifier: Option[T] => T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, T] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- reentrantLock.withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             res <- upsertLogic(txn, collectionDbi, colName, key, modifier)
                             _   <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError("Couldn't commit transaction", Some(err)))
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
      found          <- ZIO.attemptBlocking(Option(dbi.get(txn, keyBB))).mapError(err => InternalError(s"Couldn't fetch $key for upsert on $colName", Some(err)))
      mayBeRawValue  <- ZIO.foreach(found)(_ => ZIO.succeed(txn.`val`()))
      mayBeDocBefore <- ZIO.foreach(mayBeRawValue) { rawValue =>
                          ZIO.fromEither(codec.decode(rawValue)).mapError[UpsertErrors](msg => CodecFailure(msg))
                        }
      docAfter        = modifier(mayBeDocBefore)
      docBytes        = codec.encode(docAfter)
      valueBuffer    <- ZIO.attemptBlocking(ByteBuffer.allocateDirect(docBytes.size)).mapError(err => InternalError("Couldn't allocate byte buffer for encoded value", Some(err)))
      _              <- ZIO.attemptBlocking(valueBuffer.put(docBytes).flip).mapError(err => InternalError("Couldn't copy value bytes to buffer", Some(err)))
      _              <- ZIO.attemptBlocking(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't upsert $key into $colName", Some(err)))
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
      collected     <- ZIO.scoped {
                         for {
                           txn <- ZIO.acquireRelease(
                                    ZIO
                                      .attemptBlocking(env.txnRead())
                                      .mapError[CollectErrors](err => InternalError(s"Couldn't acquire read transaction on $colName", Some(err)))
                                  )(txn =>
                                    ZIO
                                      .attemptBlocking(txn.close())
                                      .ignoreLogged
                                  )
                           res <- collectLogic(txn, collectionDbi, colName, keyFilter, valueFilter, startAfter, backward, limit)
                         } yield res
                       }
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

  /** @inheritdoc */
  override def stream[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
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
        stream <- streamLogic(db)
      } yield stream

    ZStream.unwrapScoped(result) // TODO not sure this is the good way ???
  }

  /** @inheritdoc */
  override def streamWithKeys[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
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
        stream <- streamLogic(db)
      } yield stream

    ZStream.unwrapScoped(result) // TODO not sure this is the good way ???
  }

  /** Gets or opens an index DBI handle. */
  private def getIndexDbi(name: IndexName, txn: Option[Txn[ByteBuffer]] = None): IO[IndexNotFound, Dbi[ByteBuffer]] = {
    val logic = for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      dbi                  <- ZIO
                                .fromOption(openedCollectionDbis.get(name))
                                .orElse {
                                  for {
                                    newDbi <- txn match {
                                                case Some(t) =>
                                                  ZIO.attempt(env.openDbi(t, name.getBytes(StandardCharsets.UTF_8), null, false, DbiFlags.MDB_DUPSORT))
                                                case None    => ZIO.attemptBlocking(env.openDbi(name, DbiFlags.MDB_DUPSORT))
                                              }
                                    _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                  } yield newDbi
                                }
    } yield dbi

    reentrantLock.withWriteLock(logic).mapError(_ => IndexNotFound(name))
  }

  /** Internal logic to create an index. */
  private def indexCreateLogic(name: IndexName): ZIO[Any, StorageSystemError, Unit] = reentrantLock.withWriteLock {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      _                    <- ZIO.when(!openedCollectionDbis.contains(name)) {
                                for {
                                  newDbi <- ZIO
                                              .attemptBlocking(env.openDbi(name, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT))
                                              .mapError(err => InternalError(s"Couldn't create Index $name", Some(err)))
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

  /** @inheritdoc */
  override def indexes(): IO[IndexErrors, List[IndexName]] = {
    collectionsAvailable().mapError(e => e)
  }

  /** @inheritdoc */
  override def index[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Unit] = {
    for {
      dbi <- getIndexDbi(name)
      _   <- reentrantLock.withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   _ <- indexLogic(txn, dbi, name, key, targetKey)
                   _ <- ZIO.attemptBlocking(txn.commit()).mapError(err => InternalError("Couldn't commit index transaction", Some(err)))
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
                       .attemptBlocking(dbi.put(txn, keyBuffer, valueBuffer))
                       .mapError(err => InternalError(s"Couldn't index $key -> $targetKey in $name", Some(err)))
    } yield ()
  }

  /** @inheritdoc */
  override def indexContains[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attemptBlocking(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name", Some(err)))
                        )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
                 res <- indexContainsLogic(txn, dbi, name, key, targetKey)
               } yield res
             }
    } yield res
  }

  /** @inheritdoc */
  override def indexHasKey[FROM_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attemptBlocking(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name", Some(err)))
                        )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
                 res <- indexHasKeyLogic(txn, dbi, name, key)
               } yield res
             }
    } yield res
  }

  private def indexHasKeyLogic[FROM_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): ZIO[Scope, IndexErrors, Boolean] = {
    for {
      keyBuffer <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursor    <- ZIO.acquireRelease(
                     ZIO.attemptBlocking(dbi.openCursor(txn)).mapError(e => InternalError("Cursor error", Some(e)))
                   )(c => ZIO.attemptBlocking(c.close()).ignoreLogged)
      found     <- ZIO
                     .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                     .mapError(e => InternalError("Get error", Some(e)))
    } yield found
  }

  private def indexSeek[FROM_KEY, TO_KEY](name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      db     <- getIndexDbi(name).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
      result <- ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attemptBlocking(env.txnRead())
                               .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $name", Some(err)))
                           )(txn =>
                             ZIO
                               .attemptBlocking(txn.close())
                               .ignoreLogged
                           )
                    res <- indexSeekLogic(txn, db, name, recordKey, seekOperation)(keyCodec, toKeyCodec)
                  } yield res
                }
    } yield result
  }

  private def indexSeekLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      cursor      <- ZIO.acquireRelease(
                       ZIO
                         .attemptBlocking(dbi.openCursor(txn))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $name", Some(err)))
                     )(cursor =>
                       ZIO
                         .attemptBlocking(cursor.close())
                         .ignoreLogged
                     )
      key         <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e })
      _           <- ZIO.foreachDiscard(key) { k =>
                       ZIO
                         .attempt(cursor.get(k, GetOp.MDB_SET))
                         .mapError[FetchErrors](err => InternalError(s"Couldn't set cursor at $recordKey for $name", Some(err)))
                     }
      seekSuccess <- ZIO
                       .attempt(cursor.seek(seekOperation))
                       .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name", Some(err)))
      seekedKey   <- ZIO
                       .fromEither(keyCodec.decode(cursor.key()))
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get key at cursor for $name", None))
      valBuffer   <- ZIO
                       .attempt(cursor.`val`())
                       .when(seekSuccess)
                       .mapError[FetchErrors](err => InternalError(s"Couldn't get value at cursor for stored $name", Some(err)))
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

  /** @inheritdoc */
  override def unindex[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- reentrantLock.withWriteLock(
               withWriteTransaction(name) { txn =>
                 for {
                   res <- unindexLogic(txn, dbi, name, key, targetKey)
                   _   <- ZIO.attemptBlocking(txn.commit()).mapError(e => InternalError("Commit error", Some(e)))
                 } yield res
               }
             )
    } yield res
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
                       .attemptBlocking(dbi.delete(txn, keyBuffer, valueBuffer))
                       .mapError(e => InternalError("Delete error", Some(e)))
    } yield deleted
  }

  /** @inheritdoc */
  override def indexed[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZStream[Any, IndexErrors, TO_KEY] = {
    ZStream.unwrapScoped {
      for {
        db <- getIndexDbi(name)

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

  /** @inheritdoc */
  override def readOnly[R, E, A](f: LMDBReadOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] = {
    ZIO.scoped(
      for {
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attemptBlocking(env.txnRead())
                   .mapError(err => InternalError("Couldn't acquire read transaction", Some(err)))
               )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
        ops  = new LMDBReadOpsLive(txn)
        res <- f(ops)
      } yield res
    )
  }

  /** @inheritdoc */
  override def readWrite[R, E, A](f: LMDBWriteOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError, A] = {
    reentrantLock.withWriteLock(
      ZIO.scoped(
        for {
          txn <- ZIO.acquireRelease(
                   ZIO
                     .attemptBlocking(env.txnWrite())
                     .mapError(err => InternalError("Couldn't acquire write transaction", Some(err)))
                 )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
          ops  = new LMDBWriteOpsLive(txn)
          res <- f(ops)
          _   <- ZIO
                   .attemptBlocking(txn.commit())
                   .mapError(err => InternalError("Couldn't commit transaction", Some(err)))
        } yield res
      )
    )
  }

  /** Live implementation of read-only operations using a shared transaction. */
  private class LMDBReadOpsLive(txn: Txn[ByteBuffer]) extends LMDBReadOps {

    /** @inheritdoc */
    override def collectionExists(name: CollectionName): IO[StorageSystemError, Boolean] = LMDBLive.this.collectionExists(name)

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
    override def indexExists(name: IndexName): IO[IndexErrors, Boolean] = LMDBLive.this.indexExists(name)

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
  }
}

object LMDBLive {

  private def lmdbCreateEnv(config: LMDBConfig, databasePath: File) = {
    val syncFlag = if (!config.fileSystemSynchronized) Some(EnvFlags.MDB_NOSYNC) else None

    val flags = Array(
      EnvFlags.MDB_NOTLS
        // MDB_NOLOCK : the caller must enforce single-writer semantics
        // MDB_NOLOCK : the caller must ensure that no readers are using old transactions while a writer is active
        // EnvFlags.MDB_NOLOCK // Locks managed using ZIO ReentrantLock
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
      reentrantLock        <- TReentrantLock.make.commit
    } yield new LMDBLive(environment, openedCollectionDbis, reentrantLock, databasePath.toString)
  }
}
