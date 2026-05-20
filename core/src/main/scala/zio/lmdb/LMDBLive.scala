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
import org.lmdbjava.{Cursor, Dbi, DbiFlags, Env, EnvFlags, KeyRange, PutFlags, Txn, Verifier}
import org.lmdbjava.SeekOp._
import org.lmdbjava.CursorIterable.KeyVal
import org.lmdbjava.GetOp
import org.lmdbjava.SeekOp

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import zio.lmdb.json._
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
  activeWriteTransactionRef: FiberRef[Option[ActiveTransaction]],
  writeExecutor: Executor,
  readSemaphore: Semaphore,
  readExecutor: Executor,
  /** @inheritdoc */
  val databasePath: String,
  val config: LMDBConfig
) extends LMDB {

  private[lmdb] def initializeMetadata(): ZIO[Any, StorageSystemError, Unit] = {
    collectionCreateLogic(config.metaDataCollectionName)
  }

  /** Eagerly open every collection / index / multi recorded in the metadata collection. After this returns, the DBI cache reflects everything on disk and the `Some(txn)` fail-fast branch of `getCollectionDbi` & friends will only fire for
    * genuinely-missing names.
    *
    * Without this, the first touch of an existing-on-disk collection inside a `readOnly { ops => ... }` block would have to open its DBI inside that read transaction — an unsafe pattern (see `withExclusiveDbiOpen` for the full explanation) that was
    * the source of the production `Assertion 'root > 1' failed in mdb_page_search()` SIGABRT.
    */
  private[lmdb] def openAllKnownDbis(): ZIO[Any, StorageSystemError, Unit] = {
    collect[String, MetaDataEntry](config.metaDataCollectionName, limit = None)
      .catchAll(_ => ZIO.succeed(Nil))
      .flatMap { metas =>
        ZIO.foreachDiscard(metas) { meta =>
          meta.collectionKind match {
            case CollectionKind.Regular => getCollectionDbi(meta.collectionName).ignore
            case CollectionKind.Index   => getIndexDbi(meta.collectionName).ignore
            case CollectionKind.Multi   => getMultiDbi(meta.collectionName).ignore
          }
        }
      }
  }

  private def metadataUpdate(name: String, kind: CollectionKind): ZIO[Any, StorageSystemError, Unit] = {
    val entry = MetaDataEntry(name, kind, None, None)
    upsertOverwrite(config.metaDataCollectionName, name, entry)
      .mapError {
        case e: StorageSystemError => e
        case e: StorageUserError   => InternalError(s"Metadata update failed for $name: $e")
      }
      .when(name != config.metaDataCollectionName)
      .unit
  }

  private def metadataRemove(name: String): ZIO[Any, StorageSystemError, Unit] = {
    delete[String, MetaDataEntry](config.metaDataCollectionName, name)
      .mapError {
        case e: StorageSystemError => e
        case e: StorageUserError   => InternalError(s"Metadata removal failed for $name: $e")
      }
      .when(name != config.metaDataCollectionName)
      .unit
  }

  private def withWriteLock[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.scoped(writeMutex.withPermit(effect)).onExecutor(writeExecutor)

  /** Drain all in-flight readers, then acquire the write mutex, then run `effect`. Use only around code paths that open a new LMDB DBI handle.
    *
    * Background: a `Dbi` opened in one txn becomes visible to other txns via the env's `me_dbs[]`, but each txn snapshots its `mt_dbs[]` at begin time. If a read transaction A has started, and a new DBI is opened after A's snapshot, A's `mt_dbs[]`
    * slot for the new DBI is still zero-filled. Sharing the new `Dbi` handle across fibers (via our process-wide cache) lets A pick it up — and then a cursor on it fails the `root > 1` assertion inside `mdb_page_search`, since `md_root` is zero in
    * A's stale slot. Draining all readers before the open guarantees that every read txn started after the open sees the DBI in its snapshot, eliminating the race.
    */
  private def withExclusiveDbiOpen[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    readSemaphore.withPermits(config.maxConcurrentReaders.toLong)(withWriteLock(effect))

  /** Gate around every read-side LMDB operation.
    *
    * Even though LMDB's MVCC lets readers run in parallel, in practice we must bound the number of *concurrent* read transactions and keep them on a small, dedicated thread pool. Without that, a fan-out of fibers each calling `env.txnRead()` +
    * `openCursor` + `cursor.seek` on a different ZScheduler worker has been observed to:
    *
    *   - exhaust the LMDB reader table (`ReadersFullException`), and
    *   - under heavy host load, segfault in `mdb_page_search` because the cursor's C struct is freed while a sibling worker is still inside the JNI `mdb_cursor_get` call.
    *
    * The semaphore bounds parallelism; `onExecutor(readExecutor)` pins the work to a small fixed pool so JIT-compiled JNI stubs see a stable, bounded set of threads.
    */
  private def withReadLock[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    readSemaphore.withPermit(effect).onExecutor(readExecutor)

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

  /** Gets a cached collection DBI handle, or opens it if no transaction is already in flight on this fiber.
    *
    * SAFETY: we deliberately refuse to open a new DBI when called from inside an existing transaction (`txn = Some(_)`). Opening a DBI inside a read transaction creates a handle that is private to that txn (LMDB does not commit it to `me_dbs[]`), so
    * caching it process-wide for other txns to pick up is undefined behavior — and in practice triggers a `root > 1` assertion failure (SIGABRT) in `mdb_page_search`. With eager-open at setup, this fail-fast branch should never fire for collections
    * that exist on disk; if it does, the collection genuinely does not exist.
    */
  private def getCollectionDbi(name: CollectionName, txn: Option[Txn[ByteBuffer]] = None): IO[CollectionNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None =>
          txn match {
            case Some(_) =>
              ZIO.fail(CollectionNotFound(name))
            case None =>
              withExclusiveDbiOpen {
                openedCollectionDbisRef.get.flatMap { openedAgain =>
                  openedAgain.get(name) match {
                    case Some(alreadyOpened) => ZIO.succeed(alreadyOpened)
                    case None =>
                      for {
                        newDbi <- ZIO.attempt(env.openDbi(name))
                        _ <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                      } yield newDbi
                  }
                }
              }
          }
      }
    }
  }.orElseFail(CollectionNotFound(name))

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
      count         <- withReadLock(withReadTransaction(name) { txn =>
                         collectionSizeLogic(txn, collectionDbi, name)
                       })
    } yield count
  }

  private def collectionSizeLogic(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: CollectionName): IO[SizeErrors, Long] = {
    ZIO
      .attempt(dbi.stat(txn))
      .mapError(err => InternalError(s"Couldn't get $name size: $err", Some(err)))
      .map(_.entries)
  }

  /** @inheritdoc */
  override def stats(): IO[StorageSystemError, LMDBStats] = {
    for {
      info               <- ZIO.attemptBlocking(env.info()).mapError(err => InternalError(s"Couldn't get env info: $err", Some(err)))
      stat               <- ZIO.attemptBlocking(env.stat()).mapError(err => InternalError(s"Couldn't get env stat: $err", Some(err)))
      metas              <- collect[String, MetaDataEntry](config.metaDataCollectionName, limit = None).catchAll(_ => ZIO.succeed(Nil))
      numCollections      = metas.count(_.collectionKind == CollectionKind.Regular)
      numIndexes          = metas.count(_.collectionKind == CollectionKind.Index)
      numMultiCollections = metas.count(_.collectionKind == CollectionKind.Multi)
    } yield LMDBStats(
      databasePath = databasePath,
      mapSize = info.mapSize,
      lastPageNumber = info.lastPageNumber,
      lastTransactionId = info.lastTransactionId,
      maxReaders = info.maxReaders,
      numReaders = info.numReaders,
      numCollections = numCollections,
      numIndexes = numIndexes,
      numMultis = numMultiCollections,
      envStats = LMDBEnvStats(
        pageSize = stat.pageSize,
        depth = stat.depth,
        branchPages = stat.branchPages,
        leafPages = stat.leafPages,
        overflowPages = stat.overflowPages,
        entries = stat.entries
      )
    )
  }

  /** @inheritdoc */
  override def collectionAllocate(name: CollectionName): IO[CreateErrors, Unit] = {
    for {
      exists <- collectionExists(name)
      _      <- ZIO.cond[CollectionAlreadExists, Unit](!exists, (), CollectionAlreadExists(name))
      _      <- collectionCreateLogic(name)
      _      <- metadataUpdate(name, CollectionKind.Regular).mapError(e => e: CreateErrors)
    } yield ()
  }

  /** @inheritdoc */
  override def collectionCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CreateErrors, LMDBCollection[K, T]] = {
    val allocateLogic = if (failIfExists) {
      collectionAllocate(name)
    } else {
      collectionAllocate(name).catchSome { case CollectionAlreadExists(_) =>
        metadataUpdate(name, CollectionKind.Regular).mapError(e => e: CreateErrors) *>
          getCollectionDbi(name).ignore
      }
    }
    allocateLogic.as(LMDBCollection[K, T](name, this))
  }

  /** Internal logic to create a collection. */
  private def collectionCreateLogic(name: CollectionName): ZIO[Any, StorageSystemError, Unit] = withExclusiveDbiOpen {
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

  /** Scoped read transaction. The native txn-begin / txn-close calls run on the blocking pool (off the ZIO compute pool) because they are JNI calls into LMDB; making them `attemptBlocking` keeps the native side from starving the compute pool and
    * prevents fiber-interruption from running a release between two cursor operations on the same txn.
    */
  private def withReadTransaction(colName: CollectionName): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO
        .attemptBlocking(env.txnRead())
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
      _             <- metadataRemove(colName).mapError(e => e: DropErrors)
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
                                 .getDbiNames
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
      result <- withReadLock(withReadTransaction(colName) { txn =>
                  fetchLogic(txn, db, colName, key)
                })
    } yield result
  }

  private def fetchLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Any, FetchErrors, Option[T]] = {
    for {
      keyBB         <- makeKeyByteBuffer(key)
      found         <- ZIO.attempt(Option(dbi.get(txn, keyBB))).mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName: $err", Some(err)))
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
                               .attempt(txn.close())
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
    // Same fused-JNI rationale as `indexSeekLogic` above. The walk itself is
    // O(index), but it stays on a single blocking-pool thread so no
    // interrupt-driven scope teardown can occur mid-walk.
    for {
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor  = dbi.openCursor(txn)
                                var i       = 0L
                                var ok      = cursor.seek(SeekOp.MDB_FIRST)
                                while (i < index && ok) {
                                  ok = cursor.seek(SeekOp.MDB_NEXT)
                                  i += 1
                                }
                                val success = i == index && ok
                                val decoded =
                                  if (success) Some((kodec.decode(cursor.key()), codec.decode(cursor.`val`())))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
      result           <- cursorWithResult._2 match {
                            case None                       => ZIO.none
                            case Some((Left(_), _))         => ZIO.fail(InternalError(s"Couldn't decode key at cursor for $colName", None): FetchErrors)
                            case Some((_, Left(err)))       => ZIO.fail(CodecFailure(err): FetchErrors)
                            case Some((Right(k), Right(v))) => ZIO.some(k -> v)
                          }
    } yield result
  }

  private def indexFetchLogic[FROM_KEY, TO_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, FetchErrors, Option[TO_KEY]] = {
    // Same fused-JNI rationale as `indexSeekLogic` above.
    for {
      keyBuffer        <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor  = dbi.openCursor(txn)
                                val found   = cursor.get(keyBuffer, GetOp.MDB_SET)
                                val decoded =
                                  if (found) Some(toKeyCodec.decode(cursor.`val`()))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
      result           <- cursorWithResult._2 match {
                            case None            => ZIO.none
                            case Some(Left(err)) => ZIO.fail(CodecFailure(err): FetchErrors)
                            case Some(Right(v))  => ZIO.some(v)
                          }
    } yield result
  }

  private def indexFetchAtLogic[FROM_KEY, TO_KEY](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    name: IndexName,
    position: Long
  )(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): ZIO[Scope, FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    // Same fused-JNI rationale as `indexSeekLogic` above.
    for {
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor  = dbi.openCursor(txn)
                                var i       = 0L
                                var ok      = cursor.seek(SeekOp.MDB_FIRST)
                                while (i < position && ok) {
                                  ok = cursor.seek(SeekOp.MDB_NEXT)
                                  i += 1
                                }
                                val success = i == position && ok
                                val decoded =
                                  if (success) Some((keyCodec.decode(cursor.key()), toKeyCodec.decode(cursor.`val`())))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't acquire iterable on $name: $err", Some(err)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
      result           <- cursorWithResult._2 match {
                            case None                       => ZIO.none
                            case Some((Left(_), _))         => ZIO.fail(InternalError(s"Couldn't decode key at cursor for $name", None): FetchErrors)
                            case Some((_, Left(err)))       => ZIO.fail(CodecFailure(err): FetchErrors)
                            case Some((Right(k), Right(v))) => ZIO.some(k -> v)
                          }
    } yield result
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
                               .attempt(txn.close())
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
    // Same fused-JNI rationale as `indexSeekLogic` further below.
    for {
      keyBB            <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk))
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor  = dbi.openCursor(txn)
                                keyBB.foreach(k => cursor.get(k, GetOp.MDB_SET))
                                val success = cursor.seek(seekOperation)
                                val decoded =
                                  if (success) Some((kodec.decode(cursor.key()), codec.decode(cursor.`val`())))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName: $err", Some(err)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
      result           <- cursorWithResult._2 match {
                            case None                       => ZIO.none
                            case Some((Left(_), _))         => ZIO.fail(InternalError(s"Couldn't decode key at cursor for $colName", None): FetchErrors)
                            case Some((_, Left(err)))       => ZIO.fail(CodecFailure(err): FetchErrors)
                            case Some((Right(k), Right(v))) => ZIO.some(k -> v)
                          }
    } yield result
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
      result <- withReadLock(withReadTransaction(colName) { txn =>
                  containsLogic(txn, db, colName, key)
                })
    } yield result
  }

  private def containsLogic[K](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): ZIO[Any, ContainsErrors, Boolean] = {
    for {
      keyBB <- makeKeyByteBuffer(key)
      found <- ZIO.attempt(Option(dbi.get(txn, keyBB))).mapError[ContainsErrors](err => InternalError(s"Couldn't check $key on $colName: $err", Some(err)))
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
                            valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
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
      valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      _           <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't upsertOverwrite $key into $colName: $err", Some(err)))
    } yield ()
  }

  /** @inheritdoc */
  override def insert[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[InsertErrors, Unit] = {
    for {
      collectionDbi <- getCollectionDbi(colName)
      result        <- withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- insertLogic(txn, collectionDbi, colName, key, document)
                             _ <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield ()
                         }
                       )
    } yield result
  }

  /** logic for inserting a record, failing if the key already exists
    * @param txn
    *   transaction
    * @param dbi
    *   database handle
    * @param colName
    *   collection name
    * @param key
    *   key to insert
    * @param document
    *   record content
    */
  private def insertLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[InsertErrors, Unit] = {
    for {
      keyBB       <- makeKeyByteBuffer(key)
      docBytes     = codec.encode(document)
      valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      inserted    <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer, PutFlags.MDB_NOOVERWRITE)).mapError(err => InternalError(s"Couldn't insert $key into $colName: $err", Some(err)))
      _           <- ZIO.unless(inserted)(ZIO.fail(KeyAlreadyExists(colName, key.toString): InsertErrors))
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
      valueBuffer    <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
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
    limit: Option[Long] = None
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
                                      .attempt(txn.close())
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
    limit: Option[Long] = None
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, CollectErrors, List[T]] = {
    for {
      startAfterBB <- ZIO.foreach(startAfter)(makeKeyByteBuffer)
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError[CollectErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attemptBlocking(cursor.close())
                          .ignoreLogged
                      )
      collected    <- ZIO
                        .attempt {
                          def content =
                            LazyList
                              .from(KeyValueIterator[K, T](iterable.iterator()))
                              .map(kv => kv.key.flatMap(key => kv.value.map(value => (key, value))))
                              .collect { case either if either.isLeft || either.exists((k, v) => keyFilter(k) && valueFilter(v)) => either.map((k, v) => v) }
                          val limited = limit match {
                            case None    => content.toList
                            case Some(l) => content.take(l.toInt).toList
                          }
                          limited
                        }
                        .flatMap { r => ZIO.foreach(r)(ZIO.from(_)) }
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

  private case class KeyValueIterator[K, T](jiterator: java.util.Iterator[KeyVal[ByteBuffer]])(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]) extends Iterator[KeyValue[K, T]] {

    private def extractKeyVal[LK, LT](keyval: KeyVal[ByteBuffer])(implicit kodec: KeyCodec[LK], codec: LMDBCodec[LT]): KeyValue[LK, LT] = {
      val key   = keyval.key()
      val value = keyval.`val`()
      KeyValue(kodec.decode(key), codec.decode(value))
    }

    override def hasNext: Boolean = jiterator.hasNext

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
    val result =
      for {
        db  <- getCollectionDbi(colName)
        _   <- readSemaphore.withPermitScoped
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

    ZStream.unwrapScoped(result).onExecutor(readExecutor)
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
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
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

  /** @inheritdoc */
  override def streamWithKeys[K, T](
    colName: CollectionName,
    keyFilter: K => Boolean = (_: K) => true,
    startAfter: Option[K] = None,
    backward: Boolean = false
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
    val result =
      for {
        db  <- getCollectionDbi(colName)
        _   <- readSemaphore.withPermitScoped
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

    ZStream.unwrapScoped(result).onExecutor(readExecutor)
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
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
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

  /** Gets a cached index DBI handle, or opens it if no transaction is in flight on this fiber. See `getCollectionDbi` for the safety contract.
    */
  private def getIndexDbi(name: IndexName, txn: Option[Txn[ByteBuffer]] = None): IO[IndexNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None =>
          txn match {
            case Some(_) =>
              ZIO.fail(IndexNotFound(name))
            case None =>
              withExclusiveDbiOpen {
                openedCollectionDbisRef.get.flatMap { openedAgain =>
                  openedAgain.get(name) match {
                    case Some(alreadyOpened) => ZIO.succeed(alreadyOpened)
                    case None =>
                      for {
                        newDbi <- ZIO.attempt(env.openDbi(name, DbiFlags.MDB_DUPSORT))
                        _ <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                      } yield newDbi
                  }
                }
              }
          }
      }
    }
  }.orElseFail(IndexNotFound(name))

  /** Internal logic to create an index. */
  private def indexCreateLogic(name: IndexName): ZIO[Any, StorageSystemError, Unit] = withExclusiveDbiOpen {
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
      _      <- metadataUpdate(name, CollectionKind.Index).mapError(e => e: IndexErrors)
    } yield ()
  }

  /** @inheritdoc */
  override def indexCreate[FROM_KEY, TO_KEY](name: IndexName, failIfExists: Boolean)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, LMDBIndex[FROM_KEY, TO_KEY]] = {
    val allocateLogic = if (failIfExists) {
      indexAllocate(name)
    } else {
      indexAllocate(name).catchSome { case IndexAlreadyExists(_) =>
        metadataUpdate(name, CollectionKind.Index).mapError(e => e: IndexErrors) *>
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
      _   <- metadataRemove(name).mapError(e => e: IndexErrors)
    } yield ()
  }

  /** @inheritdoc */
  override def indexes(): IO[IndexErrors, List[IndexName]] = {
    for {
      metas <- collect[String, MetaDataEntry](config.metaDataCollectionName, limit = None).mapError {
                 case e: OverSizedKey       => e: IndexErrors
                 case e: CollectionNotFound => InternalError(s"Metadata collection not found: ${config.metaDataCollectionName}", Some(new RuntimeException(e.toString))): IndexErrors
                 case e: CodecFailure       => e: IndexErrors
                 case e: StorageSystemError => e: IndexErrors
               }
    } yield metas.filter(_.collectionKind == CollectionKind.Index).map(_.collectionName)
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
    // Same fused-JNI rationale as `indexSeekLogic`.
    for {
      keyBuffer        <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor = dbi.openCursor(txn)
                                val found  = cursor.get(keyBuffer, GetOp.MDB_SET)
                                (cursor, found)
                              }
                              .mapError(e => InternalError(s"Cursor error: $e", Some(e)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
    } yield cursorWithResult._2
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
                               .attempt(txn.close())
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
    // Fuse open-cursor -> get -> seek -> key/val into a single
    // attemptBlocking so the entire JNI sequence runs on one blocking-pool
    // thread, atomically. Without this, fiber interruption between two
    // consecutive `attemptBlocking` calls can fire the cursor's scope
    // finalizer (cursor.close + txn.close) — freeing the txn's `mt_dbxs[]`
    // — while the next blocking-pool worker still holds the cursor for the
    // following JNI call. That is the use-after-free observed at
    // `mdb_page_search+0x53` in sotohp.
    for {
      keyBB            <- ZIO.foreach(recordKey)(rk => makeKeyByteBuffer(rk).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e })
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor      = dbi.openCursor(txn)
                                keyBB.foreach(k => cursor.get(k, GetOp.MDB_SET))
                                val seekSuccess = cursor.seek(seekOperation)
                                if (seekSuccess) {
                                  val k    = cursor.key()
                                  val v    = cursor.`val`()
                                  val dkey = keyCodec.decode(k)
                                  val dval = toKeyCodec.decode(v)
                                  (cursor, Some((dkey, dval)))
                                } else (cursor, None)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name: $err", Some(err)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
      result           <- ZIO.fromEither(
                            cursorWithResult._2 match {
                              case None           => Right(None)
                              case Some((dk, dv)) =>
                                for {
                                  k <- dk.left.map(e => InternalError(s"Couldn't decode key for $name: $e", None): FetchErrors)
                                  v <- dv.left.map(e => CodecFailure(e): FetchErrors)
                                } yield Some(k -> v)
                            }
                          )
    } yield result
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
                               .attempt(txn.close())
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
                               .attempt(txn.close())
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
    // Same fused-JNI rationale as `indexSeekLogic`.
    for {
      keyBuffer        <- makeKeyByteBuffer(key)(keyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      valueBuffer      <- makeKeyByteBuffer(targetKey)(toKeyCodec).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attemptBlocking {
                                val cursor               = dbi.openCursor(txn)
                                @scala.annotation.tailrec
                                def findValue(): Boolean = {
                                  if (cursor.`val`().compareTo(valueBuffer) == 0) true
                                  else if (cursor.seek(SeekOp.MDB_NEXT_DUP)) findValue()
                                  else false
                                }
                                val found                =
                                  if (cursor.get(keyBuffer, GetOp.MDB_SET)) findValue()
                                  else false
                                (cursor, found)
                              }
                              .mapError(e => InternalError(s"Cursor error: $e", Some(e)))
                          )(cw => ZIO.attemptBlocking(cw._1.close()).ignoreLogged)
    } yield cursorWithResult._2
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
    ZStream
      .unwrapScoped {
        for {
          db  <- getIndexDbi(name)
          _   <- readSemaphore.withPermitScoped
          txn <- ZIO.acquireRelease(
                   ZIO
                     .attemptBlocking(env.txnRead())
                     .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                 )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
          s   <- indexedLogic(txn, db, name, key, limitToKey)(keyCodec, toKeyCodec)
        } yield s
      }
      .onExecutor(readExecutor)
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

  /** Gets a cached multi-collection DBI handle, or opens it if no transaction is in flight on this fiber. See `getCollectionDbi` for the safety contract.
    */
  private def getMultiDbi(name: CollectionName, txn: Option[Txn[ByteBuffer]] = None): IO[CollectionNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None =>
          txn match {
            case Some(_) =>
              ZIO.fail(CollectionNotFound(name))
            case None =>
              withExclusiveDbiOpen {
                openedCollectionDbisRef.get.flatMap { openedAgain =>
                  openedAgain.get(name) match {
                    case Some(alreadyOpened) => ZIO.succeed(alreadyOpened)
                    case None =>
                      for {
                        newDbi <- ZIO.attempt(env.openDbi(name, DbiFlags.MDB_DUPSORT))
                        _ <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                      } yield newDbi
                  }
                }
              }
          }
      }
    }
  }.orElseFail(CollectionNotFound(name))

  private def multiCreateLogic(name: CollectionName): ZIO[Any, StorageSystemError, Unit] = withExclusiveDbiOpen {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      _                    <- ZIO.when(!openedCollectionDbis.contains(name)) {
                                for {
                                  newDbi <- ZIO
                                              .attempt(env.openDbi(name, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT))
                                              .mapError(err => InternalError(s"Couldn't create MultiCollection $name: $err", Some(err)))
                                  _      <- openedCollectionDbisRef.update(_ + (name -> newDbi))
                                } yield ()
                              }
    } yield ()
  }

  private def multiAllocate(name: CollectionName): IO[CreateErrors, Unit] = {
    for {
      exists <- multiExists(name)
      _      <- ZIO.cond[CollectionAlreadExists, Unit](!exists, (), CollectionAlreadExists(name))
      _      <- multiCreateLogic(name)
      _      <- metadataUpdate(name, CollectionKind.Multi).mapError(e => e: CreateErrors)
    } yield ()
  }

  override def multiCreate[K, T](name: CollectionName, failIfExists: Boolean = true)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[CreateErrors, LMDBMulti[K, T]] = {
    val allocateLogic = if (failIfExists) {
      multiAllocate(name)
    } else {
      multiAllocate(name).catchSome { case CollectionAlreadExists(_) =>
        metadataUpdate(name, CollectionKind.Multi).mapError(e => e: CreateErrors) *>
          getMultiDbi(name).ignore
      }
    }
    allocateLogic.as(LMDBMulti[K, T](name, this))
  }

  override def multiGet[K, T](name: CollectionName)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[GetErrors, LMDBMulti[K, T]] = {
    for {
      exists <- multiExists(name)
      _      <- ZIO.cond[CollectionNotFound, Unit](exists, (), CollectionNotFound(name))
    } yield LMDBMulti[K, T](name, this)
  }

  override def multiExists(name: CollectionName): IO[StorageSystemError, Boolean] = {
    for {
      openedCollectionDbis <- openedCollectionDbisRef.get
      found                <- if (openedCollectionDbis.contains(name)) ZIO.succeed(true)
                              else collectionsAvailable().map(_.contains(name))
    } yield found
  }

  override def multiSize(name: CollectionName): IO[SizeErrors, Long] = {
    for {
      collectionDbi <- getMultiDbi(name)
      count         <- withReadLock(withReadTransaction(name) { txn =>
                         collectionSizeLogic(txn, collectionDbi, name)
                       })
    } yield count
  }

  override def multiClear(name: CollectionName): IO[ClearErrors, Unit] = {
    for {
      collectionDbi <- getMultiDbi(name)
      _             <- withWriteLock(
                         withWriteTransaction(name) { txn =>
                           for {
                             _ <- collectionClearLogic(txn, collectionDbi, name)
                             _ <- ZIO.attempt(txn.commit()).mapError[ClearErrors](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield ()
                         }
                       )
    } yield ()
  }

  override def multiDrop(name: CollectionName): IO[DropErrors, Unit] = {
    for {
      collectionDbi <- getMultiDbi(name)
      _             <- collectionClearOrDropLogic(collectionDbi, name, true)
      _             <- openedCollectionDbisRef.updateAndGet(_.removed(name))
      _             <- metadataRemove(name).mapError(e => e: DropErrors)
    } yield ()
  }

  private def multiFetchLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): ZIO[Scope, FetchErrors, List[T]] = {
    for {
      keyBuffer <- makeKeyByteBuffer(key).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e }
      cursor    <- ZIO.acquireRelease(
                     ZIO.attemptBlocking(dbi.openCursor(txn)).mapError[FetchErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                   )(c => ZIO.attemptBlocking(c.close()).ignoreLogged)
      found     <- ZIO
                     .attemptBlocking(cursor.get(keyBuffer, GetOp.MDB_SET))
                     .mapError[FetchErrors](e => InternalError(s"Get error: $e", Some(e)))
      result    <- if (found) {
                     ZIO
                       .attempt {
                         val builder = List.newBuilder[T]
                         var hasNext = true
                         while (hasNext) {
                           val valBuffer = cursor.`val`()
                           codec.decode(valBuffer) match {
                             case Right(v) => builder += v
                             case Left(e)  => () // Ignore or fail? Let's ignore for now or we could fail. Actually, we should fail if codec fails.
                           }
                           hasNext = cursor.seek(SeekOp.MDB_NEXT_DUP)
                         }
                         builder.result()
                       }
                       .mapError[FetchErrors](e => InternalError(s"Iteration error: $e", Some(e)))
                   } else ZIO.succeed(Nil)
    } yield result
  }

  override def multiFetch[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, List[T]] = {
    for {
      db  <- getMultiDbi(colName)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attemptBlocking(env.txnRead())
                            .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                        )(txn => ZIO.attemptBlocking(txn.close()).ignoreLogged)
                 res <- multiFetchLogic(txn, db, colName, key)
               } yield res
             })
    } yield res
  }

  private def multiPutLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    for {
      keyBB       <- makeKeyByteBuffer(key)
      docBytes     = codec.encode(document)
      valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      _           <- ZIO.attempt(dbi.put(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't multiPut $key into $colName: $err", Some(err)))
    } yield ()
  }

  override def multiPut[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    for {
      collectionDbi <- getMultiDbi(colName)
      _             <- withWriteLock(
                         withWriteTransaction(colName) { txn =>
                           for {
                             _ <- multiPutLogic(txn, collectionDbi, colName, key, document)
                             _ <- ZIO.attempt(txn.commit()).mapError(err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                           } yield ()
                         }
                       )
    } yield ()
  }

  private def multiDeleteLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Boolean] = {
    for {
      keyBB       <- makeKeyByteBuffer(key)
      docBytes     = codec.encode(document)
      valueBuffer <- ZIO.attempt(ByteBuffer.allocateDirect(docBytes.length)).mapError(err => InternalError(s"Couldn't allocate byte buffer for encoded value: $err", Some(err)))
      _           <- ZIO.attempt(valueBuffer.put(docBytes).flip).mapError(err => InternalError(s"Couldn't copy value bytes to buffer: $err", Some(err)))
      deleted     <- ZIO.attempt(dbi.delete(txn, keyBB, valueBuffer)).mapError(err => InternalError(s"Couldn't multiDelete $key from $colName: $err", Some(err)))
    } yield deleted
  }

  override def multiDelete[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Boolean] = {
    for {
      db  <- getMultiDbi(colName)
      res <- withWriteLock(
               withWriteTransaction(colName) { txn =>
                 for {
                   res <- multiDeleteLogic(txn, db, colName, key, document)
                   _   <- ZIO.attempt(txn.commit()).mapError[DeleteErrors](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                 } yield res
               }
             )
    } yield res
  }

  private def multiDeleteAllLogic[K](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[DeleteErrors, Boolean] = {
    for {
      keyBB   <- makeKeyByteBuffer(key)
      deleted <- ZIO.attempt(dbi.delete(txn, keyBB)).mapError(err => InternalError(s"Couldn't multiDeleteAll $key from $colName: $err", Some(err)))
    } yield deleted
  }

  override def multiDeleteAll[K](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[DeleteErrors, Boolean] = {
    for {
      db  <- getMultiDbi(colName)
      res <- withWriteLock(
               withWriteTransaction(colName) { txn =>
                 for {
                   res <- multiDeleteAllLogic(txn, db, colName, key)
                   _   <- ZIO.attempt(txn.commit()).mapError[DeleteErrors](err => InternalError(s"Couldn't commit transaction: $err", Some(err)))
                 } yield res
               }
             )
    } yield res
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
  override def readWrite[R, E, A](f: LMDBWriteOps => ZIO[R, E, A]): ZIO[R, E | StorageSystemError | StorageUserError.NestedWriteTransactionError, A] = {
    activeWriteTransactionRef.get.flatMap {
      case Some(active) => ZIO.fail(NestedWriteTransactionError(active))
      case None         =>
        Clock.currentDateTime.flatMap { now =>
          activeWriteTransactionRef.locally(Some(ActiveTransaction(now))) {
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
      limit: Option[Long]
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

    /** @inheritdoc */
    override def multiExists(name: CollectionName): IO[StorageSystemError, Boolean] = {
      getMultiDbi(name, Some(txn)).as(true).catchAll(_ => ZIO.succeed(false))
    }

    /** @inheritdoc */
    override def multiSize(name: CollectionName): IO[SizeErrors, Long] = {
      for {
        collectionDbi <- getMultiDbi(name, Some(txn))
        size          <- collectionSizeLogic(txn, collectionDbi, name)
      } yield size
    }

    /** @inheritdoc */
    override def multiFetch[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, List[T]] = {
      for {
        db  <- getMultiDbi(colName, Some(txn))
        res <- ZIO.scoped(multiFetchLogic(txn, db, colName, key))
      } yield res
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
    override def insert[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[InsertErrors, Unit] = {
      for {
        collectionDbi <- getCollectionDbi(collectionName, Some(txn))
        _             <- insertLogic(txn, collectionDbi, collectionName, key, document)
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

    /** @inheritdoc */
    override def multiClear(name: CollectionName): IO[ClearErrors, Unit] = {
      for {
        collectionDbi <- getMultiDbi(name, Some(txn))
        _             <- collectionClearLogic(txn, collectionDbi, name)
      } yield ()
    }

    /** @inheritdoc */
    override def multiPut[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
      for {
        collectionDbi <- getMultiDbi(collectionName, Some(txn))
        _             <- multiPutLogic(txn, collectionDbi, collectionName, key, document)
      } yield ()
    }

    /** @inheritdoc */
    override def multiDelete[K, T](collectionName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[DeleteErrors, Boolean] = {
      for {
        db  <- getMultiDbi(collectionName, Some(txn))
        res <- multiDeleteLogic(txn, db, collectionName, key, document)
      } yield res
    }

    /** @inheritdoc */
    override def multiDeleteAll[K](collectionName: CollectionName, key: K)(implicit kodec: KeyCodec[K]): IO[DeleteErrors, Boolean] = {
      for {
        db  <- getMultiDbi(collectionName, Some(txn))
        res <- multiDeleteAllLogic(txn, db, collectionName, key)
      } yield res
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
    require(config.maxConcurrentReaders > 0, "LMDBConfig.maxConcurrentReaders must be > 0")
    require(config.readExecutorThreads > 0, "LMDBConfig.readExecutorThreads must be > 0")
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
      readExecutorService  <- ZIO.acquireRelease(
                                ZIO.attempt(java.util.concurrent.Executors.newFixedThreadPool(config.readExecutorThreads))
                              )(es => ZIO.attempt(es.shutdown()).ignoreLogged)
      readExecutor          = Executor.fromJavaExecutor(readExecutorService)
      readSemaphore        <- Semaphore.make(config.maxConcurrentReaders.toLong)
      lmdb                  = new LMDBLive(
                                environment,
                                openedCollectionDbis,
                                writeMutex,
                                activeTransactionRef,
                                writeExecutor,
                                readSemaphore,
                                readExecutor,
                                databasePath.toString,
                                config
                              )
      _                    <- lmdb.initializeMetadata().mapError(e => new RuntimeException(s"Failed to initialize metadata: $e"))
      _                    <- lmdb.openAllKnownDbis().mapError(e => new RuntimeException(s"Failed to eager-open DBIs: $e"))
    } yield lmdb
  }
}
