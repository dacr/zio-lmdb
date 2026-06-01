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

  /** Bounded checkout/checkin pool of direct ByteBuffers sized to `env.getMaxKeySize`, used for key buffers that must outlive a single JNI call.
    *
    * Why this exists: any key buffer passed to `dbi.iterate` (via `KeyRange.greaterThan*`) or to `dbi.openCursor` + `cursor.get` is retained by lmdbjava's `CursorIterable` / `Cursor` for the entire cursor lifetime — sometimes thousands of yield
    * boundaries. That rules out the per-thread `keyScratch` ThreadLocal (which assumes single-JNI-call atomicity). Without pooling, each cursor open allocates a fresh `DirectByteBuffer`, which on a heap-constrained JVM accumulates in the Cleaner
    * backlog and bloats off-heap memory.
    *
    * Capacity policy: every buffer is allocated at `env.getMaxKeySize` (LMDB's compile-time hard cap, typically 511 bytes) — uniform size class avoids size-matching at borrow time and keeps the pool's peak footprint tiny (`poolMaxSize * maxKeySize`
    * ≈ 32 KB at default settings).
    *
    * Growth policy: unbounded `ConcurrentLinkedQueue`; we soft-cap retention at `poolMaxSize` on release (excess buffers are dropped and GC-reclaimed). Borrow on empty pool allocates fresh — bounded by simultaneous-cursor count, which is itself
    * bounded by `readSemaphore` permits + 1 writer.
    */
  private val keyBufferPoolMaxSize: Int                                             = 64
  private val keyBufferPool: java.util.concurrent.ConcurrentLinkedQueue[ByteBuffer] =
    new java.util.concurrent.ConcurrentLinkedQueue[ByteBuffer]()
  private val keyBufferPoolSize: java.util.concurrent.atomic.AtomicInteger          =
    new java.util.concurrent.atomic.AtomicInteger(0)

  private def borrowKeyBuffer(): ByteBuffer = {
    val pooled = keyBufferPool.poll()
    if (pooled != null) { keyBufferPoolSize.decrementAndGet(); pooled.clear(); pooled }
    else ByteBuffer.allocateDirect(env.getMaxKeySize.toInt)
  }

  private def releaseKeyBuffer(buf: ByteBuffer): Unit = {
    if (buf.capacity() == env.getMaxKeySize.toInt && keyBufferPoolSize.get() < keyBufferPoolMaxSize) {
      buf.clear()
      keyBufferPool.offer(buf)
      keyBufferPoolSize.incrementAndGet()
    }
    // else drop; native bytes will be reclaimed by the Cleaner on the next GC
  }

  /** Borrow a key buffer from the pool, fill it with the encoded key, and register its release as a finalizer on the current `Scope`.
    *
    * Use for anchor / `startAfter` buffers handed to `dbi.iterate` or `dbi.openCursor` — i.e. anywhere the buffer must survive past a single `ZIO.attempt` JNI block. The buffer is returned to the pool when the scope closes (after the cursor's own
    * finalizer has already released the JNI-side reference, thanks to LIFO finalizer order). For ordinary point operations, use `fillKeyScratch` / `fillValueScratch` instead — those reuse a per-thread buffer with zero allocation and zero pool
    * contention.
    */
  private def borrowKeyBufferScoped[K](id: K)(implicit kodec: KeyCodec[K]): ZIO[Scope, KeyErrors, ByteBuffer] = {
    val keyBytes: Array[Byte] = kodec.encode(id)
    if (keyBytes.length > env.getMaxKeySize)
      ZIO.fail(OverSizedKey(id.toString, keyBytes.length, env.getMaxKeySize))
    else
      ZIO.acquireRelease(
        ZIO
          .attempt {
            val buf = borrowKeyBuffer()
            buf.put(keyBytes).flip()
            buf
          }
          .mapError(err => InternalError(s"Couldn't borrow buffer for key: $err", Some(err)))
      )(buf => ZIO.succeed(releaseKeyBuffer(buf)))
  }

  /** Per-thread scratch DirectByteBuffer for keys.
    *
    * Sized once to `env.getMaxKeySize` (LMDB hard-caps key length at compile time; default 511 bytes) so every encoded key fits without reallocation. The buffer is filled and consumed inside a single `ZIO.attempt` block, so no fiber yield can
    * interleave another op on the same thread. Read and write executors are dedicated pools, so no foreign code touches this ThreadLocal.
    */
  private val keyScratch: ThreadLocal[ByteBuffer] = ThreadLocal.withInitial { () =>
    ByteBuffer.allocateDirect(env.getMaxKeySize.toInt)
  }

  /** Per-thread scratch DirectByteBuffer for DUPSORT values (where `dbi.reserve` is not allowed).
    *
    * Grows on demand and never shrinks, so steady-state allocation is zero once the largest value size has been seen on a thread.
    */
  private val valueScratch: ThreadLocal[ByteBuffer] = new ThreadLocal[ByteBuffer]

  /** Fill the per-thread key scratch buffer with `bytes` and return it positioned and flipped. */
  private def fillKeyScratch(bytes: Array[Byte]): ByteBuffer = {
    val buf = keyScratch.get()
    buf.clear()
    buf.put(bytes)
    buf.flip()
    buf
  }

  /** Fill the per-thread value scratch buffer with `bytes`, growing the buffer if needed, and return it positioned and flipped. */
  private def fillValueScratch(bytes: Array[Byte]): ByteBuffer = {
    val current = valueScratch.get()
    val buf     =
      if (current != null && current.capacity() >= bytes.length) current
      else {
        val grown = ByteBuffer.allocateDirect(bytes.length)
        valueScratch.set(grown)
        grown
      }
    buf.clear()
    buf.put(bytes)
    buf.flip()
    buf
  }

  /** Position `cursor` for a `seek` operation that may carry an anchor key, returning `true` iff the cursor lands on a valid entry.
    *
    * Why this exists: a naive `cursor.get(anchor, MDB_SET) + cursor.seek(op)` is unsafe — if `MDB_SET` returns false the cursor is in the LMDB uninitialized state, and the subsequent `cursor.seek(MDB_NEXT/MDB_PREV)` routes through `mdb_cursor_first`
    * → `mdb_page_search`, which trips the internal `mdb_cassert(mc, root > 1)` and aborts the JVM with SIGABRT. Using `MDB_SET_RANGE` (position at first key >= anchor) for ranged navigation avoids that path entirely and gives well-defined semantics
    * whether the anchor is present in the DB or not.
    *
    * Must be called inside a single `ZIO.attempt` block alongside `dbi.openCursor` — all calls here are synchronous JNI, and fusing them into one effect prevents fiber interruption between the cursor's `openCursor` and `seek` (see Round 3).
    */
  private def positionCursorForSeek(cursor: Cursor[ByteBuffer], anchor: Option[ByteBuffer], seekOp: SeekOp): Boolean = {
    (anchor, seekOp) match {
      case (None, op)                 =>
        cursor.seek(op)
      case (Some(k), SeekOp.MDB_NEXT) =>
        if (!cursor.get(k, GetOp.MDB_SET_RANGE)) false
        else if (cursor.key().equals(k)) cursor.seek(SeekOp.MDB_NEXT) // anchor present: advance strictly past
        else true // already positioned at the first key > anchor
      case (Some(k), SeekOp.MDB_PREV) =>
        if (cursor.get(k, GetOp.MDB_SET_RANGE)) cursor.seek(SeekOp.MDB_PREV)
        else cursor.seek(SeekOp.MDB_LAST) // no key >= anchor: biggest key (if any) is < anchor
      case (Some(k), op) =>
        // For other anchored ops (e.g. MDB_NEXT_DUP) exact-key positioning is required.
        cursor.get(k, GetOp.MDB_SET) && cursor.seek(op)
    }
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
        case None    =>
          txn match {
            case Some(_) =>
              ZIO.fail(CollectionNotFound(name))
            case None    =>
              withExclusiveDbiOpen {
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

  /** Scoped read transaction. The native txn-begin / txn-close calls run as plain `ZIO.attempt` (not `attemptBlocking`) so they stay on the dedicated `readExecutor` thread set up by the outer `withReadLock`. This matters for two reasons:
    *
    *   1. lmdbjava maintains a `ThreadLocal<ArrayDeque<ByteBuffer>>` pool inside `ByteBufferProxy` for its per-`KeyVal` direct buffers. `attemptBlocking` would shift the work to ZIO's mobile-identity blocking pool, allocate fresh buffers there, then
    *      deallocate to a different blocking-pool thread on close — so the pool never warms up and every txn open/close pair creates 2 fresh `DirectByteBuffer` wrappers. Staying on the small dedicated read pool keeps the per-thread queue warm and
    *      drops steady-state allocation to zero.
    *   2. Fiber interruption between two consecutive blocking effects on the same txn would previously fire the scope finalizer (`txn.close`) while another blocking-pool worker still held the cursor — the SIGBUS at `mdb_page_search+0x53` documented
    *      in Round 3. The fix for that round was to **fuse** multi-step JNI sequences into a single effect block; that fusion is preserved here regardless of whether the effect is `attempt` or `attemptBlocking`.
    *
    * Compute-pool starvation isn't a concern: `readExecutor` is itself a dedicated fixed pool, not the compute pool.
    */
  private def withReadTransaction(colName: CollectionName): ZIO.Release[Any, StorageSystemError, Txn[ByteBuffer]] =
    ZIO.acquireReleaseWith(
      ZIO
        .attempt(env.txnRead())
        .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
    )(txn =>
      ZIO
        .attempt(txn.close())
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
                               env.getDbiNames.asScala
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): DeleteErrors)
        else {
          val keyBB    = fillKeyScratch(keyBytes)
          val found    = Option(dbi.get(txn, keyBB))
          val mayBeDoc = found.map(_ => codec.decode(txn.`val`()))
          val _        = dbi.delete(txn, keyBB)
          mayBeDoc match {
            case None            => Right(None)
            case Some(Right(v))  => Right(Some(v))
            case Some(Left(msg)) => Left(CodecFailure(msg): DeleteErrors)
          }
        }
      }
      .mapError[DeleteErrors](err => InternalError(s"Couldn't delete $key from $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): FetchErrors)
        else {
          val keyBB = fillKeyScratch(keyBytes)
          if (dbi.get(txn, keyBB) == null) Right(None)
          else
            codec.decode(txn.`val`()) match {
              case Right(v)  => Right(Some(v))
              case Left(msg) => Left(CodecFailure(msg): FetchErrors)
            }
        }
      }
      .mapError[FetchErrors](err => InternalError(s"Couldn't fetch $key on $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
  }

  /** @inheritdoc */
  override def fetchAt[K, T](colName: CollectionName, index: Long)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, Option[(K, T)]] = {
    for {
      db     <- getCollectionDbi(colName)
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attempt(env.txnRead())
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
                              .attempt {
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
                          )(cw => ZIO.attempt(cw._1.close()).ignoreLogged)
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
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attempt {
                                val keyBytes = keyCodec.encode(key)
                                if (keyBytes.length > env.getMaxKeySize) (None, Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): FetchErrors))
                                else {
                                  val cursor  = dbi.openCursor(txn)
                                  val found   = cursor.get(fillKeyScratch(keyBytes), GetOp.MDB_SET)
                                  val decoded =
                                    if (found) Some(toKeyCodec.decode(cursor.`val`()))
                                    else None
                                  (Some(cursor), Right(decoded))
                                }
                              }
                              .mapError[FetchErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                          )(cw => ZIO.foreachDiscard(cw._1)(c => ZIO.attempt(c.close()).ignoreLogged))
      result           <- cursorWithResult._2 match {
                            case Left(err)             => ZIO.fail(err)
                            case Right(None)           => ZIO.none
                            case Right(Some(Left(e)))  => ZIO.fail(CodecFailure(e): FetchErrors)
                            case Right(Some(Right(v))) => ZIO.some(v)
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
                              .attempt {
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
                          )(cw => ZIO.attempt(cw._1.close()).ignoreLogged)
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
                               .attempt(env.txnRead())
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
    // Anchored next/prev positioning is delegated to `positionCursorForSeek` (see its docstring for the SET_RANGE rationale).
    for {
      keyBB            <- ZIO.foreach(recordKey)(rk => borrowKeyBufferScoped(rk))
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attempt {
                                val cursor  = dbi.openCursor(txn)
                                val decoded =
                                  if (positionCursorForSeek(cursor, keyBB, seekOperation))
                                    Some((kodec.decode(cursor.key()), codec.decode(cursor.`val`())))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $colName: $err", Some(err)))
                          )(cw => ZIO.attempt(cw._1.close()).ignoreLogged)
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): ContainsErrors)
        else {
          val keyBB = fillKeyScratch(keyBytes)
          Right(dbi.get(txn, keyBB) != null)
        }
      }
      .mapError[ContainsErrors](err => InternalError(s"Couldn't check $key on $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): UpdateErrors)
        else {
          val keyBB = fillKeyScratch(keyBytes)
          if (dbi.get(txn, keyBB) == null) Right(None)
          else
            codec.decode(txn.`val`()) match {
              case Left(msg)        => Left(CodecFailure(msg): UpdateErrors)
              case Right(docBefore) =>
                val docAfter = modifier(docBefore)
                val docBytes = codec.encode(docAfter)
                // dbi.reserve returns a buffer pointing into LMDB's allocated DB page — no Java-side direct allocation. The dbi.get above invalidated txn.val(); we must rewrite the key buffer because the reserve call needs a stable
                // key view (the previous get-call left it positioned, but lmdbjava's KV proxy reads from position to limit on every call so a re-flip would also work — re-filling is the safer invariant).
                val keyBB2   = fillKeyScratch(keyBytes)
                val valBB    = dbi.reserve(txn, keyBB2, docBytes.length)
                valBB.put(docBytes)
                Right(Some(docAfter))
            }
        }
      }
      .mapError[UpdateErrors](err => InternalError(s"Couldn't update $key into $collectionName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): UpsertErrors)
        else {
          val keyBB    = fillKeyScratch(keyBytes)
          val docBytes = codec.encode(document)
          val valBB    = dbi.reserve(txn, keyBB, docBytes.length)
          valBB.put(docBytes)
          Right(())
        }
      }
      .mapError[UpsertErrors](err => InternalError(s"Couldn't upsertOverwrite $key into $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): InsertErrors)
        else {
          val keyBB    = fillKeyScratch(keyBytes)
          val docBytes = codec.encode(document)
          // NOOVERWRITE means dbi.put returns false on duplicate (clean Boolean result). We can't use dbi.reserve here because reserve throws KeyExistsException instead, which would force us to catch an exception in the hot path —
          // dbi.put + valueScratch keeps the no-overwrite branch fast and exception-free.
          val valBB    = fillValueScratch(docBytes)
          if (!dbi.put(txn, keyBB, valBB, PutFlags.MDB_NOOVERWRITE)) Left(KeyAlreadyExists(colName, key.toString): InsertErrors)
          else Right(())
        }
      }
      .mapError[InsertErrors](err => InternalError(s"Couldn't insert $key into $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): UpsertErrors)
        else {
          val keyBB       = fillKeyScratch(keyBytes)
          val mayBeBefore =
            if (dbi.get(txn, keyBB) == null) Right(None: Option[T])
            else codec.decode(txn.`val`()).map(Some(_))
          mayBeBefore match {
            case Left(msg)     => Left(CodecFailure(msg): UpsertErrors)
            case Right(before) =>
              val docAfter = modifier(before)
              val docBytes = codec.encode(docAfter)
              // Re-fill key scratch: lmdbjava's KV proxy was driven by the earlier dbi.get; we re-write the bytes so the buffer's position/limit are unambiguously fresh for the reserve call below.
              val keyBB2   = fillKeyScratch(keyBytes)
              val valBB    = dbi.reserve(txn, keyBB2, docBytes.length)
              valBB.put(docBytes)
              Right(docAfter)
          }
        }
      }
      .mapError[UpsertErrors](err => InternalError(s"Couldn't upsert $key into $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
                                      .attempt(env.txnRead())
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
      startAfterBB <- ZIO.foreach(startAfter)(borrowKeyBufferScoped(_))
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError[CollectErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attempt(cursor.close())
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

  /** Iterator that stops as soon as the LMDB cursor reaches a key whose raw bytes no longer carry
    * `prefixBytes` as a byte-level prefix. Used by `streamPrefix` / `streamPrefixWithKeys`.
    */
  private case class PrefixKeyValueIterator[K, T](
    jiterator: java.util.Iterator[KeyVal[ByteBuffer]],
    prefixBytes: Array[Byte]
  )(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]) extends Iterator[KeyValue[K, T]] {

    private var nextEntry: KeyValue[K, T] = null
    private var nextLoaded: Boolean       = false
    private var done: Boolean             = false

    private def keyStartsWithPrefix(keyBuf: ByteBuffer): Boolean = {
      val limit    = keyBuf.limit()
      val position = keyBuf.position()
      if (limit - position < prefixBytes.length) false
      else {
        var i = 0
        var ok = true
        while (ok && i < prefixBytes.length) {
          if (keyBuf.get(position + i) != prefixBytes(i)) ok = false
          i += 1
        }
        ok
      }
    }

    private def advance(): Unit = {
      if (done) {
        nextLoaded = true
        nextEntry = null
      } else if (!jiterator.hasNext) {
        done = true
        nextLoaded = true
        nextEntry = null
      } else {
        val kv     = jiterator.next()
        val keyBuf = kv.key()
        if (!keyStartsWithPrefix(keyBuf)) {
          done = true
          nextLoaded = true
          nextEntry = null
        } else {
          nextEntry = KeyValue(kodec.decode(keyBuf), codec.decode(kv.`val`()))
          nextLoaded = true
        }
      }
    }

    override def hasNext: Boolean = {
      if (!nextLoaded) advance()
      nextEntry != null
    }

    override def next(): KeyValue[K, T] = {
      if (!nextLoaded) advance()
      val res = nextEntry
      nextLoaded = false
      nextEntry = null
      res
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
                   .attempt(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attempt(txn.close())
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
      startAfterBB <- ZIO.foreach(startAfter)(borrowKeyBufferScoped(_))
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attempt(cursor.close())
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
                   .attempt(env.txnRead())
                   .mapError(err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attempt(txn.close())
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
      startAfterBB <- ZIO.foreach(startAfter)(borrowKeyBufferScoped(_))
      iterable     <- ZIO.acquireRelease(
                        ZIO
                          .attempt(dbi.iterate(txn, makeRange(startAfterBB, backward)))
                          .mapError(err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                      )(cursor =>
                        ZIO
                          .attempt(cursor.close())
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

  private def streamPrefixLogic[P, K, T](
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    colName: CollectionName,
    prefix: P
  )(implicit
    pcodec: KeyCodec[P],
    kcodec: KeyCodec[K],
    codec: LMDBCodec[T]
  ): ZIO[Scope, StreamErrors, ZStream[Any, StreamErrors, KeyValue[K, T]]] = {
    val prefixBytes = pcodec.encode(prefix)
    if (prefixBytes.length > env.getMaxKeySize)
      ZIO.fail(OverSizedKey(prefix.toString, prefixBytes.length, env.getMaxKeySize))
    else
      for {
        prefixBB <- ZIO.acquireRelease(
                      ZIO
                        .attempt {
                          val buf = borrowKeyBuffer()
                          buf.put(prefixBytes).flip()
                          buf
                        }
                        .mapError[StreamErrors](err => InternalError(s"Couldn't borrow buffer for prefix on $colName: $err", Some(err)))
                    )(buf => ZIO.succeed(releaseKeyBuffer(buf)))
        iterable <- ZIO.acquireRelease(
                      ZIO
                        .attempt(dbi.iterate(txn, KeyRange.atLeast(prefixBB)))
                        .mapError[StreamErrors](err => InternalError(s"Couldn't acquire iterable on $colName: $err", Some(err)))
                    )(cursor =>
                      ZIO
                        .attempt(cursor.close())
                        .ignoreLogged
                    )
      } yield ZStream
        .fromIterator(PrefixKeyValueIterator[K, T](iterable.iterator(), prefixBytes))
        .mapError[StreamErrors](err => InternalError(s"Couldn't streamPrefix from $colName: $err", Some(err)))
  }

  /** @inheritdoc */
  override def streamPrefix[P, K, T](
    colName: CollectionName,
    prefix: P
  )(implicit pcodec: KeyCodec[P], kcodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
    val result =
      for {
        db  <- getCollectionDbi(colName)
        _   <- readSemaphore.withPermitScoped
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attempt(env.txnRead())
                   .mapError[StreamErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attempt(txn.close())
                   .ignoreLogged
               )
        s   <- streamPrefixLogic[P, K, T](txn, db, colName, prefix)
      } yield s
        .mapZIO { entry => ZIO.fromEither(entry.value).mapError(err => CodecFailure(err): StreamErrors) }

    ZStream.unwrapScoped(result).onExecutor(readExecutor)
  }

  /** @inheritdoc */
  override def streamPrefixWithKeys[P, K, T](
    colName: CollectionName,
    prefix: P
  )(implicit pcodec: KeyCodec[P], kcodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
    val result =
      for {
        db  <- getCollectionDbi(colName)
        _   <- readSemaphore.withPermitScoped
        txn <- ZIO.acquireRelease(
                 ZIO
                   .attempt(env.txnRead())
                   .mapError[StreamErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
               )(txn =>
                 ZIO
                   .attempt(txn.close())
                   .ignoreLogged
               )
        s   <- streamPrefixLogic[P, K, T](txn, db, colName, prefix)
      } yield s
        .mapZIO { entry =>
          ZIO
            .fromEither(entry.value.flatMap(value => entry.key.left.map(_.toString).map(key => key -> value)))
            .mapError(err => CodecFailure(err): StreamErrors)
        }

    ZStream.unwrapScoped(result).onExecutor(readExecutor)
  }

  /** Gets a cached index DBI handle, or opens it if no transaction is in flight on this fiber. See `getCollectionDbi` for the safety contract.
    */
  private def getIndexDbi(name: IndexName, txn: Option[Txn[ByteBuffer]] = None): IO[IndexNotFound, Dbi[ByteBuffer]] = {
    openedCollectionDbisRef.get.flatMap { opened =>
      opened.get(name) match {
        case Some(d) => ZIO.succeed(d)
        case None    =>
          txn match {
            case Some(_) =>
              ZIO.fail(IndexNotFound(name))
            case None    =>
              withExclusiveDbiOpen {
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
    ZIO
      .attempt {
        val keyBytes    = keyCodec.encode(key)
        val targetBytes = toKeyCodec.encode(targetKey)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): IndexErrors)
        else if (targetBytes.length > env.getMaxKeySize) Left(OverSizedKey(targetKey.toString, targetBytes.length, env.getMaxKeySize): IndexErrors)
        else {
          val keyBB = fillKeyScratch(keyBytes)
          // DUPSORT collection: dbi.reserve is forbidden. Use the value-scratch buffer instead.
          val valBB = fillValueScratch(targetBytes)
          val _     = dbi.put(txn, keyBB, valBB)
          Right(())
        }
      }
      .mapError[IndexErrors](err => InternalError(s"Couldn't index $key -> $targetKey in $name: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
  }

  /** @inheritdoc */
  override def indexContains[FROM_KEY, TO_KEY](name: IndexName, key: FROM_KEY, targetKey: TO_KEY)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[IndexErrors, Boolean] = {
    for {
      dbi <- getIndexDbi(name)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attempt(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                        )(txn => ZIO.attempt(txn.close()).ignoreLogged)
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
                            .attempt(env.txnRead())
                            .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                        )(txn => ZIO.attempt(txn.close()).ignoreLogged)
                 res <- indexHasKeyLogic(txn, dbi, name, key)
               } yield res
             })
    } yield res
  }

  private def indexHasKeyLogic[FROM_KEY](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], name: IndexName, key: FROM_KEY)(implicit keyCodec: KeyCodec[FROM_KEY]): ZIO[Scope, IndexErrors, Boolean] = {
    // Same fused-JNI rationale as `indexSeekLogic`.
    for {
      cursor <- ZIO.acquireRelease(
                  ZIO.attempt(dbi.openCursor(txn)).mapError[IndexErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                )(c => ZIO.attempt(c.close()).ignoreLogged)
      found  <- ZIO
                  .attempt {
                    val keyBytes = keyCodec.encode(key)
                    if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): IndexErrors)
                    else Right(cursor.get(fillKeyScratch(keyBytes), GetOp.MDB_SET))
                  }
                  .mapError[IndexErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                  .flatMap(ZIO.fromEither(_))
    } yield found
  }

  private def indexSeek[FROM_KEY, TO_KEY](name: IndexName, recordKey: Option[FROM_KEY], seekOperation: SeekOp)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]): IO[FetchErrors, Option[(FROM_KEY, TO_KEY)]] = {
    for {
      db     <- getIndexDbi(name).catchAll { case IndexNotFound(n) => ZIO.fail(CollectionNotFound(n): FetchErrors) }
      result <- withReadLock(ZIO.scoped {
                  for {
                    txn <- ZIO.acquireRelease(
                             ZIO
                               .attempt(env.txnRead())
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
    // `ZIO.attempt` block so the entire JNI sequence runs atomically on the
    // dedicated read-executor thread. Without this, fiber interruption
    // between two consecutive effects can fire the cursor's scope finalizer
    // (cursor.close + txn.close) — freeing the txn's `mt_dbxs[]` — while
    // a sibling worker still holds the cursor for the following JNI call.
    // That is the use-after-free observed at `mdb_page_search+0x53` in
    // sotohp (Round 3). Note: we used to wrap this in `attemptBlocking`
    // but that shifted the work to ZIO's mobile-identity blocking pool,
    // defeating lmdbjava's per-thread `ByteBufferProxy` buffer cache
    // (Round 6 residual). `attempt` stays on `readExecutor`, which is
    // already off the compute pool, so blocking semantics are preserved
    // without losing thread affinity.
    for {
      keyBB            <- ZIO.foreach(recordKey)(rk => borrowKeyBufferScoped(rk).mapError { case e: OverSizedKey => e; case e: StorageSystemError => e })
      cursorWithResult <- ZIO.acquireRelease(
                            ZIO
                              .attempt {
                                val cursor  = dbi.openCursor(txn)
                                val decoded =
                                  if (positionCursorForSeek(cursor, keyBB, seekOperation))
                                    Some((keyCodec.decode(cursor.key()), toKeyCodec.decode(cursor.`val`())))
                                  else None
                                (cursor, decoded)
                              }
                              .mapError[FetchErrors](err => InternalError(s"Couldn't seek cursor for $name: $err", Some(err)))
                          )(cw => ZIO.attempt(cw._1.close()).ignoreLogged)
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
                               .attempt(env.txnRead())
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
                               .attempt(env.txnRead())
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
      cursor <- ZIO.acquireRelease(
                  ZIO.attempt(dbi.openCursor(txn)).mapError[IndexErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                )(c => ZIO.attempt(c.close()).ignoreLogged)
      found  <- ZIO
                  .attempt {
                    val keyBytes    = keyCodec.encode(key)
                    val targetBytes = toKeyCodec.encode(targetKey)
                    if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): IndexErrors)
                    else if (targetBytes.length > env.getMaxKeySize) Left(OverSizedKey(targetKey.toString, targetBytes.length, env.getMaxKeySize): IndexErrors)
                    else {
                      // keyScratch is consumed by cursor.get(MDB_SET); valueScratch holds targetBytes for the duration of the dup-value scan.
                      val keyBB                = fillKeyScratch(keyBytes)
                      val valueBB              = fillValueScratch(targetBytes)
                      @scala.annotation.tailrec
                      def findValue(): Boolean = {
                        if (cursor.`val`().compareTo(valueBB) == 0) true
                        else if (cursor.seek(SeekOp.MDB_NEXT_DUP)) findValue()
                        else false
                      }
                      val result               = if (cursor.get(keyBB, GetOp.MDB_SET)) findValue() else false
                      Right(result)
                    }
                  }
                  .mapError[IndexErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                  .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes    = keyCodec.encode(key)
        val targetBytes = toKeyCodec.encode(targetKey)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): IndexErrors)
        else if (targetBytes.length > env.getMaxKeySize) Left(OverSizedKey(targetKey.toString, targetBytes.length, env.getMaxKeySize): IndexErrors)
        else {
          val keyBB   = fillKeyScratch(keyBytes)
          val valueBB = fillValueScratch(targetBytes)
          Right(dbi.delete(txn, keyBB, valueBB))
        }
      }
      .mapError[IndexErrors](e => InternalError(s"Delete error: $e", Some(e)))
      .flatMap(ZIO.fromEither(_))
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
                     .attempt(env.txnRead())
                     .mapError(err => InternalError(s"Couldn't acquire read transaction on $name: $err", Some(err)))
                 )(txn => ZIO.attempt(txn.close()).ignoreLogged)
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
      cursor <- ZIO.acquireRelease(
                  ZIO
                    .attempt(dbi.openCursor(txn))
                    .mapError(err => InternalError(s"Couldn't acquire cursor on $name: $err", Some(err)))
                )(cursor => ZIO.attempt(cursor.close()).ignoreLogged)

      // keyScratch is consumed by cursor.get(MDB_SET) below; subsequent MDB_NEXT_DUP / MDB_NEXT iteration calls in the stream do not touch the buffer, so reuse is safe.
      found <- ZIO
                 .attempt {
                   val keyBytes = keyCodec.encode(key)
                   if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): IndexErrors)
                   else Right(cursor.get(fillKeyScratch(keyBytes), GetOp.MDB_SET))
                 }
                 .mapError[IndexErrors](err => InternalError(s"Seek error: $err", Some(err)))
                 .flatMap(ZIO.fromEither(_))

    } yield {
      if (!found) ZStream.empty
      else {
        ZStream.paginateChunkZIO(true) { isFirst =>
          ZIO
            .attempt {
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
        case None    =>
          txn match {
            case Some(_) =>
              ZIO.fail(CollectionNotFound(name))
            case None    =>
              withExclusiveDbiOpen {
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
      cursor <- ZIO.acquireRelease(
                  ZIO.attempt(dbi.openCursor(txn)).mapError[FetchErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                )(c => ZIO.attempt(c.close()).ignoreLogged)
      result <- ZIO
                  .attempt {
                    val keyBytes = kodec.encode(key)
                    if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): FetchErrors)
                    else {
                      val keyBB = fillKeyScratch(keyBytes)
                      if (!cursor.get(keyBB, GetOp.MDB_SET)) Right(Nil: List[T])
                      else {
                        val builder = List.newBuilder[T]
                        var hasNext = true
                        while (hasNext) {
                          codec.decode(cursor.`val`()) match {
                            case Right(v) => builder += v
                            case Left(_)  => ()
                          }
                          hasNext = cursor.seek(SeekOp.MDB_NEXT_DUP)
                        }
                        Right(builder.result())
                      }
                    }
                  }
                  .mapError[FetchErrors](e => InternalError(s"multiFetch error on $colName: $e", Some(e)))
                  .flatMap(ZIO.fromEither(_))
    } yield result
  }

  override def multiFetch[K, T](colName: CollectionName, key: K)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[FetchErrors, List[T]] = {
    for {
      db  <- getMultiDbi(colName)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attempt(env.txnRead())
                            .mapError[FetchErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                        )(txn => ZIO.attempt(txn.close()).ignoreLogged)
                 res <- multiFetchLogic(txn, db, colName, key)
               } yield res
             })
    } yield res
  }

  private def multiContainsLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit
    kodec: KeyCodec[K],
    codec: LMDBCodec[T]
  ): ZIO[Scope, ContainsErrors, Boolean] = {
    // Mirrors `indexContainsLogic`: position the cursor at the key with MDB_SET,
    // then scan dup values comparing each ByteBuffer to the encoded `document`.
    // Equivalent to MDB_GET_BOTH without requiring the lmdbjava op exposure.
    for {
      cursor <- ZIO.acquireRelease(
                  ZIO.attempt(dbi.openCursor(txn)).mapError[ContainsErrors](e => InternalError(s"Cursor error: $e", Some(e)))
                )(c => ZIO.attempt(c.close()).ignoreLogged)
      found  <- ZIO
                  .attempt {
                    val keyBytes   = kodec.encode(key)
                    val valueBytes = codec.encode(document)
                    if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): ContainsErrors)
                    else {
                      val keyBB   = fillKeyScratch(keyBytes)
                      val valueBB = fillValueScratch(valueBytes)
                      @scala.annotation.tailrec
                      def findValue(): Boolean = {
                        if (cursor.`val`().compareTo(valueBB) == 0) true
                        else if (cursor.seek(SeekOp.MDB_NEXT_DUP)) findValue()
                        else false
                      }
                      val result  = if (cursor.get(keyBB, GetOp.MDB_SET)) findValue() else false
                      Right(result)
                    }
                  }
                  .mapError[ContainsErrors](e => InternalError(s"multiContains error on $colName: $e", Some(e)))
                  .flatMap(ZIO.fromEither(_))
    } yield found
  }

  override def multiContains[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[ContainsErrors, Boolean] = {
    for {
      db  <- getMultiDbi(colName)
      res <- withReadLock(ZIO.scoped {
               for {
                 txn <- ZIO.acquireRelease(
                          ZIO
                            .attempt(env.txnRead())
                            .mapError[ContainsErrors](err => InternalError(s"Couldn't acquire read transaction on $colName: $err", Some(err)))
                        )(txn => ZIO.attempt(txn.close()).ignoreLogged)
                 res <- multiContainsLogic(txn, db, colName, key, document)
               } yield res
             })
    } yield res
  }

  private def multiPutLogic[K, T](txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[UpsertErrors, Unit] = {
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): UpsertErrors)
        else {
          val keyBB    = fillKeyScratch(keyBytes)
          val docBytes = codec.encode(document)
          // DUPSORT collection: dbi.reserve is forbidden. Use the value-scratch buffer instead.
          val valBB    = fillValueScratch(docBytes)
          val _        = dbi.put(txn, keyBB, valBB)
          Right(())
        }
      }
      .mapError[UpsertErrors](err => InternalError(s"Couldn't multiPut $key into $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): DeleteErrors)
        else {
          val keyBB    = fillKeyScratch(keyBytes)
          val docBytes = codec.encode(document)
          val valBB    = fillValueScratch(docBytes)
          Right(dbi.delete(txn, keyBB, valBB))
        }
      }
      .mapError[DeleteErrors](err => InternalError(s"Couldn't multiDelete $key from $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
    ZIO
      .attempt {
        val keyBytes = kodec.encode(key)
        if (keyBytes.length > env.getMaxKeySize) Left(OverSizedKey(key.toString, keyBytes.length, env.getMaxKeySize): DeleteErrors)
        else {
          val keyBB = fillKeyScratch(keyBytes)
          Right(dbi.delete(txn, keyBB))
        }
      }
      .mapError[DeleteErrors](err => InternalError(s"Couldn't multiDeleteAll $key from $colName: $err", Some(err)))
      .flatMap(ZIO.fromEither(_))
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
                     .attempt(env.txnRead())
                     .mapError(err => InternalError(s"Couldn't acquire read transaction: $err", Some(err)))
                 )(txn => ZIO.attempt(txn.close()).ignoreLogged)
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
    override def streamPrefix[P, K, T](
      collectionName: CollectionName,
      prefix: P
    )(implicit pcodec: KeyCodec[P], kcodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, T] = {
      val result = for {
        db     <- getCollectionDbi(collectionName, Some(txn))
        stream <- streamPrefixLogic[P, K, T](txn, db, collectionName, prefix)
      } yield stream
        .mapZIO { entry => ZIO.fromEither(entry.value).mapError(err => CodecFailure(err): StreamErrors) }
      ZStream.unwrapScoped(result)
    }

    /** @inheritdoc */
    override def streamPrefixWithKeys[P, K, T](
      collectionName: CollectionName,
      prefix: P
    )(implicit pcodec: KeyCodec[P], kcodec: KeyCodec[K], codec: LMDBCodec[T]): ZStream[Any, StreamErrors, (K, T)] = {
      val result = for {
        db     <- getCollectionDbi(collectionName, Some(txn))
        stream <- streamPrefixLogic[P, K, T](txn, db, collectionName, prefix)
      } yield stream
        .mapZIO { entry =>
          ZIO
            .fromEither(entry.value.flatMap(value => entry.key.left.map(_.toString).map(key => key -> value)))
            .mapError(err => CodecFailure(err): StreamErrors)
        }
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

    /** @inheritdoc */
    override def multiContains[K, T](colName: CollectionName, key: K, document: T)(implicit kodec: KeyCodec[K], codec: LMDBCodec[T]): IO[ContainsErrors, Boolean] = {
      for {
        db  <- getMultiDbi(colName, Some(txn))
        res <- ZIO.scoped(multiContainsLogic(txn, db, colName, key, document))
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
