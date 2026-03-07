package zio.lmdb

import zio.*

/** Defines callbacks for updating indices when a collection is modified.
  * @tparam K The collection key type
  * @tparam T The collection value type
  */
trait IndexUpdater[K, T] {
  def onInsert(ops: LMDBWriteOps, key: K, newValue: T): IO[IndexErrors, Unit]
  def onDelete(ops: LMDBWriteOps, key: K, oldValue: T): IO[IndexErrors, Unit]
  def onUpdate(ops: LMDBWriteOps, key: K, oldValue: T, newValue: T): IO[IndexErrors, Unit]
  def onClear(ops: LMDBWriteOps): IO[IndexErrors, Unit]
}
