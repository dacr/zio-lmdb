package zio.lmdb

case class UpsertState[T](previous:Option[T], current: T)
