package zio.lmdb

case class DatabaseNotFound(name: String)
case class JsonFailure(issue: String)

sealed trait LMDBError {
  val message: String
  val cause: Option[Throwable]
}
case class InternalError(message: String, cause: Option[Throwable]=None) extends LMDBError
