package zio.lmdb

enum StorageUserError extends Exception {
  case DatabaseNotFound(name: String)                          extends StorageUserError
  case JsonFailure(issue: String)                              extends StorageUserError
  case OverSizedKey(id: String, expandedSize: Int, limit: Int) extends StorageUserError
}

enum StorageSystemError extends Error {
  case InternalError(message: String, cause: Option[Throwable] = None) extends StorageSystemError
}
