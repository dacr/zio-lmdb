package zio.lmdb

enum StorageUserError {
  case CollectionAlreadExists(name: CollectionName)            extends StorageUserError
  case CollectionNotFound(name: CollectionName)                extends StorageUserError
  case JsonFailure(issue: String)                              extends StorageUserError
  case OverSizedKey(id: String, expandedSize: Int, limit: Int) extends StorageUserError
}

enum StorageSystemError {
  case InternalError(message: String, cause: Option[Throwable] = None) extends StorageSystemError
}
