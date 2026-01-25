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

/** Errors that can be triggered by user actions or invalid data. */
enum StorageUserError {
  case CollectionAlreadExists(name: CollectionName)
  case CollectionNotFound(name: CollectionName)
  case IndexAlreadyExists(name: IndexName)
  case IndexNotFound(name: IndexName)
  case CodecFailure(issue: String)
  case OverSizedKey(id: String, expandedSize: Int, limit: Int)
}

/** Errors indicating a failure within the underlying storage system or library. */
enum StorageSystemError {
  case InternalError(message: String, cause: Option[Throwable] = None) extends StorageSystemError
}
