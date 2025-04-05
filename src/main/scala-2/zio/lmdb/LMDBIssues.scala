/*
 * Copyright 2023 David Crosson
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

trait LmdbError
trait StorageUserError extends LmdbError
object StorageUserError {
  case class CollectionAlreadExists(name: CollectionName)            extends StorageUserError
  case class CollectionNotFound(name: CollectionName)                extends StorageUserError
  case class CodecFailure(issue: String)                             extends StorageUserError
  case class OverSizedKey(id: String, expandedSize: Int, limit: Int) extends StorageUserError
}

trait StorageSystemError extends LmdbError
object StorageSystemError {
  case class InternalError(message: String, cause: Option[Throwable] = None) extends StorageSystemError
}
