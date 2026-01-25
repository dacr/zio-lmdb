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

package zio

import zio.lmdb.StorageUserError.*

package object lmdb {
  type CollectionName = String
  type RecordKey      = String
  type IndexName      = String

  type KeyErrors      = OverSizedKey | StorageSystemError
  type SizeErrors     = CollectionNotFound | StorageSystemError
  type ClearErrors    = CollectionNotFound | StorageSystemError
  type DropErrors     = CollectionNotFound | StorageSystemError
  type GetErrors      = CollectionNotFound | StorageSystemError
  type CreateErrors   = CollectionAlreadExists | StorageSystemError
  type FetchErrors    = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type ContainsErrors = OverSizedKey | CollectionNotFound | StorageSystemError
  type UpdateErrors   = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type UpsertErrors   = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type DeleteErrors   = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type CollectErrors  = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type StreamErrors   = OverSizedKey | CollectionNotFound | CodecFailure | StorageSystemError
  type IndexErrors    = IndexNotFound | IndexAlreadyExists | OverSizedKey | CodecFailure | StorageSystemError
}
