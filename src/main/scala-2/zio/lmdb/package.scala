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

package zio

package object lmdb {
  type CollectionName = String
  type RecordKey      = String

  type KeyErrors      = LmdbError
  type SizeErrors     = LmdbError
  type ClearErrors    = LmdbError
  type GetErrors      = LmdbError
  type CreateErrors   = LmdbError
  type FetchErrors    = LmdbError
  type ContainsErrors = LmdbError
  type UpsertErrors   = LmdbError
  type DeleteErrors   = LmdbError
  type CollectErrors  = LmdbError
  type StreamErrors   = LmdbError
}
