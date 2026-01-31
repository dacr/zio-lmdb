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

import zio.*
import zio.stream.*

/** A helper class to simplify user experience by avoiding repeating index name and data types
  *
  * @param name
  *   index name
  * @param description
  *   index description
  * @param lmdb
  *   LMDB service
  * @tparam FROM_KEY
  *   the type of the key
  * @tparam TO_KEY
  *   the type of the value (target key)
  */
case class LMDBIndex[FROM_KEY, TO_KEY](
  name: IndexName,
  description: Option[String],
  lmdb: LMDB
)(implicit keyCodec: KeyCodec[FROM_KEY], toKeyCodec: KeyCodec[TO_KEY]) {

  /** Add a mapping to the index
    * @param key
    *   the key to index
    * @param toKey
    *   the target key to map to
    */
  def index(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Unit] = lmdb.index[FROM_KEY, TO_KEY](name, key, toKey)

  /** Check if the index contains the given mapping
    * @param key
    *   the key to check
    * @param toKey
    *   the target key to check
    * @return
    *   true if the index contains the mapping
    */
  def indexContains(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.indexContains[FROM_KEY, TO_KEY](name, key, toKey)

  /** Remove a mapping from the index
    * @param key
    *   the key to unindex
    * @param toKey
    *   the target key to unmap
    * @return
    *   true if the mapping was found and removed
    */
  def unindex(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.unindex[FROM_KEY, TO_KEY](name, key, toKey)

  /** Get a stream of target keys for a given key in the index
    * @param key
    *   the key to look for
    * @return
    *   a stream of target keys
    */
  def indexed(
    key: FROM_KEY
  ): ZStream[Any, IndexErrors, TO_KEY] = lmdb.indexed[FROM_KEY, TO_KEY](name, key)

}
