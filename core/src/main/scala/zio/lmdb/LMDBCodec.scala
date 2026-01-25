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

import java.nio.ByteBuffer

/** Trait representing a codec for encoding and decoding values of type `T` to and from a binary format using LMDB.
  *
  * @tparam T
  *   The type of the values to be encoded and decoded.
  */
trait LMDBCodec[T] {

  /** Encodes a value of type `T` into a byte array.
    * @param value
    *   the value to encode
    * @return
    *   the byte array representation
    */
  def encode(value: T): Array[Byte]

  /** Decodes a value of type `T` from a byte buffer.
    * @param valueBytes
    *   the byte buffer containing the encoded value
    * @return
    *   the decoded value or an error message
    */
  def decode(valueBytes: ByteBuffer): Either[String, T]
}
