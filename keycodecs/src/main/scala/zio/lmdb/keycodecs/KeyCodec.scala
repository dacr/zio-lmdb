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
package zio.lmdb.keycodecs

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.{Try, Success, Failure}

/** A codec abstraction for encoding and decoding keys of type `K` into a byte array representation for use with an LMDB database. The trait provides methods for serialization and deserialization, allowing a bidirectional mapping between `K` and its
  * byte representation.
  *
  * @tparam K
  *   The type of key to be encoded and decoded.
  */
trait KeyCodec[K] {

  /** Encodes a key of type `K` into a byte array.
    * @param key
    *   the key to encode
    * @return
    *   the byte array representation
    */
  def encode(key: K): Array[Byte]

  /** Decodes a key of type `K` from a byte buffer.
    * @param keyBytes
    *   the byte buffer containing the encoded key
    * @return
    *   the decoded key or an error message
    */
  def decode(keyBytes: ByteBuffer): Either[String, K] // TODO Replace String by Throwable
}

object KeyCodec {

  given KeyCodec[String] = new KeyCodec[String] {
    private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

    override def encode(key: String): Array[Byte] = key.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[String, String] =
      Right(charset.decode(keyBytes).toString)
  }

  given KeyCodec[UUID] = new KeyCodec[UUID] {
    override def encode(key: UUID): Array[Byte] = UUIDTools.uuidToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[String, UUID] = {
      if (keyBytes.remaining() < 16) Left(s"Not enough bytes for UUID, expected 16 but got ${keyBytes.remaining()}")
      else {
        val msb = keyBytes.getLong
        val lsb = keyBytes.getLong
        Right(new UUID(msb, lsb))
      }
    }
  }

  given KeyCodec[GEOTools.Location] = new KeyCodec[GEOTools.Location] {
    override def encode(key: GEOTools.Location): Array[Byte] = GEOTools.locationToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[String, GEOTools.Location] = {
      if (keyBytes.remaining() < 8) Left(s"Not enough bytes for Location, expected 8 but got ${keyBytes.remaining()}")
      else {
        val bytes = new Array[Byte](8)
        keyBytes.get(bytes)
        Right(GEOTools.bytesToLocation(bytes))
      }
    }
  }

}