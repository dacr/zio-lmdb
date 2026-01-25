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
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.{Try, Success, Failure}

/** A codec abstraction for encoding and decoding keys of type `K` into a byte array representation for use with an LMDB database. The trait provides methods for serialization and deserialization, allowing a bidirectional mapping between `K` and its
  * byte representation.
  *
  * @tparam K
  *   The type of key to be encoded and decoded.
  */
trait LMDBKodec[K] {

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

object LMDBKodec {

  given LMDBKodec[String] = new LMDBKodec[String] {
    private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

    override def encode(key: String): Array[Byte] = key.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[String, String] =
      Right(charset.decode(keyBytes).toString)
  }

  /** Converts a UUID to a byte array.
    * @param uuid
    *   the UUID to convert
    * @return
    *   a 16-byte array
    */
  def uuidToBytes(uuid: UUID): Array[Byte] = {
    val out = new Array[Byte](16)
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits

    // Fill the array using bit-shifting for maximum speed
    for (i <- 0 until 8) {
      out(i) = (msb >>> (8 * (7 - i))).toByte
    }
    for (i <- 8 until 16) {
      out(i) = (lsb >>> (8 * (15 - i))).toByte
    }
    out
  }

  /** Converts a 16-byte array to a UUID.
    * @param bytes
    *   the 16-byte array
    * @return
    *   the UUID
    */
  def bytesToUUID(bytes: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(bytes)
    new UUID(bb.getLong, bb.getLong)
  }

  given LMDBKodec[UUID] = new LMDBKodec[UUID] {
    override def encode(key: UUID): Array[Byte] = uuidToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[String, UUID] = {
      if (keyBytes.remaining() < 16) Left(s"Not enough bytes for UUID, expected 16 but got ${keyBytes.remaining()}")
      else {
        val msb = keyBytes.getLong
        val lsb = keyBytes.getLong
        Right(new UUID(msb, lsb))
      }
    }
  }

}
