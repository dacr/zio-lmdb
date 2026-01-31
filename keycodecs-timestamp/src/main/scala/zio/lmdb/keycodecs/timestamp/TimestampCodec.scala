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
package zio.lmdb.keycodecs.timestamp

import zio.lmdb.keycodecs.KeyCodec
import java.nio.ByteBuffer
import java.time.Instant

object TimestampCodec {

  /**
   * Encodes Instant into 12 bytes:
   * - 8 bytes for epoch seconds (sign bit flipped for lexicographical sorting)
   * - 4 bytes for nanoseconds
   */
  given timestampKeyCodec: KeyCodec[Instant] = new KeyCodec[Instant] {
    override def encode(key: Instant): Array[Byte] = {
      val buffer = ByteBuffer.allocate(12)
      // Flip sign bit to make signed long lexicographically sortable as unsigned bytes
      buffer.putLong(key.getEpochSecond ^ Long.MinValue)
      buffer.putInt(key.getNano)
      buffer.array()
    }

    override def decode(keyBytes: ByteBuffer): Either[String, Instant] = {
      if (keyBytes.remaining() < 12) Left(s"Not enough bytes for Instant, expected 12 but got ${keyBytes.remaining()}")
      else {
        val secondsEncoded = keyBytes.getLong
        val nanos = keyBytes.getInt
        // Restore original seconds by flipping sign bit again
        val seconds = secondsEncoded ^ Long.MinValue
        Right(Instant.ofEpochSecond(seconds, nanos.toLong))
      }
    }

    override def width: Option[Int] = Some(12)
  }
}
