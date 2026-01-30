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
package zio.lmdb.keytools

import java.nio.ByteBuffer
import java.util.UUID

object UUIDTools {

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
}
