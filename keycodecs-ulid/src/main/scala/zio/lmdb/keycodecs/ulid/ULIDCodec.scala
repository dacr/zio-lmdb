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
package zio.lmdb.keycodecs.ulid

import wvlet.airframe.ulid.ULID
import zio.lmdb.keycodecs.KeyCodec
import java.nio.ByteBuffer

object ULIDCodec {
  given KeyCodec[ULID] = new KeyCodec[ULID] {
    override def encode(key: ULID): Array[Byte] = key.toBytes

    override def decode(keyBytes: ByteBuffer): Either[String, ULID] = {
      if (keyBytes.remaining() < 16) Left(s"Not enough bytes for ULID, expected 16 but got ${keyBytes.remaining()}")
      else {
        val bytes = new Array[Byte](16)
        keyBytes.get(bytes)
        Right(ULID.fromBytes(bytes))
      }
    }

    override def width: Option[Int] = Some(16)
  }
}
