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
package zio.lmdb.keycodecs.geo

import zio.lmdb.keycodecs.KeyCodec
import java.nio.ByteBuffer
import zio.lmdb.keycodecs.KeyCodecError
import zio.lmdb.keycodecs.KeyCodecError._

object GeoCodec {
  given locationKeyCodec: KeyCodec[GEOTools.Location] = new KeyCodec[GEOTools.Location] {
    override def encode(key: GEOTools.Location): Array[Byte] = GEOTools.locationToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, GEOTools.Location] = {
      if (keyBytes.remaining() < 8) Left(InsufficientBytes(8, keyBytes.remaining()))
      else {
        val bytes = new Array[Byte](8)
        keyBytes.get(bytes)
        Right(GEOTools.bytesToLocation(bytes))
      }
    }

    override def width: Option[Int] = Some(8)
  }
}
