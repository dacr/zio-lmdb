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
package zio.lmdb.vector

import zio.lmdb.LMDBCodec

import java.nio.ByteBuffer

/** `LMDBCodec[Array[Float]]` used to store raw vectors: 4 bytes per component, no framing. Kept separate from any particular dimension so it can back indexes of any size.
  */
object VectorCodec {
  given floatArrayCodec: LMDBCodec[Array[Float]] with {
    def encode(value: Array[Float]): Array[Byte] = {
      val buffer = ByteBuffer.allocate(value.length * java.lang.Float.BYTES)
      var i      = 0
      while (i < value.length) {
        buffer.putFloat(value(i))
        i += 1
      }
      buffer.array()
    }

    def decode(valueBytes: ByteBuffer): Either[String, Array[Float]] = {
      val remaining = valueBytes.remaining()
      if (remaining % java.lang.Float.BYTES != 0)
        Left(s"invalid vector byte length: $remaining is not a multiple of ${java.lang.Float.BYTES}")
      else {
        val count  = remaining / java.lang.Float.BYTES
        val values = new Array[Float](count)
        var i      = 0
        while (i < count) {
          values(i) = valueBytes.getFloat()
          i += 1
        }
        Right(values)
      }
    }
  }
}
