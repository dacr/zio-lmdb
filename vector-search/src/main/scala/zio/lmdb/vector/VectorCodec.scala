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

import java.nio.{ByteBuffer, ByteOrder}

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
        val values = new Array[Float](remaining / java.lang.Float.BYTES)
        readFloats(valueBytes, values)
        valueBytes.position(valueBytes.limit())
        Right(values)
      }
    }
  }

  /** Copies the floats of `bytes`, from its position, into `target` in one bulk copy, as `encode` wrote them (big-endian): decoding float by float with `getFloat` is a bounds and session check per component, the major cost of a scan over a large
    * index. Leaves the position of `bytes` untouched.
    */
  def readFloats(bytes: ByteBuffer, target: Array[Float]): Unit =
    bytes.duplicate().order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(target)
}
