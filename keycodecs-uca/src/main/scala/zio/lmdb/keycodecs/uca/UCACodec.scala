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
package zio.lmdb.keycodecs.uca

import com.ibm.icu.text.Collator
import zio.lmdb.keycodecs.KeyCodec

import java.nio.ByteBuffer
import com.ibm.icu.util.ULocale

opaque type UCAKey = Array[Byte]

object UCAKey {
  def apply(bytes: Array[Byte]): UCAKey = bytes

  def from(text: String, collator: Collator = Collator.getInstance(ULocale.ROOT)): UCAKey = {
    UCAKey(collator.getCollationKey(text).toByteArray)
  }

  def fromBase64(base64: String): UCAKey =
    UCAKey(java.util.Base64.getDecoder.decode(base64))
}

extension (key: UCAKey) {
  def bytes: Array[Byte] = key

  def compare(other: UCAKey): Int =
    java.util.Arrays.compareUnsigned(key, other)

  def toBase64: String =
    java.util.Base64.getEncoder.encodeToString(key)
}

object UCAKeyCodec {
  given ucaKeyCodec: KeyCodec[UCAKey] = new KeyCodec[UCAKey] {
    override def encode(key: UCAKey): Array[Byte] = key.bytes

    override def decode(keyBytes: ByteBuffer): Either[String, UCAKey] = {
      val bytes = new Array[Byte](keyBytes.remaining())
      keyBytes.get(bytes)
      Right(UCAKey(bytes))
    }

    override def width: Option[Int] = None
  }
}
