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
import java.util.Arrays

import com.ibm.icu.util.ULocale

opaque type UCASortKey = Array[Byte]

object UCASortKey {
  def apply(bytes: Array[Byte]): UCASortKey = bytes
  
  def from(text: String, collator: Collator = Collator.getInstance(ULocale.ROOT)): UCASortKey = {
    collator.getCollationKey(text).toByteArray
  }
  
  extension (key: UCASortKey) {
    def bytes: Array[Byte] = key
    
    def compare(other: UCASortKey): Int = 
      Arrays.compareUnsigned(key, other)
  }

  given ucaSortKeyCodec: KeyCodec[UCASortKey] = new KeyCodec[UCASortKey] {
    override def encode(key: UCASortKey): Array[Byte] = key
    
    override def decode(keyBytes: ByteBuffer): Either[String, UCASortKey] = {
      val bytes = new Array[Byte](keyBytes.remaining())
      keyBytes.get(bytes)
      Right(bytes)
    }
    
    override def width: Option[Int] = None 
  }
}
