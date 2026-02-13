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

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.uca.UCAKey

import java.nio.ByteBuffer
import com.ibm.icu.text.Collator
import com.ibm.icu.util.ULocale
import zio.lmdb.keycodecs.uca.UCAKeyCodec.ucaKeyCodec

object UCACodecSpec extends ZIOSpecDefault {

  def spec = suite("UCACodec spec")(
    test("SortKey generation and ordering") {
      val collator = Collator.getInstance(ULocale.FRENCH)
      val words = List("cote", "côte", "coté", "côté")
      val sortKeys = words.map(w => UCAKey.from(w, collator))
      
      // Verify ordering of SortKeys (byte comparison) matches collator comparison
      val consistent = words.zip(sortKeys).zip(words.tail.zip(sortKeys.tail)).forall { 
        case ((w1, sk1), (w2, sk2)) =>
          val colCmp = collator.compare(w1, w2)
          val skCmp = sk1.compare(sk2)
          Integer.signum(colCmp) == Integer.signum(skCmp)
      }
      assertTrue(consistent)
    },
    test("Default collator (Root locale) sorting") {
      // Root locale handles basic Latin characters essentially like ASCII/binary but with UCA rules
      // It puts accents after base letters usually? No, it depends on weights.
      // UCA default: "coté" < "côte"?
      // Let's just verify it works and is consistent.
      
      val words = List("banana", "apple", "cherry")
      val sortKeys = words.map(w => UCAKey.from(w)) // Using default collator
      
      val expectedOrder = List("apple", "banana", "cherry")
      val sortedWords = sortKeys.zip(words).sortBy(_._1.bytes)(Ordering.fromLessThan((a, b) => java.util.Arrays.compareUnsigned(a, b) < 0)).map(_._2)
      
      assertTrue(sortedWords == expectedOrder)
    },
    test("Default collator accent handling") {
      val words = List("cote", "côte")
      val sortKeys = words.map(w => UCAKey.from(w))
      
      // In UCA default, accents are significant at secondary level.
      // "cote" < "côte" usually.
      val sk1 = sortKeys(0)
      val sk2 = sortKeys(1)
      
      assertTrue(sk1.compare(sk2) != 0) // Should not be equal
    },
    test("Base64 representation roundtrip") {
      val word = "Hello World"
      val sk = UCAKey.from(word)
      val base64 = sk.toBase64
      val decodedSk = UCAKey.fromBase64(base64)
      
      val isEqual = java.util.Arrays.equals(sk.bytes, decodedSk.bytes)
      
      assertTrue(
        sk.compare(decodedSk) == 0,
        isEqual
      )
    }
  )
}
