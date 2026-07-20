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
package zio.lmdb.keycodecs

import zio.test.*
import zio.test.Assertion.*

import java.util.Arrays

object TupleKeyLayoutSpec extends ZIOSpecDefault {

  private val longCodec = summon[KeyCodec[Long]]
  private val strCodec  = summon[KeyCodec[String]]

  private val longStr: List[Option[Int]]     = List(longCodec.width, strCodec.width)
  private val strLong: List[Option[Int]]     = List(strCodec.width, longCodec.width)
  private val strLongLong: List[Option[Int]] = List(strCodec.width, longCodec.width, longCodec.width)
  private val longStrLong: List[Option[Int]] = List(longCodec.width, strCodec.width, longCodec.width)

  private def enc2LS(t: (Long, String)): Array[Byte]        = summon[KeyCodec[(Long, String)]].encode(t)
  private def enc2SL(t: (String, Long)): Array[Byte]        = summon[KeyCodec[(String, Long)]].encode(t)
  private def enc3SLL(t: (String, Long, Long)): Array[Byte] = summon[KeyCodec[(String, Long, Long)]].encode(t)
  private def enc3LSL(t: (Long, String, Long)): Array[Byte] = summon[KeyCodec[(Long, String, Long)]].encode(t)

  private def within(lower: Array[Byte], upper: Option[Array[Byte]], key: Array[Byte]): Boolean =
    Arrays.compareUnsigned(key, lower) >= 0 && upper.forall(u => Arrays.compareUnsigned(key, u) < 0)

  def spec = suite("TupleKeyLayout spec")(
    suite("prefixBytes as full encoding")(
      test("matches tuple2 codec encoding (fixed, variable)") {
        check(Gen.long, Gen.string) { (l, s) =>
          val bytes = TupleKeyLayout.prefixBytes(longStr, List(longCodec.encode(l), strCodec.encode(s)))
          assertTrue(Arrays.equals(bytes, enc2LS((l, s))))
        }
      },
      test("matches tuple2 codec encoding (variable, fixed)") {
        check(Gen.string, Gen.long) { (s, l) =>
          val bytes = TupleKeyLayout.prefixBytes(strLong, List(strCodec.encode(s), longCodec.encode(l)))
          assertTrue(Arrays.equals(bytes, enc2SL((s, l))))
        }
      },
      test("matches tuple3 codec encoding (variable leading)") {
        check(Gen.string, Gen.long, Gen.long) { (s, a, b) =>
          val bytes = TupleKeyLayout.prefixBytes(strLongLong, List(strCodec.encode(s), longCodec.encode(a), longCodec.encode(b)))
          assertTrue(Arrays.equals(bytes, enc3SLL((s, a, b))))
        }
      },
      test("matches tuple3 codec encoding (variable in the middle)") {
        check(Gen.long, Gen.string, Gen.long) { (a, s, b) =>
          val bytes = TupleKeyLayout.prefixBytes(longStrLong, List(longCodec.encode(a), strCodec.encode(s), longCodec.encode(b)))
          assertTrue(Arrays.equals(bytes, enc3LSL((a, s, b))))
        }
      }
    ),
    suite("prefix scan bounds")(
      test("first-component equality bounds select exactly the matching tuple2 keys") {
        check(Gen.string, Gen.string, Gen.long) { (target, other, l) =>
          val lower    = TupleKeyLayout.prefixBytes(strLong, List(strCodec.encode(target)))
          val upper    = TupleKeyLayout.byteSuccessor(lower)
          val matching = enc2SL((target, l))
          val diff     = enc2SL((other, l))
          assertTrue(within(lower, upper, matching)) &&
          assertTrue(other == target || !within(lower, upper, diff))
        }
      },
      test("first-component equality bounds select exactly the matching tuple3 keys (variable in middle)") {
        check(Gen.long, Gen.long, Gen.string, Gen.long) { (target, other, s, b) =>
          val lower    = TupleKeyLayout.prefixBytes(longStrLong, List(longCodec.encode(target)))
          val upper    = TupleKeyLayout.byteSuccessor(lower)
          val matching = enc3LSL((target, s, b))
          val diff     = enc3LSL((other, s, b))
          assertTrue(within(lower, upper, matching)) &&
          assertTrue(other == target || !within(lower, upper, diff))
        }
      },
      test("two-component equality bounds select exactly the matching tuple3 keys (variable leading)") {
        check(Gen.string, Gen.long, Gen.string, Gen.long, Gen.long) { (s1, l1, s2, l2, tail) =>
          val lower    = TupleKeyLayout.prefixBytes(strLongLong, List(strCodec.encode(s1), longCodec.encode(l1)))
          val upper    = TupleKeyLayout.byteSuccessor(lower)
          val matching = enc3SLL((s1, l1, tail))
          val diff     = enc3SLL((s2, l2, tail))
          assertTrue(within(lower, upper, matching)) &&
          assertTrue(((s2, l2) == (s1, l1)) || !within(lower, upper, diff))
        }
      },
      test("range on second component after first-component equality") {
        // WHERE c1 = s AND c2 BETWEEN lo AND hi over (String, Long, Long) keys.
        check(Gen.string, Gen.long(-1000, 1000), Gen.long(-1000, 1000), Gen.long(-1000, 1000), Gen.long) { (s, a, b, probe, tail) =>
          val lo       = math.min(a, b)
          val hi       = math.max(a, b)
          val lower    = TupleKeyLayout.prefixBytes(strLongLong, List(strCodec.encode(s), longCodec.encode(lo)))
          val upper    = TupleKeyLayout.byteSuccessor(TupleKeyLayout.prefixBytes(strLongLong, List(strCodec.encode(s), longCodec.encode(hi))))
          val key      = enc3SLL((s, probe, tail))
          val expected = probe >= lo && probe <= hi
          assertTrue(within(lower, upper, key) == expected)
        }
      },
      test("ordering within a prefix group follows the remaining components") {
        check(Gen.string, Gen.long, Gen.long) { (s, a, b) =>
          val ka = enc2SL((s, a))
          val kb = enc2SL((s, b))
          assertTrue((Arrays.compareUnsigned(ka, kb) < 0) == (a < b))
        }
      }
    ),
    suite("byteSuccessor")(
      test("is strictly greater and minimal for extension") {
        check(Gen.listOf(Gen.byte).map(_.toArray)) { bytes =>
          TupleKeyLayout.byteSuccessor(bytes) match {
            case None       => assertTrue(bytes.forall(_ == -1))
            case Some(succ) =>
              val extended = bytes :+ 0x7f.toByte
              assertTrue(Arrays.compareUnsigned(succ, bytes) > 0) &&
              assertTrue(Arrays.compareUnsigned(extended, succ) < 0)
          }
        }
      }
    ),
    suite("decodeComponents")(
      test("recovers tuple2 component bytes (variable, fixed)") {
        check(Gen.string, Gen.long) { (s, l) =>
          val parts = TupleKeyLayout.decodeComponents(strLong, enc2SL((s, l)))
          assertTrue(parts.isRight) &&
          assert(parts.map(_.map(_.toList)))(isRight(equalTo(List(strCodec.encode(s).toList, longCodec.encode(l).toList))))
        }
      },
      test("recovers tuple3 component bytes (variable in middle)") {
        check(Gen.long, Gen.string, Gen.long) { (a, s, b) =>
          val parts = TupleKeyLayout.decodeComponents(longStrLong, enc3LSL((a, s, b)))
          assert(parts.map(_.map(_.toList)))(isRight(equalTo(List(longCodec.encode(a).toList, strCodec.encode(s).toList, longCodec.encode(b).toList))))
        }
      },
      test("recovers tuple3 component bytes (variable leading)") {
        check(Gen.string, Gen.long, Gen.long) { (s, a, b) =>
          val parts = TupleKeyLayout.decodeComponents(strLongLong, enc3SLL((s, a, b)))
          assert(parts.map(_.map(_.toList)))(isRight(equalTo(List(strCodec.encode(s).toList, longCodec.encode(a).toList, longCodec.encode(b).toList))))
        }
      },
      test("single-component layout returns the bytes unchanged") {
        check(Gen.string) { s =>
          val parts = TupleKeyLayout.decodeComponents(List(strCodec.width), strCodec.encode(s))
          assert(parts.map(_.map(_.toList)))(isRight(equalTo(List(strCodec.encode(s).toList))))
        }
      }
    )
  )
}
