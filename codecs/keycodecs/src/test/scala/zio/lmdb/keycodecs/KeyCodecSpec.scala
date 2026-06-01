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

import zio.*
import zio.lmdb.keycodecs.KeyCodecError
import zio.test.*
import zio.test.Assertion.*

import java.nio.ByteBuffer
import java.util.{Arrays, UUID}

object KeyCodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec spec")(
    suite("String codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.string) { str =>
          val codec   = summon[KeyCodec[String]]
          val encoded = codec.encode(str)
          val buffer  = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(decoded)(isRight(equalTo(str)))
        }
      }
    ),
    suite("Long codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.long) { l =>
          val codec   = summon[KeyCodec[Long]]
          val encoded = codec.encode(l)
          val buffer  = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(encoded.length)(equalTo(8)) &&
          assert(decoded)(isRight(equalTo(l)))
        }
      },
      test("byte-order matches numeric order") {
        check(Gen.long, Gen.long) { (a, b) =>
          val codec = summon[KeyCodec[Long]]
          val ea    = codec.encode(a)
          val eb    = codec.encode(b)
          val cmp   = Arrays.compareUnsigned(ea, eb)
          assertTrue((cmp < 0) == (a < b)) &&
          assertTrue((cmp > 0) == (a > b)) &&
          assertTrue((cmp == 0) == (a == b))
        }
      },
      test("decoding fails when buffer has insufficient bytes") {
        val codec   = summon[KeyCodec[Long]]
        val buffer  = ByteBuffer.allocateDirect(7)
        val decoded = codec.decode(buffer)
        assert(decoded)(isLeft(equalTo(KeyCodecError.InsufficientBytes(8, 7))))
      }
    ),
    suite("Int codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.int) { i =>
          val codec   = summon[KeyCodec[Int]]
          val encoded = codec.encode(i)
          val buffer  = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(encoded.length)(equalTo(4)) &&
          assert(decoded)(isRight(equalTo(i)))
        }
      },
      test("byte-order matches numeric order") {
        check(Gen.int, Gen.int) { (a, b) =>
          val codec = summon[KeyCodec[Int]]
          val ea    = codec.encode(a)
          val eb    = codec.encode(b)
          val cmp   = Arrays.compareUnsigned(ea, eb)
          assertTrue((cmp < 0) == (a < b)) &&
          assertTrue((cmp > 0) == (a > b)) &&
          assertTrue((cmp == 0) == (a == b))
        }
      },
      test("decoding fails when buffer has insufficient bytes") {
        val codec   = summon[KeyCodec[Int]]
        val buffer  = ByteBuffer.allocateDirect(3)
        val decoded = codec.decode(buffer)
        assert(decoded)(isLeft(equalTo(KeyCodecError.InsufficientBytes(4, 3))))
      }
    ),
    suite("Short codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.short) { s =>
          val codec   = summon[KeyCodec[Short]]
          val encoded = codec.encode(s)
          val buffer  = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(encoded.length)(equalTo(2)) &&
          assert(decoded)(isRight(equalTo(s)))
        }
      },
      test("byte-order matches numeric order") {
        check(Gen.short, Gen.short) { (a, b) =>
          val codec = summon[KeyCodec[Short]]
          val ea    = codec.encode(a)
          val eb    = codec.encode(b)
          val cmp   = Arrays.compareUnsigned(ea, eb)
          assertTrue((cmp < 0) == (a < b)) &&
          assertTrue((cmp > 0) == (a > b)) &&
          assertTrue((cmp == 0) == (a == b))
        }
      },
      test("decoding fails when buffer has insufficient bytes") {
        val codec   = summon[KeyCodec[Short]]
        val buffer  = ByteBuffer.allocateDirect(1)
        val decoded = codec.decode(buffer)
        assert(decoded)(isLeft(equalTo(KeyCodecError.InsufficientBytes(2, 1))))
      }
    ),
    suite("UUID codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.uuid) { uuid =>
          val codec   = summon[KeyCodec[UUID]]
          val encoded = codec.encode(uuid)
          val buffer  = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(decoded)(isRight(equalTo(uuid)))
        }
      },
      test("decoding fails when buffer has insufficient bytes") {
        val codec   = summon[KeyCodec[UUID]]
        val buffer  = ByteBuffer.allocateDirect(15) // UUID requires 16 bytes
        val decoded = codec.decode(buffer)
        assert(decoded)(isLeft(equalTo(KeyCodecError.InsufficientBytes(16, 15))))
      }
    ),
    suite("Tuple2 codec")(
      test("Fixed-Fixed (UUID, UUID)") {
        check(Gen.uuid, Gen.uuid) { (u1, u2) =>
          val codec   = summon[KeyCodec[(UUID, UUID)]]
          val tuple   = (u1, u2)
          val encoded = codec.encode(tuple)

          // Should be exactly 32 bytes (16 + 16)
          assert(encoded.length)(equalTo(32)) &&
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Variable-Fixed (String, UUID) with escaping") {
        check(Gen.string, Gen.uuid) { (s, u) =>
          val codec   = summon[KeyCodec[(String, UUID)]]
          val tuple   = (s, u)
          val encoded = codec.encode(tuple)

          // Decode check
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Fixed-Variable (UUID, String)") {
        check(Gen.uuid, Gen.string) { (u, s) =>
          val codec   = summon[KeyCodec[(UUID, String)]]
          val tuple   = (u, s)
          val encoded = codec.encode(tuple)

          // Length should be 16 + string UTF8 length
          // No separator needed for fixed first component
          val sBytes = s.getBytes("UTF-8")
          assert(encoded.length)(equalTo(16 + sBytes.length)) &&
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Variable-Variable (String, String) with escaping") {
        check(Gen.string, Gen.string) { (s1, s2) =>
          val codec   = summon[KeyCodec[(String, String)]]
          val tuple   = (s1, s2)
          val encoded = codec.encode(tuple)

          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Escaping logic preserves order (String, String)") {
        // "A" < "A\0" < "B"
        // Encoded:
        // "A" -> "A" 0x00
        // "A\0" -> "A" 0x00 0xFF 0x00
        // "B" -> "B" 0x00

        val s1 = "A"
        val s2 = "A\u0000" // A null
        val s3 = "B"

        val codec = summon[KeyCodec[(String, String)]]
        // Second component empty for simplicity
        val b1    = codec.encode((s1, ""))
        val b2    = codec.encode((s2, ""))
        val b3    = codec.encode((s3, ""))

        def compare(x: Array[Byte], y: Array[Byte]): Int = Arrays.compareUnsigned(x, y)

        assertTrue(compare(b1, b2) < 0) &&
        assertTrue(compare(b2, b3) < 0)
      },
      test("Escaping correct roundtrip with nulls") {
        val s1    = "Hello\u0000World"
        val u1    = UUID.randomUUID()
        val codec = summon[KeyCodec[(String, UUID)]]

        val encoded = codec.encode((s1, u1))
        val decoded = codec.decode(ByteBuffer.wrap(encoded))

        assert(decoded)(isRight(equalTo((s1, u1))))
      }
    ),
    suite("Tuple3 codec")(
      test("All-Fixed (UUID, Int, Long) roundtrip and width") {
        check(Gen.uuid, Gen.int, Gen.long) { (u, i, l) =>
          val codec   = summon[KeyCodec[(UUID, Int, Long)]]
          val tuple   = (u, i, l)
          val encoded = codec.encode(tuple)
          assert(codec.width)(isSome(equalTo(16 + 4 + 8))) &&
          assert(encoded.length)(equalTo(28)) &&
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Variable-Fixed-Fixed (String, UUID, Int) roundtrip") {
        check(Gen.string, Gen.uuid, Gen.int) { (s, u, i) =>
          val codec   = summon[KeyCodec[(String, UUID, Int)]]
          val tuple   = (s, u, i)
          val encoded = codec.encode(tuple)
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Fixed-Variable-Fixed (UUID, String, Long) roundtrip") {
        check(Gen.uuid, Gen.string, Gen.long) { (u, s, l) =>
          val codec   = summon[KeyCodec[(UUID, String, Long)]]
          val tuple   = (u, s, l)
          val encoded = codec.encode(tuple)
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("All-Variable (String, String, String) roundtrip with embedded nulls") {
        val s1      = "Hello World"
        val s2      = " begins"
        val s3      = "ends "
        val codec   = summon[KeyCodec[(String, String, String)]]
        val encoded = codec.encode((s1, s2, s3))
        assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo((s1, s2, s3))))
      },
      test("encode((a, b, c)) shares a prefix with encode((a, b)) (Fixed-Fixed-*)") {
        // Property that makes prefix scans cursor-friendly: when (a, b) has fixed
        // width the first 20 bytes of encode((u, i, *)) equal encode((u, i)).
        val u   = UUID.randomUUID()
        val i   = 42
        val l1  = 1L
        val l2  = 2L
        val ck2 = summon[KeyCodec[(UUID, Int)]]
        val ck3 = summon[KeyCodec[(UUID, Int, Long)]]
        val e12 = ck2.encode((u, i))
        val e1  = ck3.encode((u, i, l1))
        val e2  = ck3.encode((u, i, l2))
        assertTrue(Arrays.equals(e1.take(e12.length), e12)) &&
        assertTrue(Arrays.equals(e2.take(e12.length), e12))
      },
      test("byte order preserved on lexicographic suffix when prefix is shared") {
        val u   = UUID.randomUUID()
        val s   = "shared"
        val ck3 = summon[KeyCodec[(UUID, String, Long)]]
        check(Gen.long, Gen.long) { (l1, l2) =>
          val e1  = ck3.encode((u, s, l1))
          val e2  = ck3.encode((u, s, l2))
          val cmp = Arrays.compareUnsigned(e1, e2)
          assertTrue((cmp < 0) == (l1 < l2)) &&
          assertTrue((cmp == 0) == (l1 == l2))
        }
      }
    ),
    suite("Tuple4 codec")(
      test("All-Fixed (UUID, Int, Long, Short) roundtrip and width") {
        check(Gen.uuid, Gen.int, Gen.long, Gen.short) { (u, i, l, s) =>
          val codec   = summon[KeyCodec[(UUID, Int, Long, Short)]]
          val tuple   = (u, i, l, s)
          val encoded = codec.encode(tuple)
          assert(codec.width)(isSome(equalTo(16 + 4 + 8 + 2))) &&
          assert(encoded.length)(equalTo(30)) &&
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Mixed widths (String, UUID, String, Long) roundtrip") {
        check(Gen.string, Gen.uuid, Gen.string, Gen.long) { (s1, u, s2, l) =>
          val codec   = summon[KeyCodec[(String, UUID, String, Long)]]
          val tuple   = (s1, u, s2, l)
          val encoded = codec.encode(tuple)
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("encode((a, b, c, d)) shares a prefix with encode((a, b, c))") {
        val u   = UUID.randomUUID()
        val i   = 7
        val l   = 99L
        val ck3 = summon[KeyCodec[(UUID, Int, Long)]]
        val ck4 = summon[KeyCodec[(UUID, Int, Long, Short)]]
        val e3  = ck3.encode((u, i, l))
        val e4a = ck4.encode((u, i, l, 1.toShort))
        val e4b = ck4.encode((u, i, l, 2.toShort))
        assertTrue(Arrays.equals(e4a.take(e3.length), e3)) &&
        assertTrue(Arrays.equals(e4b.take(e3.length), e3))
      }
    )
  )
}
