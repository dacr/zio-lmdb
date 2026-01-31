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
import zio.test.*
import zio.test.Assertion.*
import java.nio.ByteBuffer
import java.util.UUID
import java.util.Arrays

object KeyCodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec spec")(
    suite("String codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.string) { str =>
          val codec = summon[KeyCodec[String]]
          val encoded = codec.encode(str)
          val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(decoded)(isRight(equalTo(str)))
        }
      }
    ),
    suite("UUID codec")(
      test("roundtrip encoding/decoding") {
        check(Gen.uuid) { uuid =>
          val codec = summon[KeyCodec[UUID]]
          val encoded = codec.encode(uuid)
          val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
          val decoded = codec.decode(buffer)
          assert(decoded)(isRight(equalTo(uuid)))
        }
      },
      test("decoding fails when buffer has insufficient bytes") {
        val codec = summon[KeyCodec[UUID]]
        val buffer = ByteBuffer.allocateDirect(15) // UUID requires 16 bytes
        val decoded = codec.decode(buffer)
        assert(decoded)(isLeft(containsString("Not enough bytes for UUID")))
      }
    ),
    suite("Tuple2 codec")(
      test("Fixed-Fixed (UUID, UUID)") {
        check(Gen.uuid, Gen.uuid) { (u1, u2) =>
          val codec = summon[KeyCodec[(UUID, UUID)]]
          val tuple = (u1, u2)
          val encoded = codec.encode(tuple)
          
          // Should be exactly 32 bytes (16 + 16)
          assert(encoded.length)(equalTo(32)) &&
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Variable-Fixed (String, UUID) with escaping") {
        check(Gen.string, Gen.uuid) { (s, u) =>
          val codec = summon[KeyCodec[(String, UUID)]]
          val tuple = (s, u)
          val encoded = codec.encode(tuple)
          
          // Decode check
          assert(codec.decode(ByteBuffer.wrap(encoded)))(isRight(equalTo(tuple)))
        }
      },
      test("Fixed-Variable (UUID, String)") {
        check(Gen.uuid, Gen.string) { (u, s) =>
          val codec = summon[KeyCodec[(UUID, String)]]
          val tuple = (u, s)
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
          val codec = summon[KeyCodec[(String, String)]]
          val tuple = (s1, s2)
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
        val b1 = codec.encode((s1, ""))
        val b2 = codec.encode((s2, ""))
        val b3 = codec.encode((s3, ""))
        
        def compare(x: Array[Byte], y: Array[Byte]): Int = Arrays.compareUnsigned(x, y)
        
        assertTrue(compare(b1, b2) < 0) &&
        assertTrue(compare(b2, b3) < 0)
      },
      test("Escaping correct roundtrip with nulls") {
        val s1 = "Hello\u0000World"
        val u1 = UUID.randomUUID()
        val codec = summon[KeyCodec[(String, UUID)]]
        
        val encoded = codec.encode((s1, u1))
        val decoded = codec.decode(ByteBuffer.wrap(encoded))
        
        assert(decoded)(isRight(equalTo((s1, u1))))
      }
    )
  )
}
