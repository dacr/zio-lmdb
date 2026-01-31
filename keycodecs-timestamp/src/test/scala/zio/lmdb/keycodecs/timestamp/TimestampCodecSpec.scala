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
package zio.lmdb.keycodecs.timestamp

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.timestamp.TimestampCodec.given
import java.time.Instant
import java.util.Arrays
import java.nio.ByteBuffer

object TimestampCodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec[Instant] spec")(
    test("roundtrip encoding/decoding") {
      check(Gen.instant) { instant =>
        val codec = summon[KeyCodec[Instant]]
        val encoded = codec.encode(instant)
        val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
        val decoded = codec.decode(buffer)
        
        assert(decoded)(isRight(equalTo(instant)))
      }
    },
    test("lexicographical sorting") {
      val codec = summon[KeyCodec[Instant]]
      def compare(a: Array[Byte], b: Array[Byte]): Int = Arrays.compareUnsigned(a, b)
      
      // 1. Specific edge cases
      val t1 = Instant.MIN
      val t2 = Instant.ofEpochSecond(-100, 0)
      val t3 = Instant.EPOCH
      val t4 = Instant.ofEpochSecond(0, 1) // 1 nano after epoch
      val t5 = Instant.now()
      val t6 = Instant.MAX

      val instants = List(t1, t2, t3, t4, t5, t6)
      val encoded = instants.map(codec.encode)

      // Verify that the list is sorted in byte representation
      val isSortedInBytes = encoded.zip(encoded.tail).forall { case (a, b) =>
        compare(a, b) < 0
      }
      assertTrue(isSortedInBytes)

      // 2. Random data verification
      check(Gen.setOfN(100)(Gen.instant)) { randomInstantsSet =>
        val sortedInstants = randomInstantsSet.toList.sorted
        val encodedBytes = sortedInstants.map(codec.encode)
        
        // Sort using byte comparison
        val sortedEncodedBytes = encodedBytes.sorted(Ordering.fromLessThan((a, b) => compare(a, b) < 0))
        
        // Decode sorted bytes
        val decodedInstants = sortedEncodedBytes.map(bytes => 
          codec.decode(ByteBuffer.wrap(bytes)).getOrElse(throw new RuntimeException("decode failed"))
        )
        
        // The decoded instants should be in the same order as the original sorted instants
        assertTrue(decodedInstants == sortedInstants)
      }
    },
    test("decoding fails when buffer has insufficient bytes") {
      val codec = summon[KeyCodec[Instant]]
      val buffer = ByteBuffer.allocateDirect(11) // Instant requires 12 bytes
      val decoded = codec.decode(buffer)
      assert(decoded)(isLeft(containsString("Not enough bytes for Instant")))
    }
  )
}