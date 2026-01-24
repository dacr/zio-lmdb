/*
 * Copyright 2025 David Crosson
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
package zio.lmdb

import zio.*
import zio.test.*
import zio.test.Assertion.*
import java.util.UUID
import java.nio.ByteBuffer

object LMDBUUIDSpec extends ZIOSpecDefault {

  def spec = suite("LMDBKodec[UUID] spec")(
    test("roundtrip encoding/decoding") {
      val uuid = UUID.randomUUID()
      val codec = summon[LMDBKodec[UUID]]
      val encoded = codec.encode(uuid)
      val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
      val decoded = codec.decode(buffer)
      assert(decoded)(isRight(equalTo(uuid)))
    },
    test("lexical ordering matches string representation") {
      val uuids = List.fill(100)(UUID.randomUUID())
      val codec = summon[LMDBKodec[UUID]]
      
      val sortedByString = uuids.sortBy(_.toString)
      
      // Lexicographical byte array comparison
      implicit val byteArrayOrdering: Ordering[Array[Byte]] = (x: Array[Byte], y: Array[Byte]) => {
        val len = Math.min(x.length, y.length)
        var i = 0
        var res = 0
        while (i < len && res == 0) {
          res = (x(i) & 0xff).compare(y(i) & 0xff)
          i += 1
        }
        if (res == 0) x.length.compare(y.length) else res
      }
      
      val sortedByBytes = uuids.sortBy(codec.encode)
      
      assertTrue(sortedByString == sortedByBytes)
    }
  )
}
