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
    )
  )
}