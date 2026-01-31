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
package zio.lmdb.keycodecs.ulid

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.ulid.ULIDCodec.given
import wvlet.airframe.ulid.ULID
import java.nio.ByteBuffer

object ULIDCodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec[ULID] spec")(
    test("roundtrip encoding/decoding") {
      val ulid = ULID.newULID
      val codec = summon[KeyCodec[ULID]]
      val encoded = codec.encode(ulid)
      val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
      val decoded = codec.decode(buffer)
      
      assert(decoded)(isRight(equalTo(ulid)))
    },
    test("decoding fails when buffer has insufficient bytes") {
      val codec = summon[KeyCodec[ULID]]
      val buffer = ByteBuffer.allocateDirect(15) // ULID requires 16 bytes
      val decoded = codec.decode(buffer)
      assert(decoded)(isLeft(containsString("Not enough bytes for ULID")))
    }
  )
}
