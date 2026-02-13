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
package zio.lmdb.keycodecs.uuidv7

import zio.*
import zio.test.*
import zio.test.Assertion.*
import _root_.zio.lmdb.keycodecs.KeyCodec
import java.nio.ByteBuffer
import java.time.Instant

object UUIDv7CodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec[UUIDv7] spec")(
    test("roundtrip encoding/decoding") {
      val uuidv7 = UUIDv7.generate()
      val codec = UUIDv7Codec.given_KeyCodec_UUIDv7
      val encoded = codec.encode(uuidv7)
      val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
      val decoded = codec.decode(buffer)
      
      assert(decoded)(isRight(equalTo(uuidv7)))
    },
    test("generated UUIDs are version 7") {
      val uuidv7 = UUIDv7.generate()
      assertTrue(uuidv7.asUUID.version() == 7)
    },
    test("generate(instant) produces UUIDv7 with correct timestamp") {
      val instant = Instant.ofEpochMilli(1700000000000L)
      val uuidv7 = UUIDv7.generate(instant)
      assertTrue(uuidv7.asUUID.version() == 7)
      val timestamp = uuidv7.asUUID.getMostSignificantBits >>> 16
      assertTrue(timestamp == instant.toEpochMilli)
    }
  )
}