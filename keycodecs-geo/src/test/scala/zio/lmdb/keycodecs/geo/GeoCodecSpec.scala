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
package zio.lmdb.keycodecs.geo

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.geo.GeoCodec.given
import java.nio.ByteBuffer

object GeoCodecSpec extends ZIOSpecDefault {

  def spec = suite("KeyCodec[Location] spec")(
    test("roundtrip encoding/decoding") {
      val loc = GEOTools.Location(latitude = 48.8566, longitude = 2.3522)
      val codec = summon[KeyCodec[GEOTools.Location]]
      val encoded = codec.encode(loc)
      val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
      val decoded = codec.decode(buffer)
      
      assert(decoded)(isRight(assertion("matching location")(d => {
        Math.abs(d.latitude - loc.latitude) < 1e-7 &&
        Math.abs(d.longitude - loc.longitude) < 1e-7
      })))
    },
    test("decoding fails when buffer has insufficient bytes") {
      val codec = summon[KeyCodec[GEOTools.Location]]
      val buffer = ByteBuffer.allocateDirect(7) // Location requires 8 bytes
      val decoded = codec.decode(buffer)
      assert(decoded)(isLeft(containsString("Not enough bytes for Location")))
    }
  )
}
