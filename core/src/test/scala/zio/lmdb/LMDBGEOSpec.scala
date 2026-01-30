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
package zio.lmdb

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.lmdb.keytools.GEOTools
import java.nio.ByteBuffer

object LMDBGEOSpec extends ZIOSpecDefault {

  def spec = suite("LMDBKodec[Location] spec")(
    test("roundtrip encoding/decoding") {
      val loc = GEOTools.Location(latitude = 48.8566, longitude = 2.3522)
      val codec = summon[LMDBKodec[GEOTools.Location]]
      val encoded = codec.encode(loc)
      val buffer = ByteBuffer.allocateDirect(encoded.length).put(encoded).flip()
      val decoded = codec.decode(buffer)
      
      assert(decoded)(isRight(assertion("matching location")(d => {
        Math.abs(d.latitude - loc.latitude) < 1e-7 &&
        Math.abs(d.longitude - loc.longitude) < 1e-7
      })))
    }
  )
}
