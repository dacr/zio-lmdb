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
import java.util.Arrays

object GEOToolsSpec extends ZIOSpecDefault {

  def spec = suite("GEOTools spec")(
    test("roundtrip conversion") {
      check(Gen.double(-90.0, 90.0), Gen.double(-180.0, 180.0)) { (lat, lon) =>
        val loc     = GEOTools.Location(latitude = lat, longitude = lon)
        val bytes   = GEOTools.locationToBytes(loc)
        val decoded = GEOTools.bytesToLocation(bytes)

        // Precision loss is expected due to integer quantization
        val latError = Math.abs(lat - decoded.latitude)
        val lonError = Math.abs(lon - decoded.longitude)

        // 32-bit precision roughly equates to ~4cm at equator for lat, ~8cm for lon
        // 180 / 2^32 approx 4.19e-8 degrees
        // 360 / 2^32 approx 8.38e-8 degrees
        assertTrue(
          latError < 1.0e-7,
          lonError < 1.0e-7
        )
      }
    },
    test("lexicographical ordering logic") {
      // Points sorted by Z-order (interleaved bits) should respect byte comparison

      val p1 = GEOTools.Location(latitude = 0.0, longitude = 0.0)
      val p2 = GEOTools.Location(latitude = 1.0, longitude = 1.0)

      val b1 = GEOTools.locationToBytes(p1)
      val b2 = GEOTools.locationToBytes(p2)

      // b2 should be greater than b1 because coordinates are larger (and positive)
      // Comparison logic for unsigned bytes
      def compare(a: Array[Byte], b: Array[Byte]): Int = Arrays.compareUnsigned(a, b)

      assertTrue(
        compare(b1, b2) < 0
      )
    },
    test("masking example") {
      // Just to verify the concept of masking for regions
      val loc   = GEOTools.Location(latitude = 48.8566, longitude = 2.3522) // Paris
      val bytes = GEOTools.locationToBytes(loc)

      // Masking lower 4 bytes (keeping high 32 bits, rough region)
      val masked = bytes.clone()
      for (i <- 4 until 8) masked(i) = 0

      val regionLoc = GEOTools.bytesToLocation(masked)

      // The region location should be "lower-left" (or similar corner) of the grid cell
      // defined by the mask.
      assertTrue(
        regionLoc.latitude <= loc.latitude,
        regionLoc.longitude <= loc.longitude
      )
    }
  )
}
