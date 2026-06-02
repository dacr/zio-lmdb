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

import java.nio.ByteBuffer

object GEOTools {
  // Scala 3.3.7 does not support experimental Named Tuples yet, so we use a case class
  // which provides the same functionality (named fields) and performance.
  final case class Location(latitude: Double, longitude: Double)

  /** Converts a latitude/longitude pair to an 8-byte array using Morton coding (Z-order curve). The result is lexicographically sortable.
    *
    * @param location
    *   The latitude and longitude
    * @return
    *   An 8-byte array representing the Morton code
    */
  def locationToBytes(location: Location): Array[Byte] = {
    val lat = location.latitude
    val lon = location.longitude

    // Normalize coordinates to 32-bit integers
    // Latitude: [-90, 90] -> [0, 2^32 - 1]
    // Longitude: [-180, 180] -> [0, 2^32 - 1]
    val latInt = ((lat + 90.0) / 180.0 * 4294967295.0).toLong
    val lonInt = ((lon + 180.0) / 360.0 * 4294967295.0).toLong

    // Interleave bits
    var morton = 0L
    for (i <- 0 until 32) {
      val latBit = (latInt >>> i) & 1L
      val lonBit = (lonInt >>> i) & 1L
      morton |= (lonBit << (2 * i)) | (latBit << (2 * i + 1))
    }

    // Convert to 8-byte array (Big Endian for lexicographical sorting)
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(morton)
    buffer.array()
  }

  /** Converts an 8-byte array (Morton code) back to a latitude/longitude pair.
    *
    * @param bytes
    *   The 8-byte array
    * @return
    *   The latitude and longitude
    */
  def bytesToLocation(bytes: Array[Byte]): Location = {
    val buffer = ByteBuffer.wrap(bytes)
    val morton = buffer.getLong

    var latInt = 0L
    var lonInt = 0L

    for (i <- 0 until 32) {
      val latBit = (morton >>> (2 * i + 1)) & 1L
      val lonBit = (morton >>> (2 * i)) & 1L
      latInt |= (latBit << i)
      lonInt |= (lonBit << i)
    }

    // Denormalize coordinates to the MIDPOINT of the Morton bin so that
    // re-encoding the decoded Location reliably yields the same long.
    // Using the lower bound (latInt/4294967295*180-90) is unstable: IEEE 754
    // rounding of (lat+90)/180*4294967295 can fall just below latInt and
    // .toLong then truncates to latInt-1, breaking head()→indexed(firstKey)
    // walks that depend on encode∘decode∘encode == encode.
    val lat = ((latInt.toDouble + 0.5) / 4294967295.0) * 180.0 - 90.0
    val lon = ((lonInt.toDouble + 0.5) / 4294967295.0) * 360.0 - 180.0

    Location(latitude = lat, longitude = lon)
  }
}
