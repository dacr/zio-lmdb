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

import zio.lmdb.keycodecs.KeyCodec

import java.nio.ByteBuffer
import java.util.UUID
import com.github.f4b6a3.uuid.UuidCreator

import java.time.Instant

opaque type UUIDv7 = UUID

object UUIDv7 {
  def apply(uuid: UUID): UUIDv7 = uuid
  
  def generate(): UUIDv7 = UuidCreator.getTimeOrderedEpoch()

  def generate(instant: Instant): UUIDv7 = UuidCreator.getTimeOrderedEpoch(instant)

  extension (uuid: UUIDv7) {
    def toUUID: UUID = uuid
  }

  given KeyCodec[UUIDv7] = new KeyCodec[UUIDv7] {
    private val codec = zio.lmdb.keycodecs.KeyCodec.uuidKeyCodec

    override def encode(key: UUIDv7): Array[Byte] = codec.encode(key)

    override def decode(keyBytes: ByteBuffer): Either[String, UUIDv7] =
      codec.decode(keyBytes).map(UUIDv7(_))
  }
}
