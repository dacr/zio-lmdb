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
package zio.lmdb.sql.runtime

import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.keycodecs.ulid.ULIDCodec.given
import zio.lmdb.keycodecs.uuidv7.{UUIDv7, asUUID}
import zio.lmdb.keycodecs.uuidv7.UUIDv7Codec.given
import zio.lmdb.keycodecs.timestamp.TimestampCodec.given
import wvlet.airframe.ulid.ULID
import zio.lmdb.sql.engine.KeyValue
import zio.lmdb.sql.engine.KeyValue.*
import zio.lmdb.sql.parser.Literal

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import scala.util.Try

/** Resolves a persisted `keyId` to the operations the dynamic engine needs: decode bytes → a
  * [[KeyValue]] (for display/projection) and encode a SQL literal → key bytes (for `WHERE _key = …`
  * and `INSERT`). The registry is keyed by the codec id rather than by a static type, so the engine
  * never has to know `K`. Scalar built-ins and the common extension keys are wired; unknown ids
  * decode to an honest hex fallback rather than a wrong guess, and reject literal encoding.
  */
object KeyRegistry {

  private def viaCodec[A](codec: KeyCodec[A], bytes: Array[Byte])(wrap: A => KeyValue): KeyValue =
    codec.decode(ByteBuffer.wrap(bytes)) match {
      case Right(a) => wrap(a)
      case Left(_)  => KBytes(bytes)
    }

  /** Decode raw key bytes using the collection's recorded id. Never fails — unknown/one-way ids
    * fall back to a hex rendering.
    */
  def decode(keyId: String, bytes: Array[Byte]): KeyValue = keyId match {
    case "lmdb:str"           => viaCodec(KeyCodec.stringKeyCodec, bytes)(KStr(_))
    case "lmdb:int64"         => viaCodec(KeyCodec.longKeyCodec, bytes)(v => KLong(v))
    case "lmdb:int32"         => viaCodec(KeyCodec.intKeyCodec, bytes)(v => KLong(v.toLong))
    case "lmdb:int16"         => viaCodec(KeyCodec.shortKeyCodec, bytes)(v => KLong(v.toLong))
    case "lmdb:uuid"          => viaCodec(KeyCodec.uuidKeyCodec, bytes)(KUuid(_))
    case "lmdb:bytes"         => KBytes(bytes)
    case "lmdb-uuidv7:v1"     => viaCodec(summon[KeyCodec[UUIDv7]], bytes)(u => KUuid(u.asUUID))
    case "lmdb-ulid:v1"       => viaCodec(summon[KeyCodec[ULID]], bytes)(u => KStr(u.toString))
    case "lmdb-ts:instant/v1" => viaCodec(summon[KeyCodec[Instant]], bytes)(KInstant(_))
    case _                    => KBytes(bytes) // geo / uca / tuples: opaque display in v1
  }

  /** Encode a SQL literal into the key bytes for the given id, for equality lookups and inserts.
    * Returns a message for ids/literal shapes not yet supported.
    */
  def encodeLiteral(keyId: String, lit: Literal): Either[String, Array[Byte]] = (keyId, lit) match {
    case ("lmdb:str", Literal.StrLit(s))     => Right(KeyCodec.stringKeyCodec.encode(s))
    case ("lmdb:int64", Literal.IntLit(n))   => Right(KeyCodec.longKeyCodec.encode(n))
    case ("lmdb:int32", Literal.IntLit(n))   => Right(KeyCodec.intKeyCodec.encode(n.toInt))
    case ("lmdb:int16", Literal.IntLit(n))   => Right(KeyCodec.shortKeyCodec.encode(n.toShort))
    case ("lmdb:bytes", Literal.StrLit(s))   => Right(s.getBytes(UTF_8))
    case ("lmdb:uuid", Literal.StrLit(s))    =>
      Try(UUID.fromString(s)).toEither.left.map(_ => s"invalid UUID literal '$s'").map(KeyCodec.uuidKeyCodec.encode)
    case ("lmdb-uuidv7:v1", Literal.StrLit(s)) =>
      Try(UUID.fromString(s)).toEither.left.map(_ => s"invalid UUID literal '$s'").map(u => summon[KeyCodec[UUIDv7]].encode(UUIDv7(u)))
    case (other, _)                          =>
      Left(s"cannot use this literal as a '$other' key (unsupported key type or literal shape in v1)")
  }
}
