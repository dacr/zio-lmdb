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

import zio.lmdb.keycodecs.{KeyCodec, TupleKeyLayout}
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

/** Resolves a persisted `keyId` to the operations the dynamic engine needs: decode bytes → a [[KeyValue]] (for display/projection) and encode a SQL literal → key bytes (for `WHERE _key = …` and `INSERT`). The registry is keyed by the codec id rather
  * than by a static type, so the engine never has to know `K`. Scalar built-ins and the common extension keys are wired; unknown ids decode to an honest hex fallback rather than a wrong guess, and reject literal encoding.
  */
object KeyRegistry {

  private def viaCodec[A](codec: KeyCodec[A], bytes: Array[Byte])(wrap: A => KeyValue): KeyValue =
    codec.decode(ByteBuffer.wrap(bytes)) match {
      case Right(a) => wrap(a)
      case Left(_)  => KBytes(bytes)
    }

  /** The component ids of a `lmdb:tuple(a,b,…)` id (splitting at top-level commas only), or `None` for a scalar id.
    */
  def tupleComponentIds(keyId: String): Option[List[String]] =
    if (!keyId.startsWith("lmdb:tuple(") || !keyId.endsWith(")")) None
    else {
      val inner = keyId.substring("lmdb:tuple(".length, keyId.length - 1)
      val parts = List.newBuilder[String]
      val cur   = new StringBuilder
      var depth = 0
      inner.foreach {
        case '('               => depth += 1; cur.append('(')
        case ')'               => depth -= 1; cur.append(')')
        case ',' if depth == 0 => parts += cur.result(); cur.clear()
        case c                 => cur.append(c)
      }
      parts += cur.result()
      Some(parts.result())
    }

  /** Fixed byte width of a scalar id the registry knows (`Some(None)` = known variable-width, `None` = unknown id).
    */
  def knownWidth(keyId: String): Option[Option[Int]] = keyId match {
    case "lmdb:str" | "lmdb:bytes" | "lmdb-uca:sortkey/v1" => Some(None)
    case "lmdb:int64"                                      => Some(Some(8))
    case "lmdb:int32"                                      => Some(Some(4))
    case "lmdb:int16"                                      => Some(Some(2))
    case "lmdb:uuid" | "lmdb-uuidv7:v1" | "lmdb-ulid:v1"   => Some(Some(16))
    case "lmdb-ts:instant/v1"                              => Some(Some(12))
    case "lmdb-geo:location/v1"                            => Some(Some(8))
    case _                                                 => None
  }

  /** Decode raw key bytes using the collection's recorded id. Never fails — unknown/one-way ids fall back to a hex rendering.
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
    case _                    =>
      tupleComponentIds(keyId).flatMap(decodeTuple(_, bytes)).getOrElse(KBytes(bytes)) // geo / uca: opaque display in v1
  }

  /** Split a composite key into its components and decode each one; `None` when any component id's width is unknown (so the layout cannot be derived from the id alone).
    */
  private def decodeTuple(componentIds: List[String], bytes: Array[Byte]): Option[KeyValue] =
    for {
      widths <- componentIds.foldRight(Option(List.empty[Option[Int]]))((id, acc) => acc.flatMap(ws => knownWidth(id).map(_ :: ws)))
      parts  <- TupleKeyLayout.decodeComponents(widths, bytes).toOption
    } yield KTuple(componentIds.zip(parts).map { case (id, b) => decode(id, b) })

  /** Encode a SQL literal into the key bytes for the given id, for equality lookups and inserts. Returns a message for ids/literal shapes not yet supported.
    */
  def encodeLiteral(keyId: String, lit: Literal): Either[String, Array[Byte]] = (keyId, lit) match {
    case ("lmdb:str", Literal.StrLit(s))       => Right(KeyCodec.stringKeyCodec.encode(s))
    case ("lmdb:int64", Literal.IntLit(n))     => Right(KeyCodec.longKeyCodec.encode(n))
    case ("lmdb:int32", Literal.IntLit(n))     => Right(KeyCodec.intKeyCodec.encode(n.toInt))
    case ("lmdb:int16", Literal.IntLit(n))     => Right(KeyCodec.shortKeyCodec.encode(n.toShort))
    case ("lmdb:bytes", Literal.StrLit(s))     => Right(s.getBytes(UTF_8))
    case ("lmdb:uuid", Literal.StrLit(s))      =>
      Try(UUID.fromString(s)).toEither.left.map(_ => s"invalid UUID literal '$s'").map(KeyCodec.uuidKeyCodec.encode)
    case ("lmdb-uuidv7:v1", Literal.StrLit(s)) =>
      Try(UUID.fromString(s)).toEither.left.map(_ => s"invalid UUID literal '$s'").map(u => summon[KeyCodec[UUIDv7]].encode(UUIDv7(u)))
    case (other, _)                            =>
      Left(s"cannot use this literal as a '$other' key (unsupported key type or literal shape in v1)")
  }

  /** Encode a SQL literal as one component of an index key, for the planner's scan bounds. A superset of [[encodeLiteral]]: also accepts timestamp components (ISO-8601 instant / offset-date-time / date, or an epoch-millis integer) and ULIDs.
    */
  def encodeComponent(keyId: String, lit: Literal): Either[String, Array[Byte]] = (keyId, lit) match {
    case ("lmdb-ts:instant/v1", Literal.StrLit(s)) =>
      parseInstantLiteral(s).map(summon[KeyCodec[Instant]].encode)
    case ("lmdb-ts:instant/v1", Literal.IntLit(n)) =>
      Right(summon[KeyCodec[Instant]].encode(Instant.ofEpochMilli(n)))
    case ("lmdb-ulid:v1", Literal.StrLit(s))       =>
      Try(ULID.fromString(s)).toEither.left.map(_ => s"invalid ULID literal '$s'").map(summon[KeyCodec[ULID]].encode)
    case _                                         => encodeLiteral(keyId, lit)
  }

  /** An instant, an offset date-time, or a plain date (taken at UTC midnight). */
  private def parseInstantLiteral(s: String): Either[String, Instant] =
    Try(Instant.parse(s))
      .orElse(Try(java.time.OffsetDateTime.parse(s).toInstant))
      .orElse(Try(java.time.LocalDate.parse(s).atStartOfDay(java.time.ZoneOffset.UTC).toInstant))
      .toEither
      .left
      .map(_ => s"invalid timestamp literal '$s' (expected ISO-8601 instant, offset date-time, or date)")
}
