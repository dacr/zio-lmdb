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
package zio.lmdb.json

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import zio.lmdb.LMDBCodec

import java.nio.ByteBuffer

/** A combined codec that provides both LMDB and JSON serialization for type `T`, powered by
  * jsoniter-scala.
  *
  * The trait exposes the underlying `JsonValueCodec[T]` so callers can reach into jsoniter's
  * `writeToString` / `readFromString` / `writeToArray` / `readFromArray` directly when they
  * need the raw JSON text (e.g. for debugging, for testing, or for storing JSON as bytes
  * outside of an LMDB collection).
  *
  * @tparam T the data class type
  */
trait LMDBCodecJson[T] extends LMDBCodec[T] {
  /** The underlying jsoniter-scala value codec. Exposed so callers can drive `writeToString`,
    * `readFromString`, etc. without going through `LMDBCodec.encode` / `LMDBCodec.decode`.
    */
  def valueCodec: JsonValueCodec[T]

  /** Encode the value to a JSON byte array. */
  override final def encode(value: T): Array[Byte] = writeToArray(value)(valueCodec)

  /** Decode the value from a `ByteBuffer` view of JSON bytes. */
  override final def decode(bytes: ByteBuffer): Either[String, T] =
    try Right(readFromByteBuffer(bytes)(valueCodec))
    catch { case t: Throwable => Left(t.getMessage) }
}

object LMDBCodecJson {

  /** Wrap an existing `JsonValueCodec[T]` as an `LMDBCodecJson[T]`. */
  def apply[T](codec: JsonValueCodec[T]): LMDBCodecJson[T] = new LMDBCodecJson[T] {
    val valueCodec: JsonValueCodec[T] = codec
  }

  /** Auto-derive an `LMDBCodecJson[T]` from `T`'s structure via jsoniter-scala's macro.
    *
    * Reached at the call site as `case class Foo(...) derives LMDBCodecJson` or
    * `LMDBCodecJson.derived[Foo]`. The macro is inlined; no runtime reflection occurs.
    */
  inline def derived[T]: LMDBCodecJson[T] = apply(JsonCodecMaker.make[T])

  /** Lowest-priority `given` so any `T` for which the macro can produce a codec is
    * automatically usable as `LMDBCodec[T]`.
    */
  inline given [T]: LMDBCodecJson[T] = derived

  /** Convenience: encode a typed value to its JSON `String` representation. Used by tests
    * and debug paths that previously called zio-json's `.toJson` extension method.
    */
  def toJsonString[T](value: T)(using codec: LMDBCodecJson[T]): String =
    writeToString(value)(codec.valueCodec)

  /** Convenience: decode a JSON `String` into a typed value. Mirrors the test-time use of
    * zio-json's `.fromJson[T]`.
    */
  def fromJsonString[T](json: String)(using codec: LMDBCodecJson[T]): Either[String, T] =
    try Right(readFromString(json)(codec.valueCodec))
    catch { case t: Throwable => Left(t.getMessage) }
}
