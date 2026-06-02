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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

package object json {

  /** Implicit codec for `String` using JSON representation. */
  implicit val stringCodec: LMDBCodec[String] = LMDBCodecJson(JsonCodecMaker.make[String])

  /** Implicit codec for `Int` using JSON representation. */
  implicit val intCodec: LMDBCodec[Int] = LMDBCodecJson(JsonCodecMaker.make[Int])

  /** Implicit codec for `Long` using JSON representation. */
  implicit val longCodec: LMDBCodec[Long] = LMDBCodecJson(JsonCodecMaker.make[Long])

  /** Implicit codec for `Double` using JSON representation. */
  implicit val doubleCodec: LMDBCodec[Double] = LMDBCodecJson(JsonCodecMaker.make[Double])

  /** Implicit codec for `Float` using JSON representation. */
  implicit val floatCodec: LMDBCodec[Float] = LMDBCodecJson(JsonCodecMaker.make[Float])

  /** Implicit codec for `Boolean` using JSON representation. */
  implicit val booleanCodec: LMDBCodec[Boolean] = LMDBCodecJson(JsonCodecMaker.make[Boolean])

  /** Encode a typed value to its JSON `String` representation.
    * Same call shape as zio-json's `.toJson` extension method.
    */
  extension [T](value: T) {
    def toJson(using codec: LMDBCodecJson[T]): String = writeToString(value)(codec.valueCodec)
  }

  /** Decode a JSON `String` into a typed value.
    * Same call shape as zio-json's `.fromJson[T]` extension method.
    */
  extension (json: String) {
    def fromJson[T](using codec: LMDBCodecJson[T]): Either[String, T] =
      try Right(readFromString(json)(codec.valueCodec))
      catch { case t: Throwable => Left(t.getMessage) }
  }

}
