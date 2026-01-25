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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import zio.json.ast.Json
import zio.json.ast.Json.*

package object json {

  private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

  /** Implicit codec for `zio.json.ast.Json`. */
  implicit val jsonCodec: LMDBCodec[Json] = new LMDBCodec {
    def encode(t: Json): Array[Byte] = Json.encoder.encodeJson(t).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Json] = Json.decoder.decodeJson(charset.decode(bytes))
  }

  /** Implicit codec for `String` using JSON representation. */
  implicit val stringCodec: LMDBCodec[String] = new LMDBCodec {
    def encode(t: String): Array[Byte] = Str.encoder.encodeJson(Str(t)).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, String] = Str.decoder.decodeJson(charset.decode(bytes)).map(_.value)
  }

  /** Implicit codec for `zio.json.ast.Json.Str`. */
  implicit val strCodec: LMDBCodec[Str] = new LMDBCodec {
    def encode(t: Str): Array[Byte] = Str.encoder.encodeJson(t).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Str] = Str.decoder.decodeJson(charset.decode(bytes))
  }

  /** Implicit codec for `Int` using JSON representation. */
  implicit val intCodec: LMDBCodec[Int] = new LMDBCodec {
    def encode(t: Int): Array[Byte] = Num.encoder.encodeJson(Num(t)).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Int] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.intValue())
  }

  /** Implicit codec for `Double` using JSON representation. */
  implicit val doubleCodec: LMDBCodec[Double] = new LMDBCodec {
    def encode(t: Double): Array[Byte] = Num.encoder.encodeJson(Num(t)).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Double] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.doubleValue())
  }

  /** Implicit codec for `Float` using JSON representation. */
  implicit val floatCodec: LMDBCodec[Float] = new LMDBCodec {
    def encode(t: Float): Array[Byte] = Num.encoder.encodeJson(Num(t)).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Float] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.floatValue())
  }

  /** Implicit codec for `zio.json.ast.Json.Num`. */
  implicit val numCodec: LMDBCodec[Num] = new LMDBCodec {
    def encode(t: Num): Array[Byte] = Num.encoder.encodeJson(t).toString.getBytes

    def decode(bytes: ByteBuffer): Either[String, Num] = Num.decoder.decodeJson(charset.decode(bytes))
  }

}
