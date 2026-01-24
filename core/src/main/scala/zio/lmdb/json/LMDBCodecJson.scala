/*
 * Copyright 2025 David Crosson
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

import zio.json.internal.{RetractReader, Write}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonCodec, JsonDecoder, JsonEncoder, JsonError}
import zio.lmdb.LMDBCodec

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.deriving.Mirror

trait LMDBCodecJson[T] extends LMDBCodec[T] with JsonEncoder[T] with JsonDecoder[T]

object LMDBCodecJson {

  private def createCodec[T](encoder: JsonEncoder[T], decoder: JsonDecoder[T]): LMDBCodecJson[T] = {
    val charset = StandardCharsets.UTF_8  // TODO enhance charset support

    new LMDBCodecJson[T] {
      override def unsafeEncode(a: T, indent: Option[Int], out: Write): Unit  = encoder.unsafeEncode(a, indent, out)
      override def unsafeDecode(trace: List[JsonError], in: RetractReader): T = decoder.unsafeDecode(trace, in)

      def encode(t: T): Array[Byte]                    = encoder.encodeJson(t).toString.getBytes
      def decode(bytes: ByteBuffer): Either[String, T] = decoder.decodeJson(charset.decode(bytes))
    }
  }

  inline def derived[T](using m: Mirror.Of[T]): LMDBCodecJson[T] = {
    val encoder = DeriveJsonEncoder.gen[T]
    val decoder = DeriveJsonDecoder.gen[T]
    createCodec(encoder, decoder)
  }

  inline given [T](using m: Mirror.Of[T]): LMDBCodecJson[T] = derived
}
