package zio.lmdb

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonEncoder}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.deriving.Mirror

trait LMDBCodecJson[T] extends LMDBCodec[T] {
}

object LMDBCodecJson {

  inline def derived[T](using m:Mirror.Of[T]): LMDBCodecJson[T] = {
    val encoder = DeriveJsonEncoder.gen[T]
    val decoder = DeriveJsonDecoder.gen[T]
    val charset = StandardCharsets.UTF_8

    new LMDBCodecJson[T] {
      def encode(t: T): Array[Byte]                    = encoder.encodeJson(t).toString.getBytes
      def decode(bytes: ByteBuffer): Either[String, T] = decoder.decodeJson(charset.decode(bytes))
    }
  }

  inline given [T](using m: Mirror.Of[T]): LMDBCodecJson[T] = derived
}
