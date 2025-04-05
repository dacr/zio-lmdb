package zio.lmdb

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.deriving.Mirror

object DeriveLMDBJsonCodec {
  inline def gen[T](using mirror: Mirror.Of[T]): LMDBCodec[T] = {
    val encoder = DeriveJsonEncoder.gen[T]
    val decoder = DeriveJsonDecoder.gen[T]
    val charset = StandardCharsets.UTF_8

    new LMDBCodec[T] {
      def encode(t: T): Array[Byte]                    = encoder.encodeJson(t).toString.getBytes
      def decode(bytes: ByteBuffer): Either[String, T] = decoder.decodeJson(charset.decode(bytes))
    }
  }
}
