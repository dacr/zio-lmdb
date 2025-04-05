package zio.lmdb

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object LMDBCodecsJson {
  import zio.json.ast.Json
  import zio.json.ast.Json.*

  val charset = StandardCharsets.UTF_8

  implicit val jsonCodec: LMDBCodec[Json] = new LMDBCodec {
    def encode(t: Json): Array[Byte]                    = Json.encoder.encodeJson(t).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Json] = Json.decoder.decodeJson(charset.decode(bytes))
  }

  implicit val stringCodec: LMDBCodec[String] = new LMDBCodec {
    def encode(t: String): Array[Byte]                    = Str.encoder.encodeJson(Str(t)).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, String] = Str.decoder.decodeJson(charset.decode(bytes)).map(_.value)
  }

  implicit val strCodec: LMDBCodec[Str] = new LMDBCodec {
    def encode(t: Str): Array[Byte]                    = Str.encoder.encodeJson(t).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Str] = Str.decoder.decodeJson(charset.decode(bytes))
  }

  implicit val intCodec: LMDBCodec[Int] = new LMDBCodec {
    def encode(t: Int): Array[Byte]                    = Num.encoder.encodeJson(Num(t)).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Int] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.intValue())
  }

  implicit val doubleCodec: LMDBCodec[Double] = new LMDBCodec {
    def encode(t: Double): Array[Byte]                    = Num.encoder.encodeJson(Num(t)).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Double] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.doubleValue())
  }

  implicit val floatCodec: LMDBCodec[Float] = new LMDBCodec {
    def encode(t: Float): Array[Byte]                    = Num.encoder.encodeJson(Num(t)).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Float] = Num.decoder.decodeJson(charset.decode(bytes)).map(_.value.floatValue())
  }

  implicit val numCodec: LMDBCodec[Num] = new LMDBCodec {
    def encode(t: Num): Array[Byte]                    = Num.encoder.encodeJson(t).toString.getBytes
    def decode(bytes: ByteBuffer): Either[String, Num] = Num.decoder.decodeJson(charset.decode(bytes))
  }

}
