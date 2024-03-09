package zio.lmdb

import zio.json._

import java.nio.charset.StandardCharsets

class LMDBCodecJson[D](implicit encoder: JsonEncoder[D], decoder: JsonDecoder[D]) extends LMDBCodec[D] {
  private val charset = StandardCharsets.UTF_8

  def encode(that: D): Bytes = {
    that.toJson.getBytes(charset)
  }

  def decode(that: Bytes): Either[String, D] = {
    new String(that, charset).fromJson[D]
  }
}
