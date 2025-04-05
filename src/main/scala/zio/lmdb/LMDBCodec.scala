package zio.lmdb

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait LMDBCodec[T] {
  def encode(t: T): Array[Byte]
  def decode(bytes: ByteBuffer): Either[String, T]
}
