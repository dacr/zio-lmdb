package zio.lmdb

import java.nio.ByteBuffer

trait LMDBCodec[T] {
  def encode(t: T): Array[Byte]
  def decode(bytes: ByteBuffer): Either[String, T]
}
