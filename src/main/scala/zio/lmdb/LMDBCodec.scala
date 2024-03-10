package zio.lmdb

trait LMDBCodec[T] {
  type Bytes = Array[Byte]
  def encode(that: T): Bytes
  def decode(those: Bytes): Either[String, T]
}
