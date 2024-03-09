package zio.lmdb

trait LMDBCodec[D] {
  type Bytes = Array[Byte]
  def encode(that: D): Bytes
  def decode(those: Bytes): Either[String, D]
}
