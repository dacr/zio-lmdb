package zio.lmdb

import zio.lmdb.json.LMDBCodecJson

case class Shopper(firstName: String, lastName: String, age: Option[Int]) derives LMDBCodecJson
