package zio.lmdb

import zio.lmdb.json.LMDBCodecJson

case class Person(name: String, age: Int) derives LMDBCodecJson