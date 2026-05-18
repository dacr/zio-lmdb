package zio.lmdb

import zio.lmdb.json.*

case class SimpleUser(name: String) derives LMDBCodecJson
