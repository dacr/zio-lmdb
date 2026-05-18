package zio.lmdb

import zio.lmdb.json.*

case class SimpleAccount(balance: Long) derives LMDBCodecJson
