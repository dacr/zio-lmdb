package zio.lmdb

import zio.lmdb.json.LMDBCodecJson

case class Login(username: String, user: Shopper) derives LMDBCodecJson
