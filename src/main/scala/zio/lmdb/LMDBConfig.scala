package zio.lmdb

import java.io.File

case class LMDBConfig(
  databasesPath: File,
  mapSize: Long = 100_000_000_000L,
  maxDbs: Int = 10_000,
  maxReaders: Int = 100
)
