package zio.lmdb

case class LMDBEnvStats(
  pageSize: Int,
  depth: Int,
  branchPages: Long,
  leafPages: Long,
  overflowPages: Long,
  entries: Long
)

case class LMDBStats(
  databasePath: String,
  mapSize: Long,
  lastPageNumber: Long,
  lastTransactionId: Long,
  maxReaders: Int,
  numReaders: Int,
  numCollections: Int,
  numIndexes: Int,
  numMultis: Int,
  envStats: LMDBEnvStats
)
