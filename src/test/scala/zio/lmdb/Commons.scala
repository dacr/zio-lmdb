package zio.lmdb

import zio._
import zio.logging._
import zio.nio.file.Files

trait Commons {
  val config: ConsoleLoggerConfig = ConsoleLoggerConfig(LogFormat.default, LogFilter.logLevel(LogLevel.None))

  val logger = Runtime.removeDefaultLoggers >>> consoleLogger(config)

  val lmdbLayer = ZLayer.scoped(
    for {
      path  <- Files.createTempDirectoryScoped(prefix = Some("lmdb"), fileAttributes = Nil)
      config = LMDBConfig.default.copy(databasesHome = Some(path.toString))
      lmdb  <- LMDBLive.setup(config)
    } yield lmdb
  )

  val randomUUID = Random.nextUUID.map(_.toString)

  val randomCollectionName = for {
    uuid <- randomUUID
    name  = s"collection-$uuid"
  } yield name

}
