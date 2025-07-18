/*
 * Copyright 2025 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio.lmdb

import zio._
import zio.logging._
import zio.nio.file.Files

trait Commons {
  val config: ConsoleLoggerConfig = ConsoleLoggerConfig(
    LogFormat.default,
    LogFilter.LogLevelByNameConfig(LogLevel.None)
  )

  val logger = Runtime.removeDefaultLoggers >>> consoleLogger(config)

  val lmdbLayer = ZLayer.scoped(
    for {
      path  <- Files.createTempDirectoryScoped(prefix = Some("lmdb"), fileAttributes = Nil)
      config = LMDBConfig.default.copy(databasesHome = Some(path.toString))
      lmdb  <- LMDBLive.setup(config)
    } yield lmdb
  )

  val randomUUID: UIO[RecordKey] = Random.nextUUID.map(_.toString)

  val randomCollectionName: UIO[String] = for {
    uuid <- randomUUID
    name  = s"collection-$uuid"
  } yield name

}
