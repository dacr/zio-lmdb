package zio.lmdb.console

import zio._
import zio.stream._
import org.jline.terminal.TerminalBuilder
import org.jline.terminal.Terminal
import org.jline.reader.LineReaderBuilder
import org.jline.reader.LineReader
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.utils.AttributedStringBuilder
import org.jline.utils.AttributedStyle
import zio.lmdb._
import zio.lmdb.keycodecs.KeyCodec
import zio.lmdb.json.LMDBCodecJson._
import zio.logging._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files, Path}
import scala.util.Try

case class ConsoleContext(
  terminal: Terminal, 
  reader: LineReader, 
  dbHome: Path, 
  activeLMDB: Ref[Option[LMDB]],
  currentScope: Ref[Option[Scope.Closeable]]
)

object Main extends ZIOAppDefault {

  given byteArrayKeyCodec: KeyCodec[Array[Byte]] = new KeyCodec[Array[Byte]] {
    override def encode(key: Array[Byte]): Array[Byte] = key
    override def decode(keyBytes: ByteBuffer): Either[zio.lmdb.keycodecs.KeyCodecError, Array[Byte]] = {
      val bytes = new Array[Byte](keyBytes.remaining())
      keyBytes.get(bytes)
      Right(bytes)
    }
  }

  given byteArrayLMDBCodec: LMDBCodec[Array[Byte]] = new LMDBCodec[Array[Byte]] {
    override def encode(value: Array[Byte]): Array[Byte] = value
    override def decode(valueBytes: ByteBuffer): Either[String, Array[Byte]] = {
      val bytes = new Array[Byte](valueBytes.remaining())
      valueBytes.get(bytes)
      Right(bytes)
    }
  }

  val loggerLayer = Runtime.removeDefaultLoggers >>> consoleLogger(
    ConsoleLoggerConfig(LogFormat.default, LogFilter.LogLevelByNameConfig(LogLevel.Info))
  )

  override def run = {
    for {
      args <- getArgs
      dbHomeArg = args.headOption.getOrElse(java.lang.System.getProperty("user.home") + "/.lmdb")
      dbHome = Paths.get(dbHomeArg).toAbsolutePath
      
      _ <- ZIO.attemptBlocking(if (!Files.exists(dbHome)) Files.createDirectories(dbHome))
      _ <- Console.printLine(s"LMDB Databases Home: $dbHome")
      
      activeLMDB <- Ref.make[Option[LMDB]](None)
      currentScope <- Ref.make[Option[Scope.Closeable]](None)
      
      _ <- repl(dbHome, activeLMDB, currentScope).provide(loggerLayer)
    } yield ()
  }.catchAll {
    case e: Throwable => 
      ZIO.attempt(e.printStackTrace()).ignore *> Console.printLine(s"Error: $e")
    case e => 
      Console.printLine(s"Error: $e")
  }

  def repl(dbHome: Path, activeLMDB: Ref[Option[LMDB]], currentScope: Ref[Option[Scope.Closeable]]): ZIO[Any, Throwable, Unit] = {
    for {
      ctx <- ZIO.attempt {
        val terminal = TerminalBuilder.builder().system(true).build()
        val historyPath = Paths.get(java.lang.System.getProperty("user.home"), ".zio-lmdb-history")
        val reader = LineReaderBuilder.builder()
          .terminal(terminal)
          .completer(new StringsCompleter("databases", "select", "list", "inspect", "delete", "stats", "exit", "help", "fetch"))
          .variable(LineReader.HISTORY_FILE, historyPath)
          .build()
        ConsoleContext(terminal, reader, dbHome, activeLMDB, currentScope)
      }
      _ <- ZIO.attempt {
        ctx.terminal.writer().println(
          new AttributedStringBuilder()
            .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN).bold())
            .append("Welcome to ZIO LMDB Console!")
            .toAnsi
        )
        ctx.terminal.writer().println(s"Databases home is set to: ${ctx.dbHome}")
        ctx.terminal.writer().println("Type 'help' for available commands.")
      }
      _ <- loop(ctx)
    } yield ()
  }

  def loop(ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val prompt = for {
      active <- ctx.activeLMDB.get
      name = active.map(a => Paths.get(a.databasePath).getFileName.toString).getOrElse("none")
    } yield s"lmdb($name)> "

    prompt.flatMap { p =>
      ZIO.attempt {
        try {
          ctx.reader.readLine(p)
        } catch {
          case _: org.jline.reader.UserInterruptException => null
          case _: org.jline.reader.EndOfFileException => null
        }
      }
    }.flatMap {
      case null => ZIO.unit
      case line if line.trim == "exit" || line.trim == "quit" => ZIO.unit
      case line if line.trim.isEmpty => loop(ctx)
      case line => 
        processCommand(line, ctx).catchAll { e =>
          ZIO.attempt(ctx.terminal.writer().println(s"Command error: $e"))
        } *> loop(ctx)
    }
  }
  
  def processCommand(line: String, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val parts = line.trim.split("\\s+").toList
    parts match {
      case "databases" :: Nil => listDatabases(ctx)
      case "select" :: dbName :: Nil => selectDatabase(dbName, ctx)
      case "list" :: Nil => withActive(ctx)(listCollections)
      case "stats" :: Nil => withActive(ctx)(showStats)
      case "inspect" :: collName :: Nil => withActive(ctx)((lmdb, c) => inspectCollection(collName, lmdb, c))
      case "delete" :: collName :: Nil => withActive(ctx)((lmdb, c) => deleteCollection(collName, lmdb, c))
      case "fetch" :: collName :: keyStr :: Nil => withActive(ctx)((lmdb, c) => fetchKey(collName, keyStr, lmdb, c))
      case "help" :: _ => printHelp(ctx)
      case _ => 
        ZIO.attempt(ctx.terminal.writer().println(s"Unknown command or wrong arguments: $line"))
    }
  }

  def withActive(ctx: ConsoleContext)(f: (LMDB, ConsoleContext) => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit] = {
    ctx.activeLMDB.get.flatMap {
      case Some(lmdb) => f(lmdb, ctx)
      case None => ZIO.attempt(ctx.terminal.writer().println("No database selected. Use 'select <name>' first."))
    }
  }

  def listDatabases(ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    ZIO.attemptBlocking {
      val stream = Files.list(ctx.dbHome)
      try {
        val dbs = stream.filter(p => Files.isDirectory(p) && Files.exists(p.resolve("data.mdb"))).map(_.getFileName.toString).toArray.toList
        ctx.terminal.writer().println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW)).append("Available Databases:").toAnsi)
        if (dbs.isEmpty) ctx.terminal.writer().println(" (none found)")
        else dbs.foreach(db => ctx.terminal.writer().println(s" - $db"))
      } finally {
        stream.close()
      }
    }
  }

  def selectDatabase(name: String, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val dbPath = ctx.dbHome.resolve(name)
    if (!Files.exists(dbPath) || !Files.isDirectory(dbPath) || !Files.exists(dbPath.resolve("data.mdb"))) {
      ZIO.attempt(ctx.terminal.writer().println(s"Database '$name' does not exist or is not an LMDB directory in ${ctx.dbHome}"))
    } else {
      ZIO.uninterruptible {
        for {
          // 1. Close previous scope if any
          oldScopeOpt <- ctx.currentScope.getAndSet(None)
          _ <- oldScopeOpt match {
            case Some(scope) => scope.close(Exit.unit).ignore
            case None => ZIO.unit
          }
          _ <- ctx.activeLMDB.set(None)
          
          // 2. Open new scope and setup LMDB
          newScope <- Scope.make
          lmdb <- LMDBLive.setup(LMDBConfig.default.copy(databasesHome = Some(ctx.dbHome.toString), databaseName = name)).provide(ZLayer.succeed(newScope))
          _ <- ctx.activeLMDB.set(Some(lmdb))
          _ <- ctx.currentScope.set(Some(newScope))
          _ <- ZIO.attempt(ctx.terminal.writer().println(s"Selected database: $name"))
        } yield ()
      }
    }
  }

  def listCollections(lmdb: LMDB, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val metaCollName = LMDBConfig.default.metaDataCollectionName
    for {
      // 1. Fetch metadata if possible
      metaEntries <- lmdb.streamWithKeys[String, MetaDataEntry](metaCollName)
                        .runCollect
                        .map(_.toMap)
                        .catchAll(_ => ZIO.succeed(Map.empty[String, MetaDataEntry]))
      
      // 2. Fetch all raw DBI names
      allDbis <- lmdb.collectionsAvailable().mapError(e => new RuntimeException(e.toString))
      
      // 3. Separate them
      regularColls = allDbis.filter(name => metaEntries.get(name).exists(_.collectionKind == CollectionKind.Regular))
      multiColls   = allDbis.filter(name => metaEntries.get(name).exists(_.collectionKind == CollectionKind.Multi))
      indexColls   = allDbis.filter(name => metaEntries.get(name).exists(_.collectionKind == CollectionKind.Index))
      internalColls = allDbis.filter(name => name == metaCollName)
      unknownColls = allDbis.filter(name => !metaEntries.contains(name) && name != metaCollName)

      w = ctx.terminal.writer()
      
      _ <- ZIO.attempt {
        w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.CYAN)).append(s"Database: ${Paths.get(lmdb.databasePath).getFileName}").toAnsi)
      }

      _ <- if (regularColls.nonEmpty || (unknownColls.nonEmpty && metaEntries.isEmpty)) {
        for {
          _ <- ZIO.attempt(w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW)).append("Collections:").toAnsi))
          _ <- ZIO.foreachDiscard((regularColls ++ (if (metaEntries.isEmpty) unknownColls else Nil)).sorted) { c =>
            for {
              size <- lmdb.collectionSize(c).catchAll(_ => ZIO.succeed(-1L))
              _ <- ZIO.attempt(w.println(f" - $c%-30s (size: $size%d)"))
            } yield ()
          }
        } yield ()
      } else ZIO.unit

      _ <- if (multiColls.nonEmpty) {
        for {
          _ <- ZIO.attempt(w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW)).append("MultiCollections:").toAnsi))
          _ <- ZIO.foreachDiscard(multiColls.sorted) { c =>
            for {
              size <- lmdb.multiSize(c).catchAll(_ => ZIO.succeed(-1L))
              _ <- ZIO.attempt(w.println(f" - $c%-30s (size: $size%d)"))
            } yield ()
          }
        } yield ()
      } else ZIO.unit

      _ <- if (indexColls.nonEmpty) {
        for {
          _ <- ZIO.attempt(w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW)).append("Indexes:").toAnsi))
          _ <- ZIO.foreachDiscard(indexColls.sorted) { i =>
            for {
              size <- lmdb.collectionSize(i).catchAll(_ => ZIO.succeed(-1L))
              _ <- ZIO.attempt(w.println(f" - $i%-30s (size: $size%d)"))
            } yield ()
          }
        } yield ()
      } else ZIO.unit

      _ <- if (internalColls.nonEmpty || (unknownColls.nonEmpty && metaEntries.nonEmpty)) {
        for {
          _ <- ZIO.attempt(w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.MAGENTA)).append("Internal/Unknown:").toAnsi))
          _ <- ZIO.foreachDiscard((internalColls ++ (if (metaEntries.nonEmpty) unknownColls else Nil)).sorted) { c =>
            for {
              size <- lmdb.collectionSize(c).catchAll(_ => ZIO.succeed(-1L))
              _ <- ZIO.attempt(w.println(f" - $c%-30s (size: $size%d)"))
            } yield ()
          }
        } yield ()
      } else ZIO.unit
    } yield ()
  }

  def showStats(lmdb: LMDB, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    for {
      stats <- lmdb.stats().mapError(e => new RuntimeException(e.toString))
      w = ctx.terminal.writer()
      _ <- ZIO.attempt {
        w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.CYAN)).append("Database Statistics").toAnsi)
        w.println(f"  Path:                    ${stats.databasePath}")
        w.println(f"  Map Size:                ${stats.mapSize} bytes")
        w.println(f"  Last Page Number:        ${stats.lastPageNumber}")
        w.println(f"  Last Transaction ID:     ${stats.lastTransactionId}")
        w.println(f"  Max Readers:             ${stats.maxReaders}")
        w.println(f"  Current Readers:         ${stats.numReaders}")
        w.println(f"  Number of Collections:   ${stats.numCollections}")
        w.println(f"  Number of MultiColls:    ${stats.numMultis}")
        w.println(f"  Number of Indexes:       ${stats.numIndexes}")

        w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.YELLOW)).append("Environment Statistics").toAnsi)
        w.println(f"  Page Size:               ${stats.envStats.pageSize}")
        w.println(f"  Tree Depth:              ${stats.envStats.depth}")
        w.println(f"  Branch Pages:            ${stats.envStats.branchPages}")
        w.println(f"  Leaf Pages:              ${stats.envStats.leafPages}")
        w.println(f"  Overflow Pages:          ${stats.envStats.overflowPages}")
        w.println(f"  Total Entries:           ${stats.envStats.entries}")
      }
    } yield ()
  }

  def inspectCollection(name: String, lmdb: LMDB, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val metaCollName = LMDBConfig.default.metaDataCollectionName
    for {
      exists <- lmdb.collectionExists(name).mapError(e => new RuntimeException(e.toString))
      _ <- if (!exists) ZIO.attempt(ctx.terminal.writer().println(s"Collection $name does not exist."))
           else {
             for {
               meta <- lmdb.fetch[String, MetaDataEntry](metaCollName, name).catchAll(_ => ZIO.succeed(None))
               isMulti = meta.exists(_.collectionKind == CollectionKind.Multi)
               kindStr = meta.map(_.collectionKind.toString).getOrElse("Unknown")
               _ <- ZIO.attempt {
                 val header = new AttributedStringBuilder()
                   .style(AttributedStyle.DEFAULT.bold())
                   .append(s"Inspecting $name")
                   .style(AttributedStyle.DEFAULT)
                   .append(s" (Kind: $kindStr)")
                   .toAnsi
                 ctx.terminal.writer().println(header)
               }
               _ <- (if (isMulti) lmdb.multiGet[Array[Byte], Array[Byte]](name).unit.catchAll(_ => ZIO.unit) else ZIO.unit) *>
                    lmdb.streamWithKeys[Array[Byte], Array[Byte]](name).take(20).runForeach { case (k, v) =>
                 ZIO.attempt {
                   val ks = TypeGuesser.formatKey(k)
                   val vs = TypeGuesser.formatValue(v)
                   val line = new AttributedStringBuilder()
                     .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
                     .append(f"${TypeGuesser.colorize(ks)}%-20s")
                     .style(AttributedStyle.DEFAULT)
                     .append(" => ")
                     .append(TypeGuesser.colorize(vs))
                     .toAnsi
                   ctx.terminal.writer().println(line)
                 }
               }.mapError(e => new RuntimeException(e.toString))
             } yield ()
           }
    } yield ()
  }

  def deleteCollection(name: String, lmdb: LMDB, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val metaCollName = LMDBConfig.default.metaDataCollectionName
    for {
      meta <- lmdb.fetch[String, MetaDataEntry](metaCollName, name).catchAll(_ => ZIO.succeed(None))
      isMulti = meta.exists(_.collectionKind == CollectionKind.Multi)
      _ <- if (isMulti) lmdb.multiDrop(name).mapError(e => new RuntimeException(e.toString))
           else lmdb.collectionDrop(name).mapError(e => new RuntimeException(e.toString))
      _ <- ZIO.attempt(ctx.terminal.writer().println(s"Collection $name dropped."))
    } yield ()
  }.catchAll(e => ZIO.attempt(ctx.terminal.writer().println(s"Failed to drop collection $name: $e")))

  def fetchKey(collName: String, keyStr: String, lmdb: LMDB, ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    val metaCollName = LMDBConfig.default.metaDataCollectionName
    val keyOptions = List(
      keyStr.getBytes(StandardCharsets.UTF_8),
      Try(ByteBuffer.allocate(8).putLong(keyStr.toLong).array()).getOrElse(Array.emptyByteArray),
      Try(ByteBuffer.allocate(4).putInt(keyStr.toInt).array()).getOrElse(Array.emptyByteArray)
    ).filter(_.nonEmpty)

    (for {
      meta <- lmdb.fetch[String, MetaDataEntry](metaCollName, collName).catchAll(_ => ZIO.succeed(None))
      isMulti = meta.exists(_.collectionKind == CollectionKind.Multi)
      
      results <- if (isMulti) {
        ZIO.foreach(keyOptions) { k =>
          lmdb.multiFetch[Array[Byte], Array[Byte]](collName, k).catchAll(_ => ZIO.succeed(Nil))
        }.map(_.flatten)
      } else {
        ZIO.foreach(keyOptions) { k =>
          lmdb.fetch[Array[Byte], Array[Byte]](collName, k).catchAll(_ => ZIO.succeed(None))
        }.map(_.flatten)
      }

      _ <- if (results.nonEmpty) {
        ZIO.foreachDiscard(results) { v =>
          val vs = TypeGuesser.formatValue(v)
          ZIO.attempt(ctx.terminal.writer().println(TypeGuesser.colorize(vs)))
        }
      } else {
        ZIO.attempt(ctx.terminal.writer().println(s"Key '$keyStr' not found in '$collName'."))
      }
    } yield ()).catchAll(e => ZIO.attempt(ctx.terminal.writer().println(s"Error fetching key: $e")))
  }

  def printHelp(ctx: ConsoleContext): ZIO[Any, Throwable, Unit] = {
    ZIO.attempt {
      val w = ctx.terminal.writer()
      w.println(new AttributedStringBuilder().style(AttributedStyle.DEFAULT.bold()).append("Available commands:").toAnsi)
      w.println(f"  ${"databases"}%-25s List all databases in current home")
      w.println(f"  ${"select <db>"}%-25s Select a database to use")
      w.println(f"  ${"list"}%-25s List all collections and indexes in active DB")
      w.println(f"  ${"inspect <coll>"}%-25s Inspect first 20 records of a collection")
      w.println(f"  ${"fetch <coll> <key>"}%-25s Fetch a value by its key (tries String, Long, Int)")
      w.println(f"  ${"delete <coll>"}%-25s Delete a collection")
      w.println(f"  ${"stats"}%-25s Show database statistics")
      w.println(f"  ${"exit"}%-25s Exit the console")
      w.println(f"  ${"help"}%-25s Show this help")
    }
  }
}
