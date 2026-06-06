/*
 * Copyright 2026 David Crosson
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
package zio.lmdb.sql.repl

import org.jline.reader.{EndOfFileException, LineReader, LineReaderBuilder, UserInterruptException}
import org.jline.reader.impl.DefaultParser
import org.jline.terminal.{Terminal, TerminalBuilder}
import zio.*
import zio.lmdb.*
import zio.lmdb.sql.engine.{Catalog, SqlEngine}
import zio.lmdb.sql.result.{Format, Renderer}

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicReference

/** The interactive SQL shell. The terminal is the only I/O; statements go through the pure
  * `SqlEngine` pipeline and are rendered with the selected [[Format]]. Anything that does not start
  * with `\` is SQL; `\`-commands handle connection, catalog shortcuts, output format, and exit.
  */
object Main extends ZIOAppDefault {

  private final case class Ctx(
    terminal: Terminal,
    reader: LineReader,
    dbHome: Path,
    active: Ref[Option[LMDB]],
    scope: Ref[Option[Scope.Closeable]],
    format: Ref[Format],
    catalog: AtomicReference[CatalogSnapshot]
  ) {
    def out(s: String): UIO[Unit]  = ZIO.attempt { terminal.writer().println(s); terminal.writer().flush() }.orDie
    def err(s: String): UIO[Unit]  = out(s"! $s")
  }

  override def run =
    (for {
      args      <- getArgs
      dbHomeArg  = args.headOption.getOrElse(java.lang.System.getProperty("user.home") + "/.lmdb")
      dbHome     = Paths.get(dbHomeArg).toAbsolutePath
      _         <- ZIO.attemptBlocking(if (!Files.exists(dbHome)) Files.createDirectories(dbHome))
      active    <- Ref.make[Option[LMDB]](None)
      scope     <- Ref.make[Option[Scope.Closeable]](None)
      format    <- Ref.make[Format](Format.Table)
      catalog    = new AtomicReference(CatalogSnapshot.empty)
      ctx       <- ZIO.attempt {
                     // JLine 4.x probes the terminal for DEC mode 2027 (grapheme-cluster) support when a
                     // terminal is built, emitting ESC[?2027$p ESC[c ESC[6n. The cursor-position (CPR)
                     // reply leaks onto stdin: it prints `^[[27;1R` before our banner and gets prepended
                     // to the first line read, so e.g. `\c name` no longer starts with `\` and is treated
                     // as SQL ("no database selected"). We don't need grapheme-width precision here, so
                     // turn the probe off. Must be set before TerminalBuilder.build().
                     java.lang.System.setProperty("org.jline.terminal.graphemeCluster", "false")
                     val terminal = TerminalBuilder.builder().system(true).build()
                     val history  = Paths.get(java.lang.System.getProperty("user.home"), ".zio-lmdb-sql-history")
                     // Our commands are psql-style and start with '\' (\c, \l, \h, \d, \format, \q). JLine's
                     // history "event expansion" treats a leading '\' as an escape and strips it, so readLine
                     // would return "l" for "\l"; the line then fails the startsWith("\\") dispatch and is run
                     // as SQL ("no database selected"). Disable event expansion to keep '\' literal (and '!',
                     // as in SQL "!="), and drop '\' as a parser escape char so word-splitting/continuation
                     // leave our commands intact.
                     val parser   = new DefaultParser()
                     parser.setEscapeChars(null)
                     val reader   = LineReaderBuilder
                                      .builder()
                                      .terminal(terminal)
                                      .parser(parser)
                                      .completer(new SqlCompleter(catalog))
                                      .variable(LineReader.HISTORY_FILE, history)
                                      .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                                      .build()
                     Ctx(terminal, reader, dbHome, active, scope, format, catalog)
                   }
      _         <- ctx.out(s"zio-lmdb-sql  —  databases home: $dbHome")
      _         <- ctx.out("Type SQL, or \\h for help. \\q to quit.")
      _         <- refreshDatabases(ctx) // populate \c completion before any connection
      _         <- loop(ctx)
    } yield ()).catchAll(e => Console.printLineError(s"Fatal: $e").orDie)

  private def loop(ctx: Ctx): Task[Unit] =
    prompt(ctx).flatMap { p =>
      ZIO.attempt {
        try ctx.reader.readLine(p)
        catch { case _: UserInterruptException => ""; case _: EndOfFileException => null }
      }
    }.flatMap {
      case null                                              => ZIO.unit
      case line if line.trim.isEmpty                         => loop(ctx)
      case line if line.trim == "\\q" || line.trim == "exit" => ZIO.unit
      case line if line.trim.startsWith("\\")                => meta(ctx, line.trim).catchAllCause(c => ctx.err(c.squash.getMessage)) *> loop(ctx)
      case line                                              => runSql(ctx, line).catchAllCause(c => ctx.err(c.squash.getMessage)) *> loop(ctx)
    }

  private def prompt(ctx: Ctx): UIO[String] =
    ctx.active.get.map {
      case Some(lmdb) => s"lmdb(${Paths.get(lmdb.databasePath).getFileName})> "
      case None       => "lmdb(-)> "
    }

  private def runSql(ctx: Ctx, sql: String): UIO[Unit] =
    ctx.active.get.flatMap {
      case None       => ctx.err("no database selected — use \\c <name>")
      case Some(lmdb) =>
        (for {
          fmt     <- ctx.format.get
          counter <- Ref.make(0L)
          timed   <- (for {
                        result <- SqlEngine.run(sql).provide(ZLayer.succeed(lmdb))
                        tapped  = result.copy(rows = result.rows.tap(_ => counter.update(_ + 1)))
                        _      <- Renderer.render(fmt, tapped).runForeach(ctx.out)
                      } yield ()).timed
          n       <- counter.get
          _       <- ctx.out(s"($n row${if (n == 1) "" else "s"} in ${formatDuration(timed._1.toMillis)})")
        } yield ()).catchAll(e => ctx.err(e.message))
    }

  /** Compact elapsed-time rendering: `42ms`, `10s200ms`, `1m42s5ms`. */
  private def formatDuration(totalMs: Long): String = {
    val ms        = totalMs % 1000
    val totalSecs = totalMs / 1000
    val secs      = totalSecs % 60
    val mins      = totalSecs / 60
    if (mins > 0) s"${mins}m${secs}s${ms}ms"
    else if (secs > 0) s"${secs}s${ms}ms"
    else s"${ms}ms"
  }

  private def meta(ctx: Ctx, line: String): Task[Unit] = {
    val parts = line.split("\\s+").toList
    parts match {
      case "\\h" :: _              => help(ctx)
      case "\\l" :: _              => listDatabases(ctx)
      case "\\c" :: name :: _      => connect(ctx, name)
      case "\\dt" :: _             => runSql(ctx, "show collections")
      case "\\di" :: _             => runSql(ctx, "show indexes")
      case "\\d" :: name :: _      => runSql(ctx, s"describe $name")
      case "\\format" :: f :: _    => setFormat(ctx, f)
      case other                   => ctx.err(s"unknown command: ${other.mkString(" ")}  (try \\h)")
    }
  }

  private def setFormat(ctx: Ctx, f: String): UIO[Unit] =
    f.toLowerCase match {
      case "table" => ctx.format.set(Format.Table) *> ctx.out("format: table")
      case "json"  => ctx.format.set(Format.Json) *> ctx.out("format: json")
      case "csv"   => ctx.format.set(Format.Csv) *> ctx.out("format: csv")
      case other   => ctx.err(s"unknown format '$other' (table|json|csv)")
    }

  /** The LMDB database directories (those containing a `data.mdb`) under the databases home. */
  private def listDbDirs(dbHome: Path): Task[List[String]] =
    ZIO.attemptBlocking {
      val stream = Files.list(dbHome)
      try stream.filter(p => Files.isDirectory(p) && Files.exists(p.resolve("data.mdb"))).map(_.getFileName.toString).sorted().toArray.toList.map(_.toString)
      finally stream.close()
    }

  private def listDatabases(ctx: Ctx): Task[Unit] =
    listDbDirs(ctx.dbHome).flatMap {
      case Nil => ctx.out("(no databases)")
      case dbs => ctx.out(dbs.map(d => s" - $d").mkString("\n"))
    }

  private def connect(ctx: Ctx, name: String): Task[Unit] = {
    val dbPath = ctx.dbHome.resolve(name)
    ZIO.attemptBlocking(Files.exists(dbPath.resolve("data.mdb")) || !Files.exists(dbPath)).flatMap { ok =>
      if (!ok) ctx.err(s"'$name' is not an LMDB database directory in ${ctx.dbHome}")
      else
        ZIO.uninterruptible {
          for {
            old      <- ctx.scope.getAndSet(None)
            _        <- ZIO.foreachDiscard(old)(_.close(Exit.unit).ignore)
            _        <- ctx.active.set(None)
            newScope <- Scope.make
            lmdb     <- LMDBLive
                          .setup(LMDBConfig.default.copy(databasesHome = Some(ctx.dbHome.toString), databaseName = name))
                          .provide(ZLayer.succeed(newScope))
            _        <- ctx.active.set(Some(lmdb))
            _        <- ctx.scope.set(Some(newScope))
            _        <- refreshDatabases(ctx)
            _        <- refreshCatalog(ctx, lmdb)
            _        <- ctx.out(s"connected to '$name'")
          } yield ()
        }
    }
  }

  /** Refresh the database list used for `\c` completion (independent of any connection). */
  private def refreshDatabases(ctx: Ctx): UIO[Unit] =
    listDbDirs(ctx.dbHome).orElseSucceed(Nil).flatMap(dbs => ZIO.succeed(ctx.catalog.updateAndGet(_.copy(databases = dbs)))).unit

  /** Refresh the collection/column lists (for `\d`, FROM, … completion) from the connected database. */
  private def refreshCatalog(ctx: Ctx, lmdb: LMDB): UIO[Unit] =
    Catalog
      .list(lmdb)
      .map { entries =>
        val names = entries.map(_.collectionName).sorted
        val cols  = entries.map(e => e.collectionName -> ("_key" :: Catalog.toInfo(e).columns.map(_.name))).toMap
        (names, cols)
      }
      .orElseSucceed((Nil, Map.empty[String, List[String]]))
      .flatMap { case (names, cols) => ZIO.succeed(ctx.catalog.updateAndGet(_.copy(collections = names, columns = cols))) }
      .unit

  private def help(ctx: Ctx): UIO[Unit] =
    ctx.out(
      """Commands:
        |  <sql>;                 run a SQL statement (SELECT / INSERT / UPDATE / DELETE / DESCRIBE / SHOW)
        |  \c <name>              connect to a database
        |  \l                     list databases
        |  \dt                    list collections        (= SHOW COLLECTIONS)
        |  \di                    list indexes            (= SHOW INDEXES)
        |  \d <collection>        describe a collection   (= DESCRIBE <collection>)
        |  \format table|json|csv set the output format
        |  \h                     this help
        |  \q                     quit
        |
        |TAB completes commands, keywords, collection and column names.
        |The key is the pseudo-column _key; value fields are columns (see \d). Examples:
        |  SELECT _key, customer FROM orders WHERE customer = 'Alice' ORDER BY _key LIMIT 10;
        |  SELECT DISTINCT customer FROM orders ORDER BY customer;
        |  SELECT customer, COUNT(*) AS n, SUM(amount), AVG(amount), MIN(amount), MAX(amount)
        |    FROM orders WHERE LENGTH(customer) > 0
        |    GROUP BY customer HAVING COUNT(*) > 1 ORDER BY n;""".stripMargin
    )
}
