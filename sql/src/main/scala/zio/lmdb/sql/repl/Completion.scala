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

import org.jline.reader.{Candidate, Completer, LineReader, ParsedLine}

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

/** The catalog data the completer needs. `databases` is the `\c` target list (scanned from the
  * databases home, available before connecting); `collections`/`columns` come from the connected
  * database and are refreshed on each `\c`.
  */
final case class CatalogSnapshot(databases: List[String], collections: List[String], columns: Map[String, List[String]])
object CatalogSnapshot { val empty: CatalogSnapshot = CatalogSnapshot(Nil, Nil, Map.empty) }

/** Context-aware TAB completion for the SQL shell. Candidates are chosen from where the cursor sits:
  *   - meta-commands when the line starts with `\` (and the database list right after `\c`/`\d`);
  *   - the database list right after FROM / INTO / UPDATE / DESCRIBE;
  *   - otherwise SQL keywords, the database list, and the columns of the FROM/INTO/UPDATE collection.
  *
  * JLine filters whatever we offer by the word prefix already typed, so it is safe to over-offer.
  * Live names come from a [[CatalogSnapshot]] updated by the REPL on connect, read here (off the ZIO
  * fiber) through an `AtomicReference`.
  */
final class SqlCompleter(snapshot: AtomicReference[CatalogSnapshot]) extends Completer {
  import SqlCompleter.*

  override def complete(reader: LineReader, line: ParsedLine, candidates: java.util.List[Candidate]): Unit = {
    val snap  = snapshot.get()
    val words = line.words().asScala.toList
    val wi    = line.wordIndex()
    val first = words.headOption.getOrElse("")
    val prev  = if (wi > 0 && wi - 1 < words.size) words(wi - 1).toLowerCase else ""

    def offer(values: Iterable[String]): Unit = values.foreach(v => candidates.add(new Candidate(v)))

    if (first.startsWith("\\")) {
      if (wi == 0) offer(metaCommands)
      else
        first match {
          case "\\c"      => offer(snap.databases)   // \c connects to a database under the databases home
          case "\\d"      => offer(snap.collections) // \d describes a collection of the connected database
          case "\\format" => offer(formats)
          case _          => ()
        }
    } else if (collectionSlot(prev)) {
      offer(snap.collections)
    } else {
      offer(keywords)
      offer(snap.collections)
      offer(columnsOfFrom(words, snap))
    }
  }
}

object SqlCompleter {
  val metaCommands: List[String] = List("\\c", "\\l", "\\dt", "\\di", "\\d", "\\format", "\\h", "\\q")
  val formats: List[String]      = List("table", "json", "csv")

  /** SQL keywords and the aggregate-call openers (so `cou`<TAB> → `count(`). */
  val keywords: List[String] = List(
    "select", "distinct", "as", "from", "where", "group", "by", "order", "limit", "asc", "desc",
    "insert", "into", "values", "update", "set", "delete", "describe", "show", "collections", "indexes",
    "and", "or", "not", "like", "is", "null", "count(", "sum(", "avg(", "min(", "max("
  )

  /** Positions where a collection name is expected next. */
  private def collectionSlot(prev: String): Boolean =
    prev == "from" || prev == "into" || prev == "update" || prev == "describe" || prev == "desc"

  /** Columns of the collection named after the statement's FROM / INTO / UPDATE token, if known. */
  private def columnsOfFrom(words: List[String], snap: CatalogSnapshot): List[String] = {
    val lower = words.map(_.toLowerCase)
    val idx   = lower.indexWhere(w => w == "from" || w == "into" || w == "update")
    if (idx >= 0 && idx + 1 < words.size) snap.columns.getOrElse(words(idx + 1), Nil) else Nil
  }
}
