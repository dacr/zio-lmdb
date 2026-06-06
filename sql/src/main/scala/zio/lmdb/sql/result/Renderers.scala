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
package zio.lmdb.sql.result

import zio.stream.ZStream
import zio.lmdb.json.JValue
import zio.lmdb.json.JValue.*
import zio.lmdb.sql.SqlError

import java.nio.charset.StandardCharsets.UTF_8

enum Format { case Table, Json, Csv }

/** Renders a [[QueryResult]] to a stream of output lines. `Json` (one object per row) and `Csv`
  * stream per row; `Table` buffers to compute column widths, then streams — the documented
  * interactive exception. All three consume the same canonical `QueryResult`.
  */
object Renderer {

  def render(format: Format, result: QueryResult): ZStream[Any, SqlError, String] =
    format match {
      case Format.Json  => json(result)
      case Format.Csv   => csv(result)
      case Format.Table => table(result)
    }

  // ── JSON lines (streaming) ───────────────────────────────────────────────────────────────────
  private def json(result: QueryResult): ZStream[Any, SqlError, String] =
    result.rows.map(row => new String(JValue.toPlainJson(row), UTF_8))

  // ── CSV (streaming, header first) ────────────────────────────────────────────────────────────
  private def csv(result: QueryResult): ZStream[Any, SqlError, String] = {
    val headers = result.columns.map(_.name)
    val header  = headers.map(csvEscape).mkString(",")
    ZStream.succeed(header) ++ result.rows.map(row => headers.map(h => csvEscape(cell(field(row, h)))).mkString(","))
  }

  private def csvEscape(s: String): String =
    if (s.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r')) "\"" + s.replace("\"", "\"\"") + "\""
    else s

  // ── Table (buffered, then streamed) ──────────────────────────────────────────────────────────
  private def table(result: QueryResult): ZStream[Any, SqlError, String] =
    ZStream.unwrap(result.rows.runCollect.map { chunk =>
      val rows    = chunk.toList
      val headers = result.columns.map(_.name)
      val matrix  = rows.map(row => headers.map(h => cell(field(row, h))))
      val widths  = headers.indices.map(i => (headers(i).length :: matrix.map(_(i).length)).max).toList

      def line(cells: List[String]): String =
        cells.zip(widths).zipWithIndex
          .map { case ((c, w), i) => if (i == cells.size - 1) c else c.padTo(w, ' ') }
          .mkString(" | ")

      val sep    = widths.map("-" * _).mkString("-+-")
      val body   = matrix.map(line)
      val footer = s"(${rows.size} row${if (rows.size == 1) "" else "s"})"
      ZStream.fromIterable(line(headers) :: sep :: body ::: List(footer))
    })

  // ── cell formatting ──────────────────────────────────────────────────────────────────────────
  private def field(row: JValue, name: String): JValue =
    row match { case MapV(m) => m.getOrElse(name, NullV); case _ => NullV }

  private def cell(jv: JValue): String =
    jv match {
      case StringV(s)     => s
      case LongV(v)       => v.toString
      case DoubleV(v)     => v.toString
      case DecimalV(v)    => v.toString
      case BoolV(b)       => b.toString
      case InstantV(t)    => t.toString
      case IdentifierV(u) => u.toString
      case NullV          => ""
      case other          => new String(JValue.toPlainJson(other), UTF_8)
    }
}
