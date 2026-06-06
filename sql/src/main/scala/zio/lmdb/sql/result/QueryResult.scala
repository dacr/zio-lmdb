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

import zio.IO
import zio.stream.ZStream
import zio.lmdb.json.JValue
import zio.lmdb.sql.SqlError

/** A column header: its name and a type hint (from the value `JsonSchema` or the key `keyId`). */
final case class Column(name: String, typeHint: String)

/** The standardized, streaming result of any statement. `columns` is known eagerly (after binding);
  * `rows` is a lazy stream of `MapV(column -> value)` documents that flows straight from the LMDB
  * cursor, so large results are never fully materialised (`ORDER BY` is the documented exception).
  * The canonical form is JSON — `rows` are `JValue`s — which is what makes assertions trivial.
  */
final case class QueryResult(columns: List[Column], rows: ZStream[Any, SqlError, JValue]) {

  /** Materialise the rows — convenience for small results and tests. */
  def toList: IO[SqlError, List[JValue]] = rows.runCollect.map(_.toList)
}

object QueryResult {

  /** An eager, already-materialised result (used for `DESCRIBE`, `SHOW`, and affected-row summaries). */
  def of(columns: List[Column], rows: List[JValue]): QueryResult =
    QueryResult(columns, ZStream.fromIterable(rows))

  /** A single-row "affected count" result, the uniform shape returned by writes. */
  def affected(n: Long): QueryResult =
    of(List(Column("affected", "integer")), List(JValue.MapV(scala.collection.immutable.ListMap("affected" -> JValue.LongV(n)))))
}
