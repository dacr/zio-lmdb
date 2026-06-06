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
package zio.lmdb.sql.parser

/** The parsed shape of a SQL statement. Pure data — no ZIO, no LMDB — so the parser is testable in
  * isolation. The key is addressed as the pseudo-column `_key`; value fields are columns named after
  * the value `JsonSchema` properties.
  */
sealed trait Statement
object Statement {
  final case class Select(projection: Projection, distinct: Boolean, from: String, where: Option[Expr], groupBy: List[String], having: Option[Expr], orderBy: Option[OrderBy], limit: Option[Long]) extends Statement
  final case class Insert(into: String, columns: List[String], values: List[Literal])                                              extends Statement
  final case class Update(table: String, assignments: List[(String, Literal)], where: Option[Expr])                                extends Statement
  final case class Delete(from: String, where: Option[Expr])                                                                       extends Statement
  final case class Describe(collection: String)                                                                                    extends Statement
  final case class Show(target: ShowTarget)                                                                                        extends Statement
}

enum ShowTarget    { case Collections, Indexes }

/** SQL aggregate functions. `COUNT` may take `*` (no column); the rest require a column. */
enum AggFunc { case Count, Sum, Avg, Min, Max }

/** One element of a (non-`*`) projection: either a plain column or an aggregate over a column
  * (`column = None` is the `COUNT(*)` special case). `alias` is the optional `AS <name>` and, when
  * present, both names the output column and may be referenced by ORDER BY.
  */
sealed trait SelectItem { def alias: Option[String] }
object SelectItem {
  final case class Col(name: String, alias: Option[String] = None)                          extends SelectItem
  final case class Agg(func: AggFunc, column: Option[String], alias: Option[String] = None) extends SelectItem
}

sealed trait Projection
object Projection {
  case object Star                                extends Projection
  /** A non-`*` projection: plain columns and/or aggregates, each optionally aliased. */
  final case class Items(items: List[SelectItem]) extends Projection
}

final case class OrderBy(column: String, descending: Boolean)

enum CmpOp { case Eq, Ne, Lt, Le, Gt, Ge }

sealed trait Expr
object Expr {
  final case class Col(name: String)                       extends Expr
  final case class Lit(value: Literal)                     extends Expr
  final case class Cmp(op: CmpOp, left: Expr, right: Expr) extends Expr
  final case class And(left: Expr, right: Expr)            extends Expr
  final case class Or(left: Expr, right: Expr)             extends Expr
  final case class Not(inner: Expr)                        extends Expr
  final case class Like(target: Expr, pattern: String)     extends Expr
  final case class IsNull(target: Expr, negated: Boolean)  extends Expr
  /** A scalar function call, e.g. `LENGTH(name)`. `name` is lower-cased. */
  final case class Func(name: String, args: List[Expr])    extends Expr
  /** An aggregate reference inside an expression (only meaningful in HAVING), e.g. `COUNT(*)`. */
  final case class Aggregate(func: AggFunc, column: Option[String]) extends Expr
}

sealed trait Literal
object Literal {
  final case class StrLit(value: String)      extends Literal
  final case class IntLit(value: Long)        extends Literal
  final case class DecLit(value: BigDecimal)  extends Literal
  final case class BoolLit(value: Boolean)    extends Literal
  case object NullLit                          extends Literal
}
