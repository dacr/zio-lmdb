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

/** The parsed shape of a SQL statement. Pure data — no ZIO, no LMDB — so the parser is testable in isolation. The key is addressed as the pseudo-column `_key`; value fields are columns named after the value `JsonSchema` properties.
  */
sealed trait Statement
object Statement {
  final case class Select(
    projection: Projection,
    distinct: Boolean,
    from: String,
    where: Option[Expr],
    groupBy: List[Expr],
    having: Option[Expr],
    orderBy: List[OrderBy],
    limit: Option[Long],
    fromAlias: Option[String] = None,
    joins: List[Join] = Nil
  ) extends Statement
  final case class Insert(into: String, columns: List[String], values: List[Literal])               extends Statement
  final case class Update(table: String, assignments: List[(String, Literal)], where: Option[Expr]) extends Statement
  final case class Delete(from: String, where: Option[Expr])                                        extends Statement
  final case class Describe(collection: String)                                                     extends Statement
  final case class Show(target: ShowTarget)                                                         extends Statement

  /** `EXPLAIN <select>` — report the access path the planner would use, without executing. */
  final case class Explain(select: Select) extends Statement
}

enum ShowTarget { case Collections, Indexes }

enum JoinType { case Inner, Left }

/** A table in a FROM clause: its collection name and optional alias used to qualify columns. */
final case class TableRef(collection: String, alias: Option[String])

/** A JOIN of a [[TableRef]] with an `ON` condition (a conjunction of equalities, plus any residual). */
final case class Join(joinType: JoinType, table: TableRef, on: Expr)

/** SQL aggregate functions. `COUNT` may take `*` (no column); the rest require a column. */
enum AggFunc { case Count, Sum, Avg, Min, Max }

/** One element of a (non-`*`) projection: either a plain column or an aggregate over a column (`column = None` is the `COUNT(*)` special case). `alias` is the optional `AS <name>` and, when present, both names the output column and may be referenced
  * by ORDER BY.
  */
sealed trait SelectItem { def alias: Option[String] }
object SelectItem       {
  final case class Col(name: String, alias: Option[String] = None) extends SelectItem

  /** An aggregate over an expression argument (`arg = None` is `COUNT(*)`), optionally `DISTINCT`. */
  final case class Agg(func: AggFunc, arg: Option[zio.lmdb.sql.parser.Expr], distinct: Boolean = false, alias: Option[String] = None) extends SelectItem

  /** A scalar expression projection, e.g. `GEO_DISTANCE(...)` or `LENGTH(name)`. Evaluated per row; only valid in a non-aggregate `SELECT`.
    */
  final case class Expr(expr: zio.lmdb.sql.parser.Expr, alias: Option[String] = None) extends SelectItem
}

sealed trait Projection
object Projection {
  case object Star extends Projection

  /** A non-`*` projection: plain columns and/or aggregates, each optionally aliased. */
  final case class Items(items: List[SelectItem]) extends Projection
}

/** One ORDER BY key: a scalar expression and its direction. A bare `Expr.Col` may name an output alias or a (qualified) column; a function expression (e.g. `GEO_DISTANCE(...)`) sorts by the computed value. A query may carry several keys (`ORDER BY
  * a, b DESC`), applied left-to-right.
  */
final case class OrderBy(expr: Expr, descending: Boolean)

enum CmpOp { case Eq, Ne, Lt, Le, Gt, Ge }

enum ArithOp { case Add, Sub, Mul, Div, Mod }

sealed trait Expr
object Expr {
  final case class Col(name: String)                       extends Expr
  final case class Lit(value: Literal)                     extends Expr
  final case class Cmp(op: CmpOp, left: Expr, right: Expr) extends Expr

  /** A binary arithmetic expression, e.g. `geo_distance(...) / 1000`. */
  final case class Arith(op: ArithOp, left: Expr, right: Expr) extends Expr
  final case class And(left: Expr, right: Expr)                extends Expr
  final case class Or(left: Expr, right: Expr)                 extends Expr
  final case class Not(inner: Expr)                            extends Expr
  final case class Like(target: Expr, pattern: String)         extends Expr
  final case class IsNull(target: Expr, negated: Boolean)      extends Expr

  /** `target [NOT] IN (item, …)` — membership test against an explicit value list. */
  final case class In(target: Expr, items: List[Expr], negated: Boolean) extends Expr

  /** `target [NOT] BETWEEN low AND high` — inclusive range test (`low <= target <= high`). */
  final case class Between(target: Expr, low: Expr, high: Expr, negated: Boolean) extends Expr

  /** A scalar function call, e.g. `LENGTH(name)`. `name` is lower-cased. */
  final case class Func(name: String, args: List[Expr]) extends Expr

  /** A `CASE` expression. `subject = None` is the searched form (`CASE WHEN <cond> THEN <r> … END`, each branch condition a boolean); `subject = Some(e)` is the simple form (`CASE <e> WHEN <v> THEN <r> … END`, each branch value compared to `e` for
    * equality). `default` is the optional `ELSE` (absent ⇒ `NULL` when no branch matches).
    */
  final case class Case(subject: Option[Expr], branches: List[(Expr, Expr)], default: Option[Expr]) extends Expr

  /** An aggregate reference inside an expression (e.g. in HAVING), e.g. `COUNT(*)`, `SUM(a + b)`, `COUNT(DISTINCT col)`. `arg = None` is `COUNT(*)`; `distinct` dedupes the argument values.
    */
  final case class Aggregate(func: AggFunc, arg: Option[Expr], distinct: Boolean) extends Expr
}

sealed trait Literal
object Literal {
  final case class StrLit(value: String)     extends Literal
  final case class IntLit(value: Long)       extends Literal
  final case class DecLit(value: BigDecimal) extends Literal
  final case class BoolLit(value: Boolean)   extends Literal
  case object NullLit                        extends Literal
}
