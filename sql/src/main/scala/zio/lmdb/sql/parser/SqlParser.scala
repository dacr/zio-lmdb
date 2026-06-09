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

import fastparse.*
import fastparse.MultiLineWhitespace.*
import zio.lmdb.sql.SqlError

/** fastparse grammar for the L2C SQL subset. Pure `String => Either[SqlError.Parse, Statement]`;
  * no ZIO, no LMDB. Keywords are case-insensitive; identifiers are letters/digits/underscore (so
  * the `_key` pseudo-column is a valid identifier).
  */
object SqlParser {

  private val reserved: Set[String] =
    Set("select", "distinct", "as", "from", "join", "inner", "left", "outer", "on", "where", "group", "having", "order", "by", "asc", "desc", "limit",
        "insert", "into", "values", "update", "set", "delete", "describe", "show", "collections", "indexes", "and", "or", "not",
        "like", "is", "null", "true", "false")

  /** A column reference as a dotted path: a bare column (`col`), a table-qualified column
    * (`alias.col`), or a nested value path of any depth (`alias.field.sub`, `field.sub.leaf`). The
    * first segment may be a table alias or a pseudo-column (`_key`/`_value`); any further segments
    * index into nested object fields. */
  private def colName[$: P]: P[String] =
    P(ident ~~ ("." ~~ ident).repX).map { case (head, tail) => (head +: tail).mkString(".") }

  private def kw[$: P](s: String): P[Unit] = P(IgnoreCase(s) ~~ !CharPred(c => c.isLetterOrDigit || c == '_'))

  private def ident[$: P]: P[String] =
    P((CharPred(c => c.isLetter || c == '_') ~~ CharsWhileIn("a-zA-Z0-9_", 0)).!).filter(s => !reserved.contains(s.toLowerCase))

  private def sqlString[$: P]: P[String] =
    P("'" ~~ (P("''").map(_ => "'") | CharPred(_ != '\'').!).repX.map(_.mkString) ~~ "'")

  private def numberLit[$: P]: P[Literal] =
    P(("-".? ~~ CharsWhileIn("0-9") ~~ ("." ~~ CharsWhileIn("0-9")).?).!).map { s =>
      if (s.contains(".")) Literal.DecLit(BigDecimal(s)) else Literal.IntLit(s.toLong)
    }

  private def intNumber[$: P]: P[Long] = P(("-".? ~~ CharsWhileIn("0-9")).!).map(_.toLong)

  private def literal[$: P]: P[Literal] =
    P(
      sqlString.map(Literal.StrLit(_)) |
        kw("true").map(_ => Literal.BoolLit(true)) |
        kw("false").map(_ => Literal.BoolLit(false)) |
        kw("null").map(_ => Literal.NullLit) |
        numberLit
    )

  private def cmpOp[$: P]: P[CmpOp] =
    P(
      P("<=").map(_ => CmpOp.Le) | P(">=").map(_ => CmpOp.Ge) | P("<>").map(_ => CmpOp.Ne) |
        P("!=").map(_ => CmpOp.Ne) | P("=").map(_ => CmpOp.Eq) | P("<").map(_ => CmpOp.Lt) | P(">").map(_ => CmpOp.Gt)
    )

  private def primary[$: P]: P[Expr] =
    P(("(" ~ expr ~ ")") | aggExpr | funcExpr | literal.map(Expr.Lit(_)) | colName.map(Expr.Col(_)))

  /** An aggregate reference inside an expression (e.g. in HAVING): COUNT(*), SUM(col), … */
  private def aggExpr[$: P]: P[Expr] =
    P(
      (kw("count") ~ "(" ~ "*" ~ ")").map(_ => Expr.Aggregate(AggFunc.Count, None)) |
        (aggFunc ~ "(" ~ colName ~ ")").map { case (f, c) => Expr.Aggregate(f, Some(c)) }
    )

  /** A scalar function call `name(arg, …)` — e.g. `LENGTH(name)`, `YEAR(timestamp)`, `NOW()`,
    * `GEO_DISTANCE(lat1, lon1, lat2, lon2)`, `GEO_WITHIN(point, lat, lon, radius)`. Function names are
    * case-insensitive and not reserved, so an identifier not followed by `(` falls through to a column
    * reference; aggregates are matched earlier in `primary`. Zero-argument calls (e.g. `NOW()`) are
    * allowed. */
  private def funcExpr[$: P]: P[Expr] =
    P(ident ~ "(" ~ expr.rep(0, sep = ",") ~ ")").map { case (name, args) => Expr.Func(name.toLowerCase, args.toList) }

  // Arithmetic binds tighter than comparison: `*` `/` `%` over `+` `-`, both over `=`/`<`/… .
  private def arithMulOp[$: P]: P[ArithOp] =
    P(P("*").map(_ => ArithOp.Mul) | P("/").map(_ => ArithOp.Div) | P("%").map(_ => ArithOp.Mod))

  private def arithAddOp[$: P]: P[ArithOp] =
    P(P("+").map(_ => ArithOp.Add) | P("-").map(_ => ArithOp.Sub))

  private def multiplicative[$: P]: P[Expr] =
    P(primary ~ (arithMulOp ~ primary).rep).map { case (h, t) => t.foldLeft(h) { case (acc, (op, r)) => Expr.Arith(op, acc, r) } }

  private def additive[$: P]: P[Expr] =
    P(multiplicative ~ (arithAddOp ~ multiplicative).rep).map { case (h, t) => t.foldLeft(h) { case (acc, (op, r)) => Expr.Arith(op, acc, r) } }

  private def term[$: P]: P[Expr] =
    P(
      additive ~ (
        (kw("is") ~ kw("not").map(_ => true).? ~ kw("null")).map(neg => (e: Expr) => Expr.IsNull(e, neg.getOrElse(false))) |
          (kw("like") ~ sqlString).map(p => (e: Expr) => Expr.Like(e, p)) |
          (cmpOp ~ additive).map { case (op, r) => (e: Expr) => Expr.Cmp(op, e, r) }
      ).?
    ).map { case (e, fOpt) => fOpt.map(_(e)).getOrElse(e) }

  private def notExpr[$: P]: P[Expr] = P((kw("not") ~ notExpr).map(Expr.Not(_)) | term)

  private def andExpr[$: P]: P[Expr] = P(notExpr ~ (kw("and") ~ notExpr).rep).map { case (h, t) => t.foldLeft(h)(Expr.And(_, _)) }

  private def expr[$: P]: P[Expr] = P(andExpr ~ (kw("or") ~ andExpr).rep).map { case (h, t) => t.foldLeft(h)(Expr.Or(_, _)) }

  // Aggregate function names are not reserved, so `count`, `sum`, ... stay usable as column names;
  // they only read as aggregates when directly followed by `(`.
  private def aggFunc[$: P]: P[AggFunc] =
    P(
      kw("count").map(_ => AggFunc.Count) | kw("sum").map(_ => AggFunc.Sum) | kw("avg").map(_ => AggFunc.Avg) |
        kw("min").map(_ => AggFunc.Min) | kw("max").map(_ => AggFunc.Max)
    )

  /** Optional `AS <name>` column alias. */
  private def aliasOpt[$: P]: P[Option[String]] = P((kw("as") ~ ident).?)

  /** A projected item: any scalar expression, classified into a plain column, an aggregate, or a
    * general expression (the last covers `GEO_DISTANCE(...)`, `LENGTH(...)`, …). */
  private def selectItem[$: P]: P[SelectItem] =
    P(expr ~ aliasOpt).map {
      case (Expr.Col(n), al)          => SelectItem.Col(n, al)
      case (Expr.Aggregate(f, c), al) => SelectItem.Agg(f, c, al)
      case (e, al)                    => SelectItem.Expr(e, al)
    }

  private def projection[$: P]: P[Projection] =
    P(P("*").map(_ => Projection.Star) | selectItem.rep(1, sep = ",").map(items => Projection.Items(items.toList)))

  /** One ORDER BY key: an expression with an optional `ASC`/`DESC` (ascending by default). */
  private def orderKey[$: P]: P[OrderBy] =
    P(expr ~ (kw("asc").map(_ => false) | kw("desc").map(_ => true)).?.map(_.getOrElse(false))).map { case (e, d) => OrderBy(e, d) }

  /** `ORDER BY <key> [, <key>]…` — several keys, applied left-to-right. */
  private def orderByClause[$: P]: P[List[OrderBy]] =
    P(kw("order") ~ kw("by") ~ orderKey.rep(1, sep = ",")).map(_.toList)

  private def distinctKw[$: P]: P[Boolean] =
    P((kw("distinct").map(_ => true)).?).map(_.getOrElse(false))

  /** `GROUP BY <expr> [, <expr>]…` — each key may be a column, a (qualified/nested) path, an output
    * alias, or any scalar expression (e.g. `YEAR(timestamp)`). */
  private def groupByClause[$: P]: P[List[Expr]] =
    P(kw("group") ~ kw("by") ~ expr.rep(1, sep = ",")).map(_.toList)

  /** `<collection> [[AS] <alias>]`. The alias parser stops at keywords (reserved), so a missing alias
    * followed by JOIN/WHERE/… is handled naturally. */
  private def tableRef[$: P]: P[TableRef] =
    P(ident ~ (kw("as").? ~ ident).?).map { case (name, alias) => TableRef(name, alias) }

  private def joinType[$: P]: P[JoinType] =
    P((kw("left") ~ kw("outer").?).map(_ => JoinType.Left) | kw("inner").map(_ => JoinType.Inner) | Pass.map(_ => JoinType.Inner))

  private def joinClause[$: P]: P[Join] =
    P(joinType ~ kw("join") ~ tableRef ~ kw("on") ~ expr).map { case (jt, tr, on) => Join(jt, tr, on) }

  // Standard SQL clause order: SELECT … FROM … [JOIN …] WHERE … GROUP BY … HAVING … ORDER BY … LIMIT.
  private def selectStmt[$: P]: P[Statement.Select] =
    P(kw("select") ~ distinctKw ~ projection ~ kw("from") ~ tableRef ~ joinClause.rep ~ (kw("where") ~ expr).? ~ groupByClause.? ~ (kw("having") ~ expr).? ~ orderByClause.? ~ (kw("limit") ~ intNumber).?)
      .map { case (distinct, proj, fromRef, joins, w, gb, hv, ob, lim) =>
        Statement.Select(proj, distinct, fromRef.collection, w, gb.getOrElse(Nil), hv, ob.getOrElse(Nil), lim, fromRef.alias, joins.toList)
      }

  private def insertStmt[$: P]: P[Statement.Insert] =
    P(kw("insert") ~ kw("into") ~ ident ~ "(" ~ ident.rep(1, sep = ",") ~ ")" ~ kw("values") ~ "(" ~ literal.rep(1, sep = ",") ~ ")")
      .map { case (into, cols, vals) => Statement.Insert(into, cols.toList, vals.toList) }

  private def updateStmt[$: P]: P[Statement.Update] =
    P(kw("update") ~ ident ~ kw("set") ~ (ident ~ "=" ~ literal).rep(1, sep = ",") ~ (kw("where") ~ expr).?)
      .map { case (t, as, w) => Statement.Update(t, as.map { case (c, v) => (c, v) }.toList, w) }

  private def deleteStmt[$: P]: P[Statement.Delete] =
    P(kw("delete") ~ kw("from") ~ ident ~ (kw("where") ~ expr).?).map { case (f, w) => Statement.Delete(f, w) }

  private def describeStmt[$: P]: P[Statement.Describe] =
    P((kw("describe") | kw("desc")) ~ ident).map(Statement.Describe(_))

  private def showStmt[$: P]: P[Statement.Show] =
    P(kw("show") ~ (kw("collections").map(_ => ShowTarget.Collections) | kw("indexes").map(_ => ShowTarget.Indexes))).map(Statement.Show(_))

  private def statement[$: P]: P[Statement] =
    P(selectStmt | insertStmt | updateStmt | deleteStmt | describeStmt | showStmt)

  private def top[$: P]: P[Statement] = P(Start ~ statement ~ ";".? ~ End)

  /** Parse one SQL statement. */
  def parse(text: String): Either[SqlError.Parse, Statement] =
    fastparse.parse(text.trim, top(using _)) match {
      case Parsed.Success(stmt, _) => Right(stmt)
      case f: Parsed.Failure       => Left(SqlError.Parse(f.index, f.trace().longAggregateMsg))
    }
}
