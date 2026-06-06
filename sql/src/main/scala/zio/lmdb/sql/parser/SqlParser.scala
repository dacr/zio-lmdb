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

  /** A column reference, optionally qualified by a table alias: `col` or `alias.col`. */
  private def colName[$: P]: P[String] = P(ident ~~ ("." ~~ ident).?).map { case (a, b) => b.fold(a)(c => s"$a.$c") }

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
    P(("(" ~ expr ~ ")") | aggExpr | scalarFuncExpr | literal.map(Expr.Lit(_)) | colName.map(Expr.Col(_)))

  /** An aggregate reference inside an expression (e.g. in HAVING): COUNT(*), SUM(col), … */
  private def aggExpr[$: P]: P[Expr] =
    P(
      (kw("count") ~ "(" ~ "*" ~ ")").map(_ => Expr.Aggregate(AggFunc.Count, None)) |
        (aggFunc ~ "(" ~ colName ~ ")").map { case (f, c) => Expr.Aggregate(f, Some(c)) }
    )

  /** Scalar functions usable in WHERE/HAVING. Currently LENGTH(<expr>). */
  private def scalarFuncExpr[$: P]: P[Expr] =
    P(kw("length").map(_ => "length") ~ "(" ~ expr ~ ")").map { case (name, arg) => Expr.Func(name, List(arg)) }

  private def term[$: P]: P[Expr] =
    P(
      primary ~ (
        (kw("is") ~ kw("not").map(_ => true).? ~ kw("null")).map(neg => (e: Expr) => Expr.IsNull(e, neg.getOrElse(false))) |
          (kw("like") ~ sqlString).map(p => (e: Expr) => Expr.Like(e, p)) |
          (cmpOp ~ primary).map { case (op, r) => (e: Expr) => Expr.Cmp(op, e, r) }
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

  private def aggItem[$: P]: P[SelectItem.Agg] =
    P(
      (kw("count") ~ "(" ~ "*" ~ ")").map(_ => SelectItem.Agg(AggFunc.Count, None)) |
        (aggFunc ~ "(" ~ colName ~ ")").map { case (f, c) => SelectItem.Agg(f, Some(c)) }
    )

  /** Optional `AS <name>` column alias. */
  private def aliasOpt[$: P]: P[Option[String]] = P((kw("as") ~ ident).?)

  private def selectItem[$: P]: P[SelectItem] =
    P(
      (aggItem ~ aliasOpt).map { case (agg, al) => agg.copy(alias = al) } |
        (colName ~ aliasOpt).map { case (n, al) => SelectItem.Col(n, al) }
    )

  private def projection[$: P]: P[Projection] =
    P(P("*").map(_ => Projection.Star) | selectItem.rep(1, sep = ",").map(items => Projection.Items(items.toList)))

  private def orderBy[$: P]: P[OrderBy] =
    P(kw("order") ~ kw("by") ~ colName ~ (kw("asc").map(_ => false) | kw("desc").map(_ => true)).?.map(_.getOrElse(false)))
      .map { case (c, d) => OrderBy(c, d) }

  private def distinctKw[$: P]: P[Boolean] =
    P((kw("distinct").map(_ => true)).?).map(_.getOrElse(false))

  private def groupByClause[$: P]: P[List[String]] =
    P(kw("group") ~ kw("by") ~ colName.rep(1, sep = ",")).map(_.toList)

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
    P(kw("select") ~ distinctKw ~ projection ~ kw("from") ~ tableRef ~ joinClause.rep ~ (kw("where") ~ expr).? ~ groupByClause.? ~ (kw("having") ~ expr).? ~ orderBy.? ~ (kw("limit") ~ intNumber).?)
      .map { case (distinct, proj, fromRef, joins, w, gb, hv, ob, lim) =>
        Statement.Select(proj, distinct, fromRef.collection, w, gb.getOrElse(Nil), hv, ob, lim, fromRef.alias, joins.toList)
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
