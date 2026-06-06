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
    Set("select", "from", "where", "order", "by", "asc", "desc", "limit", "insert", "into", "values",
        "update", "set", "delete", "describe", "show", "collections", "indexes", "and", "or", "not",
        "like", "is", "null", "true", "false")

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
    P(("(" ~ expr ~ ")") | literal.map(Expr.Lit(_)) | ident.map(Expr.Col(_)))

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

  private def countProj[$: P]: P[Projection] =
    P(kw("count") ~ "(" ~ (P("*").map(_ => None) | ident.map(Some(_))) ~ ")").map(Projection.Count(_))

  private def projection[$: P]: P[Projection] =
    P(countProj | P("*").map(_ => Projection.Star) | ident.rep(1, sep = ",").map(ns => Projection.Columns(ns.toList)))

  private def orderBy[$: P]: P[OrderBy] =
    P(kw("order") ~ kw("by") ~ ident ~ (kw("asc").map(_ => false) | kw("desc").map(_ => true)).?.map(_.getOrElse(false)))
      .map { case (c, d) => OrderBy(c, d) }

  private def selectStmt[$: P]: P[Statement.Select] =
    P(kw("select") ~ projection ~ kw("from") ~ ident ~ (kw("where") ~ expr).? ~ orderBy.? ~ (kw("limit") ~ intNumber).?)
      .map { case (proj, from, w, ob, lim) => Statement.Select(proj, from, w, ob, lim) }

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
