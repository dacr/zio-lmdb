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
package zio.lmdb.sql.engine

import zio.*
import zio.stream.ZStream
import zio.lmdb.*
import zio.lmdb.json.JValue
import zio.lmdb.json.JValue.*
import zio.lmdb.sql.SqlError
import zio.lmdb.sql.parser.*
import zio.lmdb.sql.result.{Column, QueryResult}
import zio.lmdb.sql.runtime.KeyRegistry

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.immutable.ListMap

/** The L2C execution engine: `parse → bind (catalog) → execute` over dynamic `(KeyValue, JValue)`
  * rows. Reads use the `[Array[Byte], Array[Byte]]` escape hatch and decode generically (key via the
  * recorded `keyId`, value as plain JSON), so no static `K`/`T` is needed. Reads stream; `ORDER BY`
  * is the one buffering operator.
  */
object SqlEngine {

  /** Raw value bytes, used verbatim. */
  private given rawValueCodec: LMDBCodec[Array[Byte]] = new LMDBCodec[Array[Byte]] {
    override def encode(value: Array[Byte]): Array[Byte]                            = value
    override def decode(valueBytes: ByteBuffer): Either[String, Array[Byte]]        = {
      val a = new Array[Byte](valueBytes.remaining()); valueBytes.get(a); Right(a)
    }
  }

  /** A decoded row: original key bytes (for writes), the decoded key, and the value tree. */
  private final case class RawRow(keyBytes: Array[Byte], key: KeyValue, value: JValue)

  // ── public entry point ─────────────────────────────────────────────────────────────────────

  /** Parse and execute one SQL statement, yielding a streaming [[QueryResult]]. */
  def run(sql: String): ZIO[LMDB, SqlError, QueryResult] =
    ZIO.fromEither(SqlParser.parse(sql)).flatMap(stmt => ZIO.serviceWithZIO[LMDB](execute(_, stmt)))

  private def execute(lmdb: LMDB, stmt: Statement): IO[SqlError, QueryResult] =
    stmt match {
      case s: Statement.Select   => Catalog.lookup(lmdb, s.from).map(selectResult(lmdb, _, s))
      case i: Statement.Insert   => Catalog.lookup(lmdb, i.into).flatMap(insertResult(lmdb, _, i))
      case u: Statement.Update   => Catalog.lookup(lmdb, u.table).flatMap(updateResult(lmdb, _, u))
      case d: Statement.Delete   => Catalog.lookup(lmdb, d.from).flatMap(deleteResult(lmdb, _, d))
      case d: Statement.Describe => Catalog.lookup(lmdb, d.collection).map(describeResult)
      case sh: Statement.Show    => showResult(lmdb, sh.target)
    }

  // ── SELECT ─────────────────────────────────────────────────────────────────────────────────

  private def selectResult(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select): QueryResult =
    sel.projection match {
      case Projection.Count(arg) => countResult(lmdb, info, sel.where, arg)
      case proj                  =>
        val cols  = projectionColumns(info, proj)
        val names = cols.map(_.name)

        val filtered = filterWhere(rawStream(lmdb, info), sel.where)

        val ordered: ZStream[Any, SqlError, RawRow] = sel.orderBy match {
          case Some(ob) => ZStream.fromIterableZIO(filtered.runCollect.map(c => sortRows(c.toList, ob)))
          case None     => filtered
        }
        val limited   = sel.limit.fold(ordered)(n => ordered.take(n))
        val projected = limited.map(r => projectRow(r, names))
        QueryResult(cols, projected)
    }

  /** `SELECT COUNT(*)` / `COUNT(col)`: a single scalar row, counted by streaming once over the
    * WHERE-filtered rows (no buffering). `COUNT(col)` counts rows whose `col` is non-null. ORDER BY
    * and LIMIT are meaningless on a lone aggregate and are ignored.
    */
  private def countResult(lmdb: LMDB, info: CollectionInfo, where: Option[Expr], arg: Option[String]): QueryResult = {
    val filtered = filterWhere(rawStream(lmdb, info), where)
    val counted  = arg match {
      case None      => filtered.runCount
      case Some(col) => filtered.filter(r => lookup(r)(col) != NullV).runCount
    }
    val row = counted.map(n => MapV(ListMap("count" -> LongV(n))): JValue)
    QueryResult(List(Column("count", "integer")), ZStream.fromZIO(row))
  }

  private def projectionColumns(info: CollectionInfo, proj: Projection): List[Column] =
    proj match {
      case Projection.Star =>
        val valueCols =
          if (info.columns.nonEmpty) info.columns.map(c => Column(c.name, c.typeHint))
          else List(Column("_value", "any"))
        Column("_key", info.keyId.getOrElse("key")) :: valueCols
      case Projection.Columns(ns) => ns.map(n => Column(n, hintFor(info, n)))
      case Projection.Count(_)    => List(Column("count", "integer"))
    }

  private def hintFor(info: CollectionInfo, name: String): String =
    name match {
      case "_key"   => info.keyId.getOrElse("key")
      case "_value" => "any"
      case other    => info.columns.find(_.name == other).map(_.typeHint).getOrElse("any")
    }

  // ── writes ───────────────────────────────────────────────────────────────────────────────────

  private def insertResult(lmdb: LMDB, info: CollectionInfo, ins: Statement.Insert): IO[SqlError, QueryResult] = {
    val keyId = info.keyId.getOrElse("lmdb:str")
    val pairs = ins.columns.zip(ins.values)
    pairs.collectFirst { case ("_key", v) => v } match {
      case None         => ZIO.fail(SqlError.TypeMismatch("INSERT requires a '_key' column"))
      case Some(keyLit) =>
        ZIO.fromEither(KeyRegistry.encodeLiteral(keyId, keyLit)).mapError(SqlError.KeyError(_)).flatMap { keyBytes =>
          val valueFields = pairs.filterNot(_._1 == "_key")
          val valueJV     = valueFields match {
            case List(("_value", v)) => litToJV(v)
            case _                    => MapV(ListMap.from(valueFields.map { case (c, v) => c -> litToJV(v) }))
          }
          lmdb
            .upsertOverwrite[Array[Byte], Array[Byte]](info.name, keyBytes, JValue.toPlainJson(valueJV))
            .mapError(e => SqlError.Storage(e.toString))
            .as(QueryResult.affected(1))
        }
    }
  }

  private def updateResult(lmdb: LMDB, info: CollectionInfo, upd: Statement.Update): IO[SqlError, QueryResult] =
    collectMatching(lmdb, info, upd.where).flatMap { rows =>
      ZIO
        .foreachDiscard(rows) { r =>
          val newValue = applyAssignments(r.value, upd.assignments)
          lmdb
            .upsertOverwrite[Array[Byte], Array[Byte]](info.name, r.keyBytes, JValue.toPlainJson(newValue))
            .mapError(e => SqlError.Storage(e.toString))
        }
        .as(QueryResult.affected(rows.size.toLong))
    }

  private def applyAssignments(value: JValue, assignments: List[(String, Literal)]): JValue =
    assignments.collectFirst { case ("_value", v) => v } match {
      case Some(v) => litToJV(v)
      case None    =>
        val base = value match { case MapV(m) => m; case _ => ListMap.empty[String, JValue] }
        MapV(base ++ assignments.map { case (c, v) => c -> litToJV(v) })
    }

  private def deleteResult(lmdb: LMDB, info: CollectionInfo, del: Statement.Delete): IO[SqlError, QueryResult] =
    collectMatching(lmdb, info, del.where).flatMap { rows =>
      ZIO
        .foreachDiscard(rows) { r =>
          lmdb.delete[Array[Byte], Array[Byte]](info.name, r.keyBytes).mapError(e => SqlError.Storage(e.toString)).unit
        }
        .as(QueryResult.affected(rows.size.toLong))
    }

  // ── DESCRIBE / SHOW ──────────────────────────────────────────────────────────────────────────

  private def describeResult(info: CollectionInfo): QueryResult = {
    def row(col: String, tpe: String): JValue = MapV(ListMap("column" -> StringV(col), "type" -> StringV(tpe)))
    val keyRow   = row("_key", info.keyId.getOrElse("(unknown)"))
    val valueRows =
      if (info.columns.nonEmpty) info.columns.map(c => row(c.name, c.typeHint))
      else List(row("_value", "any"))
    QueryResult.of(List(Column("column", "string"), Column("type", "string")), keyRow :: valueRows)
  }

  private def showResult(lmdb: LMDB, target: ShowTarget): IO[SqlError, QueryResult] =
    Catalog.list(lmdb).map { entries =>
      val wantIndex = target == ShowTarget.Indexes
      val rows = entries
        .filter(e => if (wantIndex) e.collectionKind == CollectionKind.Index else e.collectionKind != CollectionKind.Index)
        .sortBy(_.collectionName)
        .map(e => MapV(ListMap("name" -> StringV(e.collectionName), "kind" -> StringV(e.collectionKind.toString))))
      QueryResult.of(List(Column("name", "string"), Column("kind", "string")), rows)
    }

  // ── row plumbing ─────────────────────────────────────────────────────────────────────────────

  private def rawStream(lmdb: LMDB, info: CollectionInfo): ZStream[Any, SqlError, RawRow] = {
    val keyId = info.keyId.getOrElse("lmdb:bytes")
    lmdb
      .streamWithKeys[Array[Byte], Array[Byte]](info.name)
      .mapError(e => SqlError.Storage(e.toString): SqlError)
      .map { case (kb, vb) => RawRow(kb, KeyRegistry.decode(keyId, kb), decodeValueBytes(vb)) }
  }

  private def filterWhere(stream: ZStream[Any, SqlError, RawRow], where: Option[Expr]): ZStream[Any, SqlError, RawRow] =
    where.fold(stream)(w => stream.filter(r => evalBool(w, lookup(r))))

  private def collectMatching(lmdb: LMDB, info: CollectionInfo, where: Option[Expr]): IO[SqlError, List[RawRow]] =
    filterWhere(rawStream(lmdb, info), where).runCollect.map(_.toList)

  private def decodeValueBytes(vb: Array[Byte]): JValue =
    JValue.fromPlainJson(vb).fold(_ => StringV(new String(vb, UTF_8)), identity)

  private def lookup(r: RawRow)(name: String): JValue =
    name match {
      case "_key"   => r.key.toJValue
      case "_value" => r.value
      case other    => r.value match { case MapV(m) => m.getOrElse(other, NullV); case _ => NullV }
    }

  private def projectRow(r: RawRow, names: List[String]): JValue =
    MapV(ListMap.from(names.map(n => n -> lookup(r)(n))))

  private def sortRows(rows: List[RawRow], ob: OrderBy): List[RawRow] =
    rows.sortWith { (a, b) =>
      val c = cmpTotal(lookup(a)(ob.column), lookup(b)(ob.column))
      if (ob.descending) c > 0 else c < 0
    }

  // ── value semantics ──────────────────────────────────────────────────────────────────────────

  private def litToJV(l: Literal): JValue =
    l match {
      case Literal.StrLit(s)  => StringV(s)
      case Literal.IntLit(n)  => LongV(n)
      case Literal.DecLit(d)  => DecimalV(d)
      case Literal.BoolLit(b) => BoolV(b)
      case Literal.NullLit    => NullV
    }

  private def asBigDecimal(jv: JValue): Option[BigDecimal] =
    jv match {
      case LongV(v)    => Some(BigDecimal(v))
      case DoubleV(v)  => Some(BigDecimal(v))
      case DecimalV(v) => Some(v)
      case _           => None
    }

  private def compareJV(a: JValue, b: JValue): Option[Int] =
    (a, b) match {
      case (StringV(x), StringV(y))         => Some(x.compareTo(y))
      case (BoolV(x), BoolV(y))             => Some(x.compareTo(y))
      case (InstantV(x), InstantV(y))       => Some(x.compareTo(y))
      case (IdentifierV(x), IdentifierV(y)) => Some(x.compareTo(y))
      case _                                =>
        (asBigDecimal(a), asBigDecimal(b)) match {
          case (Some(x), Some(y)) => Some(x.compare(y))
          case _                  => None
        }
    }

  private def typeRank(jv: JValue): Int =
    jv match {
      case BoolV(_)                            => 1
      case LongV(_) | DoubleV(_) | DecimalV(_) => 2
      case StringV(_)                          => 3
      case InstantV(_)                         => 4
      case IdentifierV(_)                      => 5
      case NullV                               => 9
      case _                                   => 8
    }

  private def cmpTotal(a: JValue, b: JValue): Int = compareJV(a, b).getOrElse(typeRank(a) - typeRank(b))

  private def eqJV(a: JValue, b: JValue): Boolean =
    compareJV(a, b) match {
      case Some(0) => true
      case Some(_) => false
      case None    => a == b && a != NullV
    }

  private def compareOp(op: CmpOp, a: JValue, b: JValue): Boolean =
    op match {
      case CmpOp.Eq => eqJV(a, b)
      case CmpOp.Ne => !eqJV(a, b)
      case CmpOp.Lt => compareJV(a, b).exists(_ < 0)
      case CmpOp.Le => compareJV(a, b).exists(_ <= 0)
      case CmpOp.Gt => compareJV(a, b).exists(_ > 0)
      case CmpOp.Ge => compareJV(a, b).exists(_ >= 0)
    }

  private def operand(e: Expr, lk: String => JValue): JValue =
    e match {
      case Expr.Col(n) => lk(n)
      case Expr.Lit(l) => litToJV(l)
      case other       => BoolV(evalBool(other, lk))
    }

  private def evalBool(e: Expr, lk: String => JValue): Boolean =
    e match {
      case Expr.And(l, r)      => evalBool(l, lk) && evalBool(r, lk)
      case Expr.Or(l, r)       => evalBool(l, lk) || evalBool(r, lk)
      case Expr.Not(x)         => !evalBool(x, lk)
      case Expr.Cmp(op, l, r)  => compareOp(op, operand(l, lk), operand(r, lk))
      case Expr.IsNull(t, neg) => val isN = operand(t, lk) == NullV; if (neg) !isN else isN
      case Expr.Like(t, pat)   => operand(t, lk) match { case StringV(s) => likeMatch(s, pat); case _ => false }
      case Expr.Col(_)         => operand(e, lk) match { case BoolV(b) => b; case _ => false }
      case Expr.Lit(_)         => operand(e, lk) match { case BoolV(b) => b; case _ => false }
    }

  private def likeMatch(s: String, pattern: String): Boolean = {
    val sb = new StringBuilder("^")
    pattern.foreach {
      case '%'                                  => sb.append(".*")
      case '_'                                  => sb.append('.')
      case c if "\\.[]{}()*+-?^$|".indexOf(c) >= 0 => sb.append('\\').append(c)
      case c                                    => sb.append(c)
    }
    sb.append('$')
    s.matches(sb.toString)
  }
}
