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
      case s: Statement.Select   => Catalog.lookup(lmdb, s.from).flatMap(selectResult(lmdb, _, s))
      case i: Statement.Insert   => Catalog.lookup(lmdb, i.into).flatMap(insertResult(lmdb, _, i))
      case u: Statement.Update   => Catalog.lookup(lmdb, u.table).flatMap(updateResult(lmdb, _, u))
      case d: Statement.Delete   => Catalog.lookup(lmdb, d.from).flatMap(deleteResult(lmdb, _, d))
      case d: Statement.Describe => Catalog.lookup(lmdb, d.collection).map(describeResult)
      case sh: Statement.Show    => showResult(lmdb, sh.target)
    }

  // ── SELECT ─────────────────────────────────────────────────────────────────────────────────

  /** Three shapes: an aggregate/`GROUP BY` query (buffers per-group state), a `DISTINCT` query
    * (buffers to de-duplicate), or a plain streaming `SELECT` (the only one that never buffers).
    */
  private def selectResult(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select): IO[SqlError, QueryResult] =
    ZIO.fromEither(validateNoAggregatesInWhere(sel.where)) *> {
      if (isAggregate(sel)) aggregateResult(lmdb, info, sel)
      else if (sel.distinct) ZIO.succeed(distinctResult(lmdb, info, sel))
      else ZIO.succeed(streamingResult(lmdb, info, sel))
    }

  /** HAVING and aggregate projections both put the query on the grouping path. */
  private def isAggregate(sel: Statement.Select): Boolean =
    sel.groupBy.nonEmpty || sel.having.isDefined || (sel.projection match {
      case Projection.Items(items) => items.exists { case _: SelectItem.Agg => true; case _ => false }
      case Projection.Star         => false
    })

  private def validateNoAggregatesInWhere(where: Option[Expr]): Either[SqlError, Unit] =
    if (where.toList.flatMap(aggsInExpr).isEmpty) Right(())
    else Left(SqlError.Unsupported("aggregate functions are not allowed in WHERE (use HAVING)"))

  // ── plain streaming SELECT ─────────────────────────────────────────────────────────────────────

  private def streamingResult(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select): QueryResult = {
    val cols = projectionColumns(info, sel.projection)
    val plan = plainProjection(info, sel.projection)

    val filtered = filterWhere(rawStream(lmdb, info), sel.where)

    val ordered: ZStream[Any, SqlError, RawRow] = sel.orderBy match {
      case Some(ob) => ZStream.fromIterableZIO(filtered.runCollect.map(c => sortRows(c.toList, resolveOrderSource(sel.projection, ob))))
      case None     => filtered
    }
    val limited   = sel.limit.fold(ordered)(n => ordered.take(n))
    val projected = limited.map(r => projectRow(r, plan))
    QueryResult(cols, projected)
  }

  /** Output-name → source-column pairs for a non-aggregate projection (resolves `AS` aliases). */
  private def plainProjection(info: CollectionInfo, proj: Projection): List[(String, String)] =
    proj match {
      case Projection.Star         => projectionColumns(info, proj).map(c => c.name -> c.name)
      case Projection.Items(items) => items.collect { case SelectItem.Col(n, al) => al.getOrElse(n) -> n }
    }

  /** When ORDER BY targets a column alias, sort the underlying rows by the aliased source column. */
  private def resolveOrderSource(proj: Projection, ob: OrderBy): OrderBy =
    proj match {
      case Projection.Items(items) => items.collectFirst { case SelectItem.Col(n, Some(al)) if al == ob.column => n }.fold(ob)(src => ob.copy(column = src))
      case _                       => ob
    }

  // ── SELECT DISTINCT ────────────────────────────────────────────────────────────────────────────

  /** Project, then de-duplicate the projected rows (buffering, like ORDER BY). ORDER BY/LIMIT apply
    * to the distinct rows; ORDER BY references the projected (output) column name.
    */
  private def distinctResult(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select): QueryResult = {
    val cols = projectionColumns(info, sel.projection)
    val plan = plainProjection(info, sel.projection)
    val rows =
      filterWhere(rawStream(lmdb, info), sel.where).map(r => projectRow(r, plan)).runCollect.map { chunk =>
        val distinct = chunk.toList.distinct
        val ordered  = sel.orderBy.fold(distinct)(ob => sortJRows(distinct, ob))
        sel.limit.fold(ordered)(n => ordered.take(n.toInt))
      }
    QueryResult(cols, ZStream.fromIterableZIO(rows))
  }

  // ── aggregates / GROUP BY ──────────────────────────────────────────────────────────────────────

  /** Aggregate query: fold the WHERE-filtered rows into one accumulator set per group key (so memory
    * scales with the number of groups, not rows), then emit one row per group. With no GROUP BY there
    * is a single, always-present group — `COUNT(*)` of an empty table is `0`, other aggregates `NULL`.
    */
  private def aggregateResult(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select): IO[SqlError, QueryResult] =
    for {
      items <- ZIO.fromEither(aggregateItems(sel.projection))
      _     <- ZIO.fromEither(validateGrouping(items, sel.groupBy))
      _     <- ZIO.fromEither(validateHaving(sel.having, sel.groupBy))
      _     <- ZIO.fromEither(validateOrderBy(items, sel.orderBy))
      cols   = items.map(outputColumn(info, _))
    } yield QueryResult(cols, ZStream.fromIterableZIO(computeGroups(lmdb, info, sel, items)))

  private def aggregateItems(proj: Projection): Either[SqlError, List[SelectItem]] =
    proj match {
      case Projection.Items(items) => Right(items)
      case Projection.Star         => Left(SqlError.Unsupported("SELECT * cannot be combined with GROUP BY or aggregate functions"))
    }

  /** Every non-aggregated column must be part of the GROUP BY (and, with no GROUP BY, no plain
    * columns may sit next to aggregates).
    */
  private def validateGrouping(items: List[SelectItem], groupBy: List[String]): Either[SqlError, Unit] =
    items
      .collectFirst { case SelectItem.Col(n, _) if !groupBy.contains(n) => SqlError.Unsupported(s"column '$n' must appear in GROUP BY or be used in an aggregate function") }
      .toLeft(())

  /** A bare column in HAVING (one not inside an aggregate) must be a grouping column. */
  private def validateHaving(having: Option[Expr], groupBy: List[String]): Either[SqlError, Unit] =
    having.toList.flatMap(freeColsInExpr).find(c => !groupBy.contains(c)) match {
      case Some(c) => Left(SqlError.Unsupported(s"column '$c' in HAVING must appear in GROUP BY or be used in an aggregate function"))
      case None    => Right(())
    }

  /** In an aggregate query ORDER BY can only target a projected column (group columns are
    * identifiers; aggregate output names like `sum(x)` are not, so they can't be written anyway).
    */
  private def validateOrderBy(items: List[SelectItem], orderBy: Option[OrderBy]): Either[SqlError, Unit] =
    orderBy match {
      case Some(ob) if !items.map(outputName).contains(ob.column) =>
        Left(SqlError.Unsupported(s"ORDER BY '${ob.column}' must be one of the selected columns: ${items.map(outputName).mkString(", ")}"))
      case _ => Right(())
    }

  private def computeGroups(lmdb: LMDB, info: CollectionInfo, sel: Statement.Select, items: List[SelectItem]): IO[SqlError, List[JValue]] = {
    // Every aggregate to compute per group: those projected, plus those referenced only by HAVING.
    val projAggs   = items.collect { case SelectItem.Agg(f, c, _) => (f, c) }
    val havingAggs = sel.having.toList.flatMap(aggsInExpr)
    val aggKeys    = (projAggs ++ havingAggs).distinct
    val freshAccs  = aggKeys.map { case (f, _) => initAcc(f) }.toVector
    val seed: Map[List[JValue], Vector[Acc]] =
      if (sel.groupBy.isEmpty) Map(Nil -> freshAccs) else Map.empty
    filterWhere(rawStream(lmdb, info), sel.where)
      .runFold(seed) { (groups, row) =>
        val key     = sel.groupBy.map(c => lookup(row)(c))
        val current = groups.getOrElse(key, freshAccs)
        val updated = current.zip(aggKeys).map { case (acc, (_, c)) => acc.add(aggInput(c, row)) }
        groups.updated(key, updated)
      }
      .map { groups =>
        val kept = groups.toList.flatMap { case (key, accs) =>
          val results: Map[(AggFunc, Option[String]), JValue] = aggKeys.zip(accs).map { case (k, acc) => k -> acc.result }.toMap
          val groupLk: String => JValue = name => { val i = sel.groupBy.indexOf(name); if (i >= 0) key(i) else NullV }
          val passes = sel.having.forall(h => evalBool(h, groupLk, k => results.getOrElse(k, NullV)))
          if (passes) Some(groupRow(sel.groupBy, items, key, results)) else None
        }
        val deduped = if (sel.distinct) kept.distinct else kept
        val ordered = sel.orderBy match {
          case Some(ob)                    => sortJRows(deduped, ob)
          case None if sel.groupBy.isEmpty => deduped
          case None                        => deduped.sortWith((a, b) => compareByColumns(a, b, sel.groupBy) < 0)
        }
        sel.limit.fold(ordered)(n => ordered.take(n.toInt))
      }
  }

  private def groupRow(groupBy: List[String], items: List[SelectItem], key: List[JValue], results: Map[(AggFunc, Option[String]), JValue]): JValue = {
    val fields = items.map {
      case c @ SelectItem.Col(n, _)   => outputName(c) -> key(groupBy.indexOf(n))
      case a @ SelectItem.Agg(f, c, _) => outputName(a) -> results.getOrElse((f, c), NullV)
    }
    MapV(ListMap.from(fields))
  }

  /** The output column name: the `AS` alias when given, otherwise the column name or `func(arg)`. */
  private def outputName(item: SelectItem): String =
    item.alias.getOrElse {
      item match {
        case SelectItem.Col(n, _)          => n
        case SelectItem.Agg(f, None, _)    => s"${aggLabel(f)}(*)"
        case SelectItem.Agg(f, Some(c), _) => s"${aggLabel(f)}($c)"
      }
    }

  private def aggLabel(f: AggFunc): String =
    f match { case AggFunc.Count => "count"; case AggFunc.Sum => "sum"; case AggFunc.Avg => "avg"; case AggFunc.Min => "min"; case AggFunc.Max => "max" }

  private def outputColumn(info: CollectionInfo, item: SelectItem): Column =
    item match {
      case SelectItem.Col(n, _)        => Column(outputName(item), hintFor(info, n))
      case SelectItem.Agg(f, col, _) =>
        val tpe = f match {
          case AggFunc.Count             => "integer"
          case AggFunc.Sum | AggFunc.Avg => "number"
          case AggFunc.Min | AggFunc.Max => col.map(hintFor(info, _)).getOrElse("any")
        }
        Column(outputName(item), tpe)
    }

  /** The value fed to an aggregate from a row: the column, or a non-null tally for `COUNT(*)`. */
  private def aggInput(column: Option[String], row: RawRow): JValue =
    column match {
      case None    => BoolV(true) // COUNT(*) — always a non-null tally
      case Some(c) => lookup(row)(c)
    }

  // ── aggregate accumulators (immutable; folded over the stream) ──────────────────────────────────

  private sealed trait Acc { def add(v: JValue): Acc; def result: JValue }
  private final case class CountAcc(n: Long) extends Acc {
    def add(v: JValue): Acc = if (v != NullV) CountAcc(n + 1) else this
    def result: JValue      = LongV(n)
  }
  private final case class SumAcc(sum: BigDecimal, seen: Boolean) extends Acc {
    def add(v: JValue): Acc = asBigDecimal(v).fold(this: Acc)(d => SumAcc(sum + d, true))
    def result: JValue      = if (seen) DecimalV(sum) else NullV
  }
  private final case class AvgAcc(sum: BigDecimal, n: Long) extends Acc {
    def add(v: JValue): Acc = asBigDecimal(v).fold(this: Acc)(d => AvgAcc(sum + d, n + 1))
    def result: JValue      = if (n > 0) DecimalV(sum / BigDecimal(n)) else NullV
  }
  private final case class MinAcc(cur: Option[JValue]) extends Acc {
    def add(v: JValue): Acc = if (v == NullV) this else MinAcc(Some(cur.fold(v)(c => if (cmpTotal(v, c) < 0) v else c)))
    def result: JValue      = cur.getOrElse(NullV)
  }
  private final case class MaxAcc(cur: Option[JValue]) extends Acc {
    def add(v: JValue): Acc = if (v == NullV) this else MaxAcc(Some(cur.fold(v)(c => if (cmpTotal(v, c) > 0) v else c)))
    def result: JValue      = cur.getOrElse(NullV)
  }

  private def initAcc(func: AggFunc): Acc =
    func match {
      case AggFunc.Count => CountAcc(0)
      case AggFunc.Sum   => SumAcc(BigDecimal(0), false)
      case AggFunc.Avg   => AvgAcc(BigDecimal(0), 0)
      case AggFunc.Min   => MinAcc(None)
      case AggFunc.Max   => MaxAcc(None)
    }

  private def projectionColumns(info: CollectionInfo, proj: Projection): List[Column] =
    proj match {
      case Projection.Star =>
        val valueCols =
          if (info.columns.nonEmpty) info.columns.map(c => Column(c.name, c.typeHint))
          else List(Column("_value", "any"))
        Column("_key", info.keyId.getOrElse("key")) :: valueCols
      case Projection.Items(items) => items.map(outputColumn(info, _))
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

  /** Build an output row from `(outputName, sourceColumn)` pairs, reading each source via `lookup`. */
  private def projectRow(r: RawRow, plan: List[(String, String)]): JValue =
    MapV(ListMap.from(plan.map { case (out, src) => out -> lookup(r)(src) }))

  private def sortRows(rows: List[RawRow], ob: OrderBy): List[RawRow] =
    rows.sortWith { (a, b) =>
      val c = cmpTotal(lookup(a)(ob.column), lookup(b)(ob.column))
      if (ob.descending) c > 0 else c < 0
    }

  /** Read a named field from an already-projected `MapV` row (NULL if absent). */
  private def fieldOf(row: JValue, name: String): JValue =
    row match { case MapV(m) => m.getOrElse(name, NullV); case _ => NullV }

  /** Order projected (`MapV`) rows by one of their columns — used by DISTINCT and aggregate queries. */
  private def sortJRows(rows: List[JValue], ob: OrderBy): List[JValue] =
    rows.sortWith { (a, b) =>
      val c = cmpTotal(fieldOf(a, ob.column), fieldOf(b, ob.column))
      if (ob.descending) c > 0 else c < 0
    }

  /** Lexicographic comparison of projected rows over several columns (default GROUP BY ordering). */
  private def compareByColumns(a: JValue, b: JValue, cols: List[String]): Int =
    cols.iterator.map(c => cmpTotal(fieldOf(a, c), fieldOf(b, c))).find(_ != 0).getOrElse(0)

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

  /** Identifies an aggregate: its function and optional column (`None` = `COUNT(*)`). */
  private type AggKey = (AggFunc, Option[String])

  // `lk` resolves a column/group name; `agg` resolves an aggregate (only populated for HAVING).
  private def operand(e: Expr, lk: String => JValue, agg: AggKey => JValue = _ => NullV): JValue =
    e match {
      case Expr.Col(n)          => lk(n)
      case Expr.Lit(l)          => litToJV(l)
      case Expr.Func(name, as)  => evalFunc(name, as.map(operand(_, lk, agg)))
      case Expr.Aggregate(f, c) => agg((f, c))
      case other                => BoolV(evalBool(other, lk, agg))
    }

  private def evalBool(e: Expr, lk: String => JValue, agg: AggKey => JValue = _ => NullV): Boolean =
    e match {
      case Expr.And(l, r)      => evalBool(l, lk, agg) && evalBool(r, lk, agg)
      case Expr.Or(l, r)       => evalBool(l, lk, agg) || evalBool(r, lk, agg)
      case Expr.Not(x)         => !evalBool(x, lk, agg)
      case Expr.Cmp(op, l, r)  => compareOp(op, operand(l, lk, agg), operand(r, lk, agg))
      case Expr.IsNull(t, neg) => val isN = operand(t, lk, agg) == NullV; if (neg) !isN else isN
      case Expr.Like(t, pat)   => operand(t, lk, agg) match { case StringV(s) => likeMatch(s, pat); case _ => false }
      case other               => operand(other, lk, agg) match { case BoolV(b) => b; case _ => false }
    }

  /** Scalar functions. Currently LENGTH(x): the character length of x as text (NULL stays NULL). */
  private def evalFunc(name: String, args: List[JValue]): JValue =
    (name, args) match {
      case ("length", List(NullV))      => NullV
      case ("length", List(StringV(s))) => LongV(s.length.toLong)
      case ("length", List(v))          => LongV(jvToString(v).length.toLong)
      case _                            => NullV
    }

  private def jvToString(jv: JValue): String =
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

  /** All aggregate references in an expression (used to gather HAVING aggregates and to reject
    * aggregates in WHERE). */
  private def aggsInExpr(e: Expr): List[AggKey] =
    e match {
      case Expr.Aggregate(f, c)      => List((f, c))
      case Expr.Func(_, args)        => args.flatMap(aggsInExpr)
      case Expr.Cmp(_, l, r)         => aggsInExpr(l) ++ aggsInExpr(r)
      case Expr.And(l, r)            => aggsInExpr(l) ++ aggsInExpr(r)
      case Expr.Or(l, r)             => aggsInExpr(l) ++ aggsInExpr(r)
      case Expr.Not(x)               => aggsInExpr(x)
      case Expr.Like(t, _)           => aggsInExpr(t)
      case Expr.IsNull(t, _)         => aggsInExpr(t)
      case Expr.Col(_) | Expr.Lit(_) => Nil
    }

  /** Columns referenced outside any aggregate (these must be grouping columns in HAVING). */
  private def freeColsInExpr(e: Expr): List[String] =
    e match {
      case Expr.Col(n)          => List(n)
      case Expr.Aggregate(_, _) => Nil
      case Expr.Func(_, args)   => args.flatMap(freeColsInExpr)
      case Expr.Cmp(_, l, r)    => freeColsInExpr(l) ++ freeColsInExpr(r)
      case Expr.And(l, r)       => freeColsInExpr(l) ++ freeColsInExpr(r)
      case Expr.Or(l, r)        => freeColsInExpr(l) ++ freeColsInExpr(r)
      case Expr.Not(x)          => freeColsInExpr(x)
      case Expr.Like(t, _)      => freeColsInExpr(t)
      case Expr.IsNull(t, _)    => freeColsInExpr(t)
      case Expr.Lit(_)          => Nil
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
