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
import zio.lmdb.keycodecs.geo.GEOTools
import zio.lmdb.schema.SchemaArtifact
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
      case s: Statement.Select   => selectResult(lmdb, s)
      case i: Statement.Insert   => Catalog.lookup(lmdb, i.into).flatMap(insertResult(lmdb, _, i))
      case u: Statement.Update   => Catalog.lookup(lmdb, u.table).flatMap(updateResult(lmdb, _, u))
      case d: Statement.Delete   => Catalog.lookup(lmdb, d.from).flatMap(deleteResult(lmdb, _, d))
      case d: Statement.Describe => Catalog.lookup(lmdb, d.collection).map(describeResult)
      case sh: Statement.Show    => showResult(lmdb, sh.target)
    }

  // ── SELECT ─────────────────────────────────────────────────────────────────────────────────

  /** A read row, looked up by (possibly `alias.`-qualified) column name. This abstraction unifies
    * single-table and join rows, so WHERE / ORDER BY / GROUP BY / projection share one pipeline. */
  private type Row    = String => JValue
  private type Source = (String, CollectionInfo)

  /** Three shapes: an aggregate/`GROUP BY` query (buffers per-group state), a `DISTINCT` query
    * (buffers to de-duplicate), or a plain streaming `SELECT` (the only one that never buffers; a
    * JOIN buffers regardless).
    */
  private def selectResult(lmdb: LMDB, sel0: Statement.Select): IO[SqlError, QueryResult] = {
    val sel = resolveSelectAliases(sel0)
    for {
      _       <- ZIO.fromEither(validateNoAggregatesInWhere(sel.where))
      sources <- resolveSources(lmdb, sel)
      result  <-
        if (isAggregate(sel)) aggregateResult(lmdb, sources, sel)
        else if (sel.distinct) ZIO.succeed(distinctResult(lmdb, sources, sel))
        else ZIO.succeed(streamingResult(lmdb, sources, sel))
    } yield result
  }

  /** The row sources in order: FROM first, then each JOIN, paired with the alias used to qualify its
    * columns (the explicit alias, else the collection name). */
  private def resolveSources(lmdb: LMDB, sel: Statement.Select): IO[SqlError, List[Source]] =
    Catalog.lookup(lmdb, sel.from).flatMap { fromInfo =>
      ZIO
        .foreach(sel.joins)(j => Catalog.lookup(lmdb, j.table.collection).map(info => (j.table.alias.getOrElse(j.table.collection), info)))
        .map(joinSources => (sel.fromAlias.getOrElse(sel.from), fromInfo) :: joinSources)
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

  /** Make `AS` aliases referenceable in `WHERE`, `HAVING`, `GROUP BY`, and `ORDER BY` — a friendly
    * extension to standard SQL, which only exposes them in `ORDER BY`. Each explicit alias is
    * substituted by the expression it names, so `… geo_distance(...) AS dist … WHERE dist <= n` reuses
    * the projected expression, and `SELECT year(ts) AS y … GROUP BY y` groups by `year(ts)`. An alias
    * that resolves to an aggregate is then handled by the usual validation (e.g. an aggregate alias in
    * `WHERE` is still rejected). */
  private def resolveSelectAliases(sel: Statement.Select): Statement.Select = {
    val aliases = aliasBindings(sel.projection)
    if (aliases.isEmpty) sel
    else
      sel.copy(
        where   = sel.where.map(substituteAliases(_, aliases)),
        having  = sel.having.map(substituteAliases(_, aliases)),
        groupBy = sel.groupBy.map(substituteAliases(_, aliases)),
        orderBy = sel.orderBy.map(ob => ob.copy(expr = substituteAliases(ob.expr, aliases)))
      )
  }

  /** Explicit `AS` aliases of a projection, each mapped to the expression it names. */
  private def aliasBindings(proj: Projection): Map[String, Expr] =
    proj match {
      case Projection.Star         => Map.empty
      case Projection.Items(items) =>
        items.collect {
          case SelectItem.Col(n, Some(a))    => a -> (Expr.Col(n): Expr)
          case SelectItem.Expr(e, Some(a))   => a -> e
          case SelectItem.Agg(f, c, Some(a)) => a -> (Expr.Aggregate(f, c): Expr)
        }.toMap
    }

  /** Replace each `Expr.Col(alias)` with the expression the alias names (one level only — the
    * replacement is a SELECT expression and cannot itself reference an alias). */
  private def substituteAliases(e: Expr, aliases: Map[String, Expr]): Expr =
    e match {
      case Expr.Col(n)          => aliases.getOrElse(n, e)
      case Expr.Lit(_)          => e
      case Expr.Aggregate(_, _) => e
      case Expr.Arith(op, l, r) => Expr.Arith(op, substituteAliases(l, aliases), substituteAliases(r, aliases))
      case Expr.Cmp(op, l, r)   => Expr.Cmp(op, substituteAliases(l, aliases), substituteAliases(r, aliases))
      case Expr.And(l, r)       => Expr.And(substituteAliases(l, aliases), substituteAliases(r, aliases))
      case Expr.Or(l, r)        => Expr.Or(substituteAliases(l, aliases), substituteAliases(r, aliases))
      case Expr.Not(x)          => Expr.Not(substituteAliases(x, aliases))
      case Expr.Like(t, p)      => Expr.Like(substituteAliases(t, aliases), p)
      case Expr.IsNull(t, neg)  => Expr.IsNull(substituteAliases(t, aliases), neg)
      case Expr.Func(n, as)     => Expr.Func(n, as.map(substituteAliases(_, aliases)))
    }

  // ── plain streaming SELECT ─────────────────────────────────────────────────────────────────────

  private def streamingResult(lmdb: LMDB, sources: List[Source], sel: Statement.Select): QueryResult = {
    val cols = projectionColumns(sources, sel.projection)
    val plan = plainProjection(sources, sel.projection)

    val filtered = filterRows(rowSource(lmdb, sources, sel), sel.where)
    val ordered: ZStream[Any, SqlError, Row] =
      if (sel.orderBy.isEmpty) filtered
      else {
        val keys = sel.orderBy.map(ob => (resolveOrderExpr(plan, ob.expr), ob.descending))
        ZStream.fromIterableZIO(filtered.runCollect.map(c => sortRowsByKeys(c.toList, keys)))
      }
    val limited   = sel.limit.fold(ordered)(n => ordered.take(n))
    val projected = limited.map(row => projectRow(row, plan))
    QueryResult(cols, projected)
  }

  /** Output-name → source-expression pairs for a non-aggregate projection (resolves `AS` aliases and
    * `*`). A plain column is an `Expr.Col`; a function projection (e.g. `GEO_DISTANCE(...)`) keeps its
    * expression so it is evaluated per row. */
  private def plainProjection(sources: List[Source], proj: Projection): List[(String, Expr)] =
    proj match {
      case Projection.Star         => projectionColumns(sources, proj).map(c => c.name -> (Expr.Col(c.name): Expr))
      case Projection.Items(items) =>
        items.collect {
          case SelectItem.Col(n, al)  => al.getOrElse(unqualify(n)) -> (Expr.Col(n): Expr)
          case SelectItem.Expr(e, al) => al.getOrElse(exprLabel(e)) -> e
        }
    }

  /** If ORDER BY names a projected output (an `AS` alias or column), sort by that item's source
    * expression; otherwise sort by the ORDER BY expression as written (e.g. a `GEO_DISTANCE(...)`). */
  private def resolveOrderExpr(plan: List[(String, Expr)], e: Expr): Expr =
    e match {
      case Expr.Col(name) => plan.collectFirst { case (out, src) if out == name => src }.getOrElse(e)
      case _              => e
    }

  private def filterRows(rows: ZStream[Any, SqlError, Row], where: Option[Expr]): ZStream[Any, SqlError, Row] =
    where.fold(rows)(w => rows.filter(row => evalBool(w, row)))

  /** Multi-key sort of read rows: compare on each ORDER BY expression in turn, honouring its
    * direction, until one breaks the tie. */
  private def sortRowsByKeys(rows: List[Row], keys: List[(Expr, Boolean)]): List[Row] =
    rows.sortWith { (a, b) =>
      keys.iterator.map { case (e, desc) => val c = cmpTotal(operand(e, a), operand(e, b)); if (desc) -c else c }.find(_ != 0).getOrElse(0) < 0
    }

  private def projectRow(row: Row, plan: List[(String, Expr)]): JValue =
    MapV(ListMap.from(plan.map { case (out, e) => out -> operand(e, row) }))

  // ── SELECT DISTINCT ────────────────────────────────────────────────────────────────────────────

  /** Project, then de-duplicate the projected rows (buffering, like ORDER BY). ORDER BY/LIMIT apply
    * to the distinct rows; ORDER BY references the projected (output) column name.
    */
  private def distinctResult(lmdb: LMDB, sources: List[Source], sel: Statement.Select): QueryResult = {
    val cols = projectionColumns(sources, sel.projection)
    val plan = plainProjection(sources, sel.projection)
    val rows =
      filterRows(rowSource(lmdb, sources, sel), sel.where).map(row => projectRow(row, plan)).runCollect.map { chunk =>
        val distinct = chunk.toList.distinct
        val ordered  =
          if (sel.orderBy.isEmpty) distinct
          else sortJRowsByKeys(distinct, sel.orderBy.map(ob => (resolveOrderOutput(plan, ob.expr), ob.descending)))
        sel.limit.fold(ordered)(n => ordered.take(n.toInt))
      }
    QueryResult(cols, ZStream.fromIterableZIO(rows))
  }

  /** The projected output-column name an ORDER BY expression refers to (for DISTINCT/aggregate, which
    * sort already-projected rows): a column matching an output alias/name, or a function expression
    * equal to a projected item's source expression; falls back to the expression's own label. */
  private def resolveOrderOutput(plan: List[(String, Expr)], e: Expr): String =
    plan.collectFirst { case (out, src) if src == e || Expr.Col(out) == e => out }
      .getOrElse(e match { case Expr.Col(n) => unqualify(n); case other => exprLabel(other) })

  // ── row sources & JOIN ─────────────────────────────────────────────────────────────────────────

  private def rowSource(lmdb: LMDB, sources: List[Source], sel: Statement.Select): ZStream[Any, SqlError, Row] =
    if (sel.joins.isEmpty) {
      val (alias, info) = sources.head
      rawStream(lmdb, info).map(r => combinedLookup(List((alias, Some(r)))))
    } else
      ZStream.fromIterableZIO(executeJoins(lmdb, sources, sel.joins)).map(parts => combinedLookup(parts))

  /** Resolve a (possibly `alias.`-qualified, possibly nested) column against a combined row. A
    * leading segment that names a known source is the table alias; the remaining segments form a
    * path that may descend into nested object fields (e.g. `o.location.altitude`). An unqualified
    * path is taken from the first source that has a non-null value for it; a `None` part (an
    * unmatched LEFT join side) yields NULL for all of its columns. */
  private def combinedLookup(parts: List[(String, Option[RawRow])])(name: String): JValue = {
    val segments = splitPath(name)
    segments match {
      case alias :: rest if rest.nonEmpty && parts.exists(_._1 == alias) =>
        parts.collectFirst { case (a, ro) if a == alias => ro.fold(NullV: JValue)(r => lookupPath(r, rest)) }.getOrElse(NullV)
      case _ =>
        parts.iterator.map { case (_, ro) => ro.fold(NullV: JValue)(r => lookupPath(r, segments)) }.find(_ != NullV).getOrElse(NullV)
    }
  }

  /** Execute the JOINs left-to-right, one hash join per step (buffers both sides). Each result is a
    * combined row: the per-alias `RawRow`s, with `None` where a LEFT join found no match. */
  private def executeJoins(lmdb: LMDB, sources: List[Source], joins: List[Join]): IO[SqlError, List[List[(String, Option[RawRow])]]] = {
    val (fromAlias, fromInfo) = sources.head
    val start: IO[SqlError, List[List[(String, Option[RawRow])]]] =
      rawStream(lmdb, fromInfo).map(r => List((fromAlias, Some(r): Option[RawRow]))).runCollect.map(_.toList)
    joins.zip(sources.tail).foldLeft(start) { case (leftZ, (join, (rightAlias, rightInfo))) =>
      leftZ.flatMap(leftRows => rawStream(lmdb, rightInfo).runCollect.map(rs => hashJoin(sources, leftRows, rightAlias, rs.toList, join)))
    }
  }

  private def hashJoin(
    sources: List[Source],
    leftRows: List[List[(String, Option[RawRow])]],
    rightAlias: String,
    rightRows: List[RawRow],
    join: Join
  ): List[List[(String, Option[RawRow])]] = {
    val (pairs, residual) = joinKeys(join.on, rightAlias)
    // Coercion per equi-pair: if either side is a `_key`, normalize both sides to that key's type;
    // otherwise compare value fields structurally (their types must already match).
    val coercions = pairs.map { case (probeCol, buildCol) => colKeyId(sources, buildCol).orElse(colKeyId(sources, probeCol)) }

    val index: Map[List[JValue], List[RawRow]] =
      rightRows.foldLeft(Map.empty[List[JValue], List[RawRow]]) { (m, r) =>
        val rl = combinedLookup(List((rightAlias, Some(r))))
        val k  = pairs.zip(coercions).map { case ((_, buildCol), c) => normalizeJoinVal(rl(buildCol), c) }
        if (k.contains(NullV)) m else m.updated(k, r :: m.getOrElse(k, Nil)) // NULL never joins
      }

    leftRows.flatMap { left =>
      val leftRow  = combinedLookup(left)
      val probeKey = pairs.zip(coercions).map { case ((probeCol, _), c) => normalizeJoinVal(leftRow(probeCol), c) }
      val matched  =
        if (probeKey.contains(NullV)) Nil
        else index.getOrElse(probeKey, Nil).reverse.map(r => left :+ (rightAlias, Some(r): Option[RawRow]))
      val passing  = matched.filter(row => residual.forall(rp => evalBool(rp, combinedLookup(row))))
      join.joinType match {
        case JoinType.Inner => passing
        case JoinType.Left  => if (passing.nonEmpty) passing else List(left :+ (rightAlias, None: Option[RawRow]))
      }
    }
  }

  /** Split an ON condition into equi-join pairs `(probeColumn on the left, buildColumn on the right)`
    * and a residual predicate (anything that is not a simple `left = right` equality). */
  private def joinKeys(on: Expr, rightAlias: String): (List[(String, String)], List[Expr]) = {
    def conjuncts(e: Expr): List[Expr] = e match { case Expr.And(l, r) => conjuncts(l) ++ conjuncts(r); case _ => List(e) }
    conjuncts(on).foldLeft((List.empty[(String, String)], List.empty[Expr])) {
      case ((ps, rs), c @ Expr.Cmp(CmpOp.Eq, Expr.Col(a), Expr.Col(b))) =>
        (refersTo(a, rightAlias), refersTo(b, rightAlias)) match {
          case (true, false) => (ps :+ (b, a), rs)
          case (false, true) => (ps :+ (a, b), rs)
          case _             => (ps, rs :+ c)
        }
      case ((ps, rs), other) => (ps, rs :+ other)
    }
  }

  private def refersTo(col: String, alias: String): Boolean = { val d = col.indexOf('.'); d >= 0 && col.substring(0, d) == alias }

  /** The key id if `col` is an `alias._key` of a known source, else `None` (a value field). */
  private def colKeyId(sources: List[Source], col: String): Option[String] = {
    val dot = col.indexOf('.')
    if (dot < 0 || col.substring(dot + 1) != "_key") None
    else sources.collectFirst { case (a, info) if a == col.substring(0, dot) => info }.flatMap(_.keyId)
  }

  private def normalizeJoinVal(jv: JValue, coercion: Option[String]): JValue = coercion.fold(jv)(keyId => coerceToKey(jv, keyId))

  /** Convert a value field to the JValue type a key of `keyId` decodes to, so a value column can be
    * joined against a `_key`. NULL stays NULL; a value that cannot be converted becomes NULL (no match). */
  private def coerceToKey(jv: JValue, keyId: String): JValue =
    if (jv == NullV) NullV
    else keyId match {
      case "lmdb:str" | "lmdb-ulid:v1" | "lmdb:bytes" => StringV(jvToString(jv))
      case "lmdb:int64" | "lmdb:int32" | "lmdb:int16" => coerceToLong(jv)
      case "lmdb:uuid" | "lmdb-uuidv7:v1"             => coerceToUuid(jv)
      case "lmdb-ts:instant/v1"                       => coerceToInstant(jv)
      case _                                          => jv
    }

  private def coerceToLong(jv: JValue): JValue =
    jv match {
      case LongV(_)    => jv
      case DoubleV(d)  => LongV(d.toLong)
      case DecimalV(d) => LongV(d.toLong)
      case StringV(s)  => s.toLongOption.fold(NullV: JValue)(LongV(_))
      case _           => NullV
    }

  private def coerceToUuid(jv: JValue): JValue =
    jv match {
      case IdentifierV(_) => jv
      case StringV(s)     => scala.util.Try(java.util.UUID.fromString(s)).toOption.fold(NullV: JValue)(IdentifierV(_))
      case _              => NullV
    }

  private def coerceToInstant(jv: JValue): JValue =
    jv match {
      case InstantV(_) => jv
      case StringV(s)  => scala.util.Try(java.time.Instant.parse(s)).toOption.fold(NullV: JValue)(InstantV(_))
      case _           => NullV
    }

  // ── aggregates / GROUP BY ──────────────────────────────────────────────────────────────────────

  /** Aggregate query: fold the WHERE-filtered rows into one accumulator set per group key (so memory
    * scales with the number of groups, not rows), then emit one row per group. With no GROUP BY there
    * is a single, always-present group — `COUNT(*)` of an empty table is `0`, other aggregates `NULL`.
    */
  private def aggregateResult(lmdb: LMDB, sources: List[Source], sel: Statement.Select): IO[SqlError, QueryResult] =
    for {
      items   <- ZIO.fromEither(aggregateItems(sel.projection))
      bindings = groupBindings(sel.groupBy)
      _       <- ZIO.fromEither(validateGrouping(items, bindings))
      _       <- ZIO.fromEither(validateHaving(sel.having, bindings))
      _       <- ZIO.fromEither(validateOrderBy(sel.orderBy, bindings))
      cols     = items.map(outputColumn(sources, _))
    } yield QueryResult(cols, ZStream.fromIterableZIO(computeGroups(lmdb, sources, sel, items, bindings)))

  private def aggregateItems(proj: Projection): Either[SqlError, List[SelectItem]] =
    proj match {
      case Projection.Items(items) => Right(items)
      case Projection.Star         => Left(SqlError.Unsupported("SELECT * cannot be combined with GROUP BY or aggregate functions"))
    }

  /** A synthetic, un-typeable column name standing for the i-th GROUP BY expression's value (the leading
    * space cannot appear in a parsed identifier, so it never collides with a real column). */
  private val GroupBindPrefix = " g"

  /** Pair each GROUP BY expression with its synthetic binding name. */
  private def groupBindings(groupBy: List[Expr]): List[(Expr, String)] =
    groupBy.zipWithIndex.map { case (e, i) => (e, s"$GroupBindPrefix$i") }

  /** Rewrite an expression for evaluation in the grouped context: every occurrence of a whole GROUP BY
    * expression is replaced by a reference to its precomputed group value, so `year(ts)` in SELECT /
    * HAVING / ORDER BY reads the group key instead of re-evaluating against a (now absent) row.
    * Aggregates are left intact (resolved against the per-group accumulator results). */
  private def bindGroups(e: Expr, bindings: List[(Expr, String)]): Expr =
    bindings.collectFirst { case (g, name) if g == e => Expr.Col(name): Expr }.getOrElse {
      e match {
        case Expr.Arith(op, l, r) => Expr.Arith(op, bindGroups(l, bindings), bindGroups(r, bindings))
        case Expr.Cmp(op, l, r)   => Expr.Cmp(op, bindGroups(l, bindings), bindGroups(r, bindings))
        case Expr.And(l, r)       => Expr.And(bindGroups(l, bindings), bindGroups(r, bindings))
        case Expr.Or(l, r)        => Expr.Or(bindGroups(l, bindings), bindGroups(r, bindings))
        case Expr.Not(x)          => Expr.Not(bindGroups(x, bindings))
        case Expr.Like(t, p)      => Expr.Like(bindGroups(t, bindings), p)
        case Expr.IsNull(t, neg)  => Expr.IsNull(bindGroups(t, bindings), neg)
        case Expr.Func(n, as)     => Expr.Func(n, as.map(bindGroups(_, bindings)))
        case leaf                 => leaf // Col, Lit, Aggregate
      }
    }

  /** The source expression of a select item (a plain column, a function/arithmetic expression, or an
    * aggregate reference) — the common shape that grouping, projection, and ordering all evaluate. */
  private def itemExpr(item: SelectItem): Expr =
    item match {
      case SelectItem.Col(n, _)    => Expr.Col(n)
      case SelectItem.Expr(e, _)   => e
      case SelectItem.Agg(f, c, _) => Expr.Aggregate(f, c)
    }

  /** Columns still referenced (outside any aggregate) after binding GROUP BY expressions — i.e.
    * ungrouped, non-aggregated columns, which make a projection / HAVING / ORDER BY invalid. */
  private def ungroupedCols(e: Expr, bindings: List[(Expr, String)]): List[String] =
    freeColsInExpr(bindGroups(e, bindings)).filterNot(_.startsWith(GroupBindPrefix))

  /** Every projected column or expression must be either an aggregate or fully covered by the GROUP BY
    * (so no ungrouped, non-aggregated columns remain after binding). */
  private def validateGrouping(items: List[SelectItem], bindings: List[(Expr, String)]): Either[SqlError, Unit] =
    items.flatMap(it => ungroupedCols(itemExpr(it), bindings)).headOption match {
      case Some(c) => Left(SqlError.Unsupported(s"column '$c' must appear in GROUP BY or be used in an aggregate function"))
      case None    => Right(())
    }

  /** A bare column in HAVING (one not inside an aggregate) must be a grouping column / expression. */
  private def validateHaving(having: Option[Expr], bindings: List[(Expr, String)]): Either[SqlError, Unit] =
    having.toList.flatMap(ungroupedCols(_, bindings)).headOption match {
      case Some(c) => Left(SqlError.Unsupported(s"column '$c' in HAVING must appear in GROUP BY or be used in an aggregate function"))
      case None    => Right(())
    }

  /** In an aggregate query, every ORDER BY key must be evaluable per group — a grouping expression or
    * an aggregate — so no ungrouped column may remain after binding. */
  private def validateOrderBy(orderBy: List[OrderBy], bindings: List[(Expr, String)]): Either[SqlError, Unit] =
    orderBy.flatMap(ob => ungroupedCols(ob.expr, bindings)).headOption match {
      case Some(c) => Left(SqlError.Unsupported(s"ORDER BY column '$c' must appear in GROUP BY or be used in an aggregate function"))
      case None    => Right(())
    }

  private def computeGroups(lmdb: LMDB, sources: List[Source], sel: Statement.Select, items: List[SelectItem], bindings: List[(Expr, String)]): IO[SqlError, List[JValue]] = {
    // Every aggregate to compute per group: those in the projection, HAVING, and ORDER BY.
    val aggKeys   =
      (items.flatMap(it => aggsInExpr(itemExpr(it))) ++ sel.having.toList.flatMap(aggsInExpr) ++ sel.orderBy.flatMap(ob => aggsInExpr(ob.expr))).distinct
    val freshAccs = aggKeys.map { case (f, _) => initAcc(f) }.toVector
    val bindNames = bindings.map(_._2)
    val seed: Map[List[JValue], Vector[Acc]] =
      if (sel.groupBy.isEmpty) Map(Nil -> freshAccs) else Map.empty
    filterRows(rowSource(lmdb, sources, sel), sel.where)
      .runFold(seed) { (groups, row) =>
        val key     = sel.groupBy.map(g => operand(g, row))
        val current = groups.getOrElse(key, freshAccs)
        val updated = current.zip(aggKeys).map { case (acc, (_, c)) => acc.add(aggInput(c, row)) }
        groups.updated(key, updated)
      }
      .map { groups =>
        // Per-group evaluation context: synthetic group cols → key values; aggregates → results.
        def groupLk(key: List[JValue]): String => JValue = { val m = bindNames.zip(key).toMap; name => m.getOrElse(name, NullV) }
        def aggLk(results: Map[AggKey, JValue]): AggKey => JValue = k => results.getOrElse(k, NullV)
        def evalIn(e: Expr, key: List[JValue], results: Map[AggKey, JValue]): JValue = operand(bindGroups(e, bindings), groupLk(key), aggLk(results))

        val kept = groups.toList.flatMap { case (key, accs) =>
          val results = aggKeys.zip(accs).map { case (k, acc) => k -> acc.result }.toMap
          val passes  = sel.having.forall(h => evalBool(bindGroups(h, bindings), groupLk(key), aggLk(results)))
          if (passes) Some((key, results)) else None
        }
        val ordered =
          if (sel.orderBy.nonEmpty)
            kept.sortWith { (a, b) =>
              sel.orderBy.iterator.map { ob =>
                val c = cmpTotal(evalIn(ob.expr, a._1, a._2), evalIn(ob.expr, b._1, b._2)); if (ob.descending) -c else c
              }.find(_ != 0).getOrElse(0) < 0
            }
          else if (sel.groupBy.isEmpty) kept
          else kept.sortWith { (a, b) => compareKeys(a._1, b._1) < 0 }
        val rows    = ordered.map { case (key, results) => groupRow(items, bindings, groupLk(key), aggLk(results)) }
        val deduped = if (sel.distinct) rows.distinct else rows
        sel.limit.fold(deduped)(n => deduped.take(n.toInt))
      }
  }

  /** Build one grouped output row: each item's value is its (group-bound) source expression evaluated
    * against the group key and aggregate results. */
  private def groupRow(items: List[SelectItem], bindings: List[(Expr, String)], lk: String => JValue, agg: AggKey => JValue): JValue =
    MapV(ListMap.from(items.map(it => outputName(it) -> operand(bindGroups(itemExpr(it), bindings), lk, agg))))

  /** Lexicographic comparison of two group-key vectors (the default, deterministic group ordering). */
  private def compareKeys(a: List[JValue], b: List[JValue]): Int =
    a.iterator.zip(b.iterator).map { case (x, y) => cmpTotal(x, y) }.find(_ != 0).getOrElse(0)

  /** The output column name: the `AS` alias when given, otherwise the (unqualified) column name or
    * `func(arg)`. A qualifying table alias is dropped, so `o.amount` → `amount`, `SUM(o.amount)` →
    * `sum(amount)`. */
  private def outputName(item: SelectItem): String =
    item.alias.getOrElse {
      item match {
        case SelectItem.Col(n, _)          => unqualify(n)
        case SelectItem.Agg(f, None, _)    => s"${aggLabel(f)}(*)"
        case SelectItem.Agg(f, Some(c), _) => s"${aggLabel(f)}(${unqualify(c)})"
        case SelectItem.Expr(e, _)         => exprLabel(e)
      }
    }

  private def unqualify(name: String): String = { val i = name.lastIndexOf('.'); if (i >= 0) name.substring(i + 1) else name }

  private def aggLabel(f: AggFunc): String =
    f match { case AggFunc.Count => "count"; case AggFunc.Sum => "sum"; case AggFunc.Avg => "avg"; case AggFunc.Min => "min"; case AggFunc.Max => "max" }

  private def outputColumn(sources: List[Source], item: SelectItem): Column =
    item match {
      case SelectItem.Col(n, _)        => Column(outputName(item), hintFor(sources, n))
      case SelectItem.Expr(e, _)       => Column(outputName(item), exprTypeHint(e, sources))
      case SelectItem.Agg(f, col, _) =>
        val tpe = f match {
          case AggFunc.Count             => "integer"
          case AggFunc.Sum | AggFunc.Avg => "number"
          case AggFunc.Min | AggFunc.Max => col.map(hintFor(sources, _)).getOrElse("any")
        }
        Column(outputName(item), tpe)
    }

  /** The value fed to an aggregate from a row: the column, or a non-null tally for `COUNT(*)`. */
  private def aggInput(column: Option[String], row: Row): JValue =
    column match {
      case None    => BoolV(true) // COUNT(*) — always a non-null tally
      case Some(c) => row(c)
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

  private def projectionColumns(sources: List[Source], proj: Projection): List[Column] =
    proj match {
      case Projection.Star =>
        sources match {
          case (_, info) :: Nil => Column("_key", info.keyId.getOrElse("key")) :: valueColumns(info)
          // SELECT * over a JOIN: every column from every source, qualified to avoid collisions.
          case _                => sources.flatMap { case (alias, info) => Column(s"$alias._key", info.keyId.getOrElse("key")) :: valueColumns(info).map(c => Column(s"$alias.${c.name}", c.typeHint)) }
        }
      case Projection.Items(items) => items.map(outputColumn(sources, _))
    }

  private def valueColumns(info: CollectionInfo): List[Column] =
    if (info.columns.nonEmpty) info.columns.map(c => Column(c.name, c.typeHint)) else List(Column("_value", "any"))

  /** Type hint for a (possibly `alias.`-qualified, possibly nested) column across the query's
    * sources. Nested paths are resolved against the value `JsonSchema`; top-level columns use the
    * catalog's flat column list. */
  private def hintFor(sources: List[Source], name: String): String = {
    val segments = splitPath(name)
    segments match {
      case alias :: rest if rest.nonEmpty && sources.exists(_._1 == alias) =>
        hintForPath(sources.collect { case (a, info) if a == alias => info }, rest)
      case _ =>
        hintForPath(sources.map(_._2), segments)
    }
  }

  private def hintForPath(infos: List[CollectionInfo], path: List[String]): String =
    path match {
      case "_key" :: _   => infos.headOption.flatMap(_.keyId).getOrElse("key")
      case "_value" :: _ => "any"
      case List(single)  => infos.flatMap(_.columns).find(_.name == single).map(_.typeHint).getOrElse("any")
      case nested        => infos.iterator.map(info => nestedTypeHint(info, nested)).find(_ != "any").getOrElse("any")
    }

  /** Walk the value `JsonSchema` to find the declared type at a path of nested fields; "any" when
    * the schema is absent or the path is not described. */
  private def nestedTypeHint(info: CollectionInfo, path: List[String]): String =
    info.valueSchema match {
      case Some(SchemaArtifact.JsonSchema(root)) => schemaTypeAt(root, path)
      case _                                     => "any"
    }

  private def schemaTypeAt(schema: JValue, path: List[String]): String =
    path match {
      case Nil         => schema match { case MapV(m) => m.get("type").collect { case StringV(t) => t }.getOrElse("any"); case _ => "any" }
      case seg :: rest =>
        schema match {
          case MapV(m) =>
            m.get("properties") match {
              case Some(MapV(props)) => props.get(seg).map(schemaTypeAt(_, rest)).getOrElse("any")
              case _                 => "any"
            }
          case _ => "any"
        }
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

  private def lookup(r: RawRow)(name: String): JValue = lookupPath(r, splitPath(name))

  /** Split a column reference into its dotted segments. */
  private def splitPath(name: String): List[String] = name.split('.').toList

  /** Resolve a path against a raw row: a leading `_key`/`_value` selects the key or the whole value,
    * then any remaining segments descend into nested object fields; an ordinary path descends into
    * the value tree from the top. */
  private def lookupPath(r: RawRow, segments: List[String]): JValue =
    segments match {
      case "_key" :: rest   => walk(r.key.toJValue, rest)
      case "_value" :: rest => walk(r.value, rest)
      case all              => walk(r.value, all)
    }

  /** Descend into nested `MapV` objects following `segments`; a non-object or a missing field on the
    * way yields NULL. An empty path returns the value unchanged. */
  private def walk(root: JValue, segments: List[String]): JValue =
    segments.foldLeft(root) {
      case (MapV(m), seg) => m.getOrElse(seg, NullV)
      case _              => NullV
    }

  /** Read a named field from an already-projected `MapV` row (NULL if absent). */
  private def fieldOf(row: JValue, name: String): JValue =
    row match { case MapV(m) => m.getOrElse(name, NullV); case _ => NullV }

  /** Order projected (`MapV`) rows by several output columns in turn, each with its own direction —
    * used by DISTINCT (which sorts already-projected rows). */
  private def sortJRowsByKeys(rows: List[JValue], keys: List[(String, Boolean)]): List[JValue] =
    rows.sortWith { (a, b) =>
      keys.iterator.map { case (col, desc) => val c = cmpTotal(fieldOf(a, col), fieldOf(b, col)); if (desc) -c else c }.find(_ != 0).getOrElse(0) < 0
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

  /** Binary arithmetic over numeric values. A non-numeric operand, or division/modulo by zero, yields
    * NULL (so it propagates harmlessly through comparisons). Two integral operands stay integral for
    * `+`/`-`/`*`/`%`; division always yields a decimal (computed with `DECIMAL64` to avoid
    * non-terminating expansions). */
  private def evalArith(op: ArithOp, a: JValue, b: JValue): JValue =
    (asBigDecimal(a), asBigDecimal(b)) match {
      case (Some(x), Some(y)) =>
        op match {
          case ArithOp.Add => numResult(a, b, x + y)
          case ArithOp.Sub => numResult(a, b, x - y)
          case ArithOp.Mul => numResult(a, b, x * y)
          case ArithOp.Mod => if (y.signum == 0) NullV else numResult(a, b, x.remainder(y))
          case ArithOp.Div => if (y.signum == 0) NullV else DecimalV(BigDecimal(x.bigDecimal.divide(y.bigDecimal, java.math.MathContext.DECIMAL64)))
        }
      case _ => NullV
    }

  /** Keep integral arithmetic integral (`LongV` ⊕ `LongV` ⇒ `LongV`); otherwise a decimal. */
  private def numResult(a: JValue, b: JValue, r: BigDecimal): JValue =
    (a, b) match {
      case (LongV(_), LongV(_)) => LongV(r.toLong)
      case _                    => DecimalV(r)
    }

  private def compareJV(a: JValue, b: JValue): Option[Int] =
    (a, b) match {
      case (StringV(x), StringV(y))         => Some(x.compareTo(y))
      case (BoolV(x), BoolV(y))             => Some(x.compareTo(y))
      case (InstantV(x), InstantV(y))       => Some(x.compareTo(y))
      case (IdentifierV(x), IdentifierV(y)) => Some(x.compareTo(y))
      // A genuine instant compared against a string: parse the string as a timestamp (so `created`,
      // stored as text, compares correctly against `NOW()` or a date function's result).
      case (InstantV(x), StringV(y))        => parseInstant(y).map(x.compareTo)
      case (StringV(x), InstantV(y))        => parseInstant(x).map(_.compareTo(y))
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
      case Expr.Col(n)            => lk(n)
      case Expr.Lit(l)            => litToJV(l)
      case Expr.Func(name, as)    => evalFunc(name, as.map(operand(_, lk, agg)))
      case Expr.Aggregate(f, c)   => agg((f, c))
      case Expr.Arith(op, l, r)   => evalArith(op, operand(l, lk, agg), operand(r, lk, agg))
      case other                  => BoolV(evalBool(other, lk, agg))
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

  /** Scalar functions. NULL arguments propagate to a NULL (or non-matching) result.
    *
    *   - `LENGTH(x)` — character length of `x` as text.
    *   - `UPPER(s)` / `LOWER(s)` — case conversion.
    *   - `TRIM(s)` / `LTRIM(s)` / `RTRIM(s)` — strip surrounding / leading / trailing whitespace.
    *   - `SUBSTR(s, start [, len])` (alias `SUBSTRING`) — 1-based substring.
    *   - `CONCAT(a, b, …)` — concatenate the arguments as text.
    *   - `REPLACE(s, from, to)` — replace every literal occurrence of `from` with `to`.
    *   - `INSTR(s, sub)` — 1-based index of the first occurrence of `sub` in `s` (`0` if absent).
    *   - `GEO_DISTANCE(lat1, lon1, lat2, lon2)` / `GEO_DISTANCE(point, lat2, lon2)` — great-circle
    *     distance in metres (haversine); the object form reads `latitude`/`longitude` from `point`.
    *   - `GEO_WITHIN(lat1, lon1, lat2, lon2, radius)` / `GEO_WITHIN(point, lat2, lon2, radius)` —
    *     boolean `distance <= radius` (metres).
    *   - `NOW()` — the current instant.
    *   - `YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(ts)` — extract a UTC calendar field as an integer; `ts`
    *     may be an instant or an ISO-8601 string (date, date-time, or offset date-time).
    *   - `DATE_DIFF(unit, a, b)` — `a - b` as a whole number of `unit`s (`second`/`minute`/`hour`/
    *     `day`/`millisecond`, singular or plural).
    */
  private def evalFunc(name: String, args: List[JValue]): JValue =
    (name, args) match {
      case ("length", List(NullV))      => NullV
      case ("length", List(StringV(s))) => LongV(s.length.toLong)
      case ("length", List(v))          => LongV(jvToString(v).length.toLong)

      case ("upper", List(v)) => textFn(v)(_.toUpperCase)
      case ("lower", List(v)) => textFn(v)(_.toLowerCase)
      case ("trim", List(v))  => textFn(v)(_.trim)
      case ("ltrim", List(v)) => textFn(v)(_.stripLeading)
      case ("rtrim", List(v)) => textFn(v)(_.stripTrailing)

      case ("substr" | "substring", List(s, start))      => substr(s, start, None)
      case ("substr" | "substring", List(s, start, len)) => substr(s, start, Some(len))

      case ("concat", parts) if parts.nonEmpty =>
        val texts = parts.map(asText)
        if (texts.contains(None)) NullV else StringV(texts.flatten.mkString)

      case ("replace", List(s, from, to)) =>
        (asText(s), asText(from), asText(to)) match {
          case (Some(a), Some(b), Some(c)) => StringV(a.replace(b, c))
          case _                           => NullV
        }

      case ("instr", List(s, sub)) =>
        (asText(s), asText(sub)) match {
          case (Some(a), Some(b)) => LongV((a.indexOf(b) + 1).toLong)
          case _                  => NullV
        }

      case ("geo_distance", List(la1, lo1, la2, lo2)) => geoDistance(la1, lo1, la2, lo2)
      case ("geo_distance", List(p, la2, lo2))        => pointLatLon(p).fold(NullV: JValue) { case (la1, lo1) => geoDistance(la1, lo1, la2, lo2) }

      case ("geo_within", List(la1, lo1, la2, lo2, r)) => geoWithin(la1, lo1, la2, lo2, r)
      case ("geo_within", List(p, la2, lo2, r))        => pointLatLon(p).fold(NullV: JValue) { case (la1, lo1) => geoWithin(la1, lo1, la2, lo2, r) }

      case ("now", Nil)         => InstantV(java.time.Instant.now())
      case ("year", List(v))    => instantField(v)(_.getYear.toLong)
      case ("month", List(v))   => instantField(v)(_.getMonthValue.toLong)
      case ("day", List(v))     => instantField(v)(_.getDayOfMonth.toLong)
      case ("hour", List(v))    => instantField(v)(_.getHour.toLong)
      case ("minute", List(v))  => instantField(v)(_.getMinute.toLong)
      case ("second", List(v))  => instantField(v)(_.getSecond.toLong)
      case ("date_diff", List(unit, a, b)) => dateDiff(unit, a, b)

      case _ => NullV
    }

  /** Field set whose `DATE_DIFF`/extraction results are integer-typed (for the result `Column` hint). */
  private val dateIntFuncs: Set[String] = Set("year", "month", "day", "hour", "minute", "second")

  /** Interpret a value as an instant: an `InstantV`, or an ISO-8601 string (date / date-time / offset
    * date-time). Stored timestamps decode as plain strings, so both shapes must be accepted. */
  private def asInstant(jv: JValue): Option[java.time.Instant] =
    jv match {
      case InstantV(i) => Some(i)
      case StringV(s)  => parseInstant(s)
      case _           => None
    }

  private def parseInstant(s: String): Option[java.time.Instant] = {
    import java.time.*
    def attempt[A](f: => A): Option[A] = scala.util.Try(f).toOption
    attempt(Instant.parse(s))
      .orElse(attempt(OffsetDateTime.parse(s).toInstant))
      .orElse(attempt(LocalDateTime.parse(s).toInstant(ZoneOffset.UTC)))
      .orElse(attempt(LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant))
  }

  /** Extract a UTC calendar field from a value interpreted as an instant (NULL if it is not one). */
  private def instantField(v: JValue)(f: java.time.ZonedDateTime => Long): JValue =
    asInstant(v).fold(NullV: JValue)(i => LongV(f(i.atZone(java.time.ZoneOffset.UTC))))

  /** `a - b` as a whole number of the named unit; NULL if either side is not an instant or the unit is
    * unknown. */
  private def dateDiff(unit: JValue, a: JValue, b: JValue): JValue =
    (asInstant(a), asInstant(b), unit) match {
      case (Some(ia), Some(ib), StringV(u)) =>
        val d = java.time.Duration.between(ib, ia)
        u.toLowerCase match {
          case "second" | "seconds" | "sec"           => LongV(d.getSeconds)
          case "minute" | "minutes" | "min"           => LongV(d.toMinutes)
          case "hour" | "hours"                        => LongV(d.toHours)
          case "day" | "days"                          => LongV(d.toDays)
          case "millisecond" | "milliseconds" | "ms"  => LongV(d.toMillis)
          case _                                       => NullV
        }
      case _ => NullV
    }

  /** String functions whose result is text (for the result `Column` hint). */
  private val strTextFuncs: Set[String] = Set("upper", "lower", "trim", "ltrim", "rtrim", "substr", "substring", "concat", "replace")

  /** A value as text, or NULL (so a string function propagates NULL); a non-string is rendered. */
  private def asText(jv: JValue): Option[String] = jv match { case NullV => None; case v => Some(jvToString(v)) }

  private def asInt(jv: JValue): Option[Int] = asBigDecimal(jv).map(_.toInt)

  /** Apply a text transform NULL-safely. */
  private def textFn(v: JValue)(f: String => String): JValue = asText(v).fold(NullV: JValue)(s => StringV(f(s)))

  /** 1-based substring; `start` is clamped to the string and a missing/negative `len` runs to the end.
    * NULL (or a non-numeric `start`/`len`) yields NULL. */
  private def substr(s: JValue, start: JValue, length: Option[JValue]): JValue =
    (asText(s), asInt(start)) match {
      case (Some(str), Some(st)) =>
        val lenInt = length.map(asInt) // None = no len arg; Some(None) = non-numeric len; Some(Some(n)) = ok
        if (lenInt.contains(None)) NullV
        else {
          val from = math.max(0, st - 1)
          if (from >= str.length) StringV("")
          else StringV(str.substring(from, lenInt.flatten.fold(str.length)(l => math.min(str.length, from + math.max(0, l)))))
        }
      case _ => NullV
    }

  /** Great-circle distance in metres, or NULL if any coordinate is missing/non-numeric. */
  private def geoDistance(lat1: JValue, lon1: JValue, lat2: JValue, lon2: JValue): JValue =
    (asDouble(lat1), asDouble(lon1), asDouble(lat2), asDouble(lon2)) match {
      case (Some(a), Some(b), Some(c), Some(d)) => DoubleV(GEOTools.haversineMeters(a, b, c, d))
      case _                                    => NullV
    }

  private def geoWithin(lat1: JValue, lon1: JValue, lat2: JValue, lon2: JValue, radius: JValue): JValue =
    (geoDistance(lat1, lon1, lat2, lon2), asDouble(radius)) match {
      case (DoubleV(dist), Some(r)) => BoolV(dist <= r)
      case _                        => NullV
    }

  /** Read `latitude`/`longitude` from an object value (e.g. `o.location`), for the object-arg geo forms. */
  private def pointLatLon(p: JValue): Option[(JValue, JValue)] =
    p match {
      case MapV(m) => for { la <- m.get("latitude"); lo <- m.get("longitude") } yield (la, lo)
      case _       => None
    }

  private def asDouble(jv: JValue): Option[Double] =
    jv match {
      case LongV(v)    => Some(v.toDouble)
      case DoubleV(v)  => Some(v)
      case DecimalV(v) => Some(v.toDouble)
      case StringV(s)  => s.toDoubleOption
      case _           => None
    }

  /** A compact textual label for an expression — the default output-column name when no `AS` alias is
    * given (e.g. `geo_distance(latitude, longitude, 48.8566, 2.3522)`). */
  private def exprLabel(e: Expr): String =
    e match {
      case Expr.Col(n)          => unqualify(n)
      case Expr.Lit(l)          => litToJV(l) match { case StringV(s) => s; case other => jvToString(other) }
      case Expr.Func(name, as)  => s"$name(${as.map(exprLabel).mkString(", ")})"
      case Expr.Aggregate(f, c) => c.fold(s"${aggLabel(f)}(*)")(col => s"${aggLabel(f)}(${unqualify(col)})")
      case Expr.Arith(op, l, r) => s"${exprLabel(l)} ${arithSymbol(op)} ${exprLabel(r)}"
      case _                    => "expr"
    }

  private def arithSymbol(op: ArithOp): String =
    op match { case ArithOp.Add => "+"; case ArithOp.Sub => "-"; case ArithOp.Mul => "*"; case ArithOp.Div => "/"; case ArithOp.Mod => "%" }

  /** The result type hint of a projected expression (for the result `Column`). */
  private def exprTypeHint(e: Expr, sources: List[Source]): String =
    e match {
      case Expr.Col(n)                  => hintFor(sources, n)
      case Expr.Func("geo_distance", _) => "number"
      case Expr.Func("geo_within", _)   => "boolean"
      case Expr.Func("length", _)       => "integer"
      case Expr.Func("instr", _)        => "integer"
      case Expr.Func("date_diff", _)    => "integer"
      case Expr.Func("now", _)          => "timestamp"
      case Expr.Func(n, _) if dateIntFuncs(n) => "integer"
      case Expr.Func(n, _) if strTextFuncs(n) => "string"
      case Expr.Func(_, _)              => "any"
      case Expr.Arith(_, _, _)          => "number"
      case Expr.Lit(Literal.IntLit(_))  => "integer"
      case Expr.Lit(Literal.DecLit(_))  => "number"
      case Expr.Lit(Literal.StrLit(_))  => "string"
      case Expr.Lit(Literal.BoolLit(_)) => "boolean"
      case Expr.Lit(Literal.NullLit)    => "any"
      case _                            => "boolean" // comparisons / logical operators
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
      case Expr.Arith(_, l, r)       => aggsInExpr(l) ++ aggsInExpr(r)
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
      case Expr.Arith(_, l, r)  => freeColsInExpr(l) ++ freeColsInExpr(r)
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
