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

import zio.lmdb.{IndexComponent, IndexComponentSource}
import zio.lmdb.keycodecs.TupleKeyLayout
import zio.lmdb.sql.parser.*
import zio.lmdb.sql.runtime.KeyRegistry

/** The access path chosen for a single-table query. Byte bounds are over the scanned DBI's keys (the collection's keys for `Pk*`, the index `FROM_KEY`s for `IndexScan`); `residual` is the part of the WHERE not served by the access path (re-evaluated
  * per row). Conjuncts served by the scan are NOT re-evaluated: the index component's `keyId` ordering (e.g. instant semantics for a timestamp component) is authoritative for them.
  */
enum ScanPlan {

  /** Read every record; evaluate the whole WHERE in memory (the pre-planner behaviour). */
  case FullScan(residual: Option[Expr])

  /** Point-fetch records by primary key (`_key =` / `_key IN`). */
  case PkFetch(keys: List[Array[Byte]], residual: Option[Expr], detail: String)

  /** Range scan over the collection's own key order (`_key` comparisons). */
  case PkRange(lower: Option[Array[Byte]], upper: Option[Array[Byte]], ordered: Boolean, residual: Option[Expr], detail: String)

  /** Range scan over an index, then point-fetch each record by the primary key extracted from the index entry's `TO_KEY` (component `pkComponent`, sliced via `toWidths`).
    */
  case IndexScan(
    indexName: String,
    sourceName: String,
    lower: Option[Array[Byte]],
    upper: Option[Array[Byte]],
    toWidths: List[Option[Int]],
    pkComponent: Int,
    ordered: Boolean,
    residual: Option[Expr],
    detail: String
  )

  def residualWhere: Option[Expr] = this match {
    case FullScan(r)                          => r
    case PkFetch(_, r, _)                     => r
    case PkRange(_, _, _, r, _)               => r
    case IndexScan(_, _, _, _, _, _, _, r, _) => r
  }

  /** Whether the scan already delivers rows in the query's ORDER BY order (so sorting can be skipped). */
  def orderedSatisfied: Boolean = this match {
    case PkRange(_, _, o, _, _)               => o
    case IndexScan(_, _, _, _, _, _, o, _, _) => o
    case _                                    => false
  }
}

/** Chooses a [[ScanPlan]] for a single-table query from the WHERE conjuncts, the declared indexes (leftmost-prefix matching over their `IndexMapping` components), and the ORDER BY shape.
  */
object Planner {

  /** One pushable comparison extracted from a conjunct: the (normalized) column path it constrains, how, and with which literal(s).
    */
  private sealed trait Constraint { def conjunct: Expr; def path: String }
  private final case class EqC(path: String, lit: Literal, conjunct: Expr)                  extends Constraint
  private final case class InC(path: String, lits: List[Literal], conjunct: Expr)           extends Constraint
  private final case class RangeC(path: String, op: CmpOp, lit: Literal, conjunct: Expr)    extends Constraint
  private final case class BetweenC(path: String, lo: Literal, hi: Literal, conjunct: Expr) extends Constraint

  private def conjunctsOf(e: Expr): List[Expr] =
    e match {
      case Expr.And(l, r) => conjunctsOf(l) ++ conjunctsOf(r)
      case other          => List(other)
    }

  /** Strip a leading `alias.` qualifier so predicate paths compare against declared field paths. */
  private def normalize(alias: String, col: String): String =
    if (col.startsWith(alias + ".")) col.substring(alias.length + 1) else col

  private def constraintOf(alias: String, c: Expr): Option[Constraint] =
    c match {
      case Expr.Cmp(CmpOp.Eq, Expr.Col(p), Expr.Lit(l))                                         => Some(EqC(normalize(alias, p), l, c))
      case Expr.Cmp(CmpOp.Eq, Expr.Lit(l), Expr.Col(p))                                         => Some(EqC(normalize(alias, p), l, c))
      case Expr.Cmp(op @ (CmpOp.Lt | CmpOp.Le | CmpOp.Gt | CmpOp.Ge), Expr.Col(p), Expr.Lit(l)) => Some(RangeC(normalize(alias, p), op, l, c))
      case Expr.Cmp(op @ (CmpOp.Lt | CmpOp.Le | CmpOp.Gt | CmpOp.Ge), Expr.Lit(l), Expr.Col(p)) => Some(RangeC(normalize(alias, p), flip(op), l, c))
      case Expr.Between(Expr.Col(p), Expr.Lit(lo), Expr.Lit(hi), false)                         => Some(BetweenC(normalize(alias, p), lo, hi, c))
      case Expr.In(Expr.Col(p), items, false)                                                   =>
        val lits = items.collect { case Expr.Lit(l) => l }
        if (lits.size == items.size) Some(InC(normalize(alias, p), lits, c)) else None
      case _                                                                                    => None
    }

  private def flip(op: CmpOp): CmpOp =
    op match { case CmpOp.Lt => CmpOp.Gt; case CmpOp.Le => CmpOp.Ge; case CmpOp.Gt => CmpOp.Lt; case CmpOp.Ge => CmpOp.Le; case other => other }

  private def andAll(es: List[Expr]): Option[Expr] = es.reduceLeftOption(Expr.And(_, _))

  /** The ascending ORDER BY column paths, or `None` when the ORDER BY cannot be order-matched (empty, a DESC key, or a non-column key).
    */
  private def ascendingOrderPaths(alias: String, orderBy: List[OrderBy]): Option[List[String]] =
    if (orderBy.isEmpty) None
    else {
      val paths = orderBy.map {
        case OrderBy(Expr.Col(p), false) => Some(normalize(alias, p))
        case _                           => None
      }
      if (paths.forall(_.isDefined)) Some(paths.flatten) else None
    }

  /** The predicate path a component answers for, when it has one (`None` = not matchable). */
  private def componentPath(c: IndexComponent): Option[String] =
    c.source match {
      case IndexComponentSource.Field(p)     => Some(p)
      case IndexComponentSource.PrimaryKey   => Some("_key")
      case IndexComponentSource.Coalesce(ps) => ps.headOption
      case IndexComponentSource.Opaque(_)    => None
    }

  /** Whether a conjunct pushed on this component must still be re-checked per row (a coalesce component also indexes fallback values, so the scan is a superset).
    */
  private def needsRecheck(c: IndexComponent): Boolean =
    c.source match { case IndexComponentSource.Coalesce(_) => true; case _ => false }

  private def plusZero(b: Array[Byte]): Array[Byte] = b :+ 0.toByte

  private final case class Candidate(
    idx: IndexInfo,
    eqCount: Int,
    hasRange: Boolean,
    ordered: Boolean,
    lower: Option[Array[Byte]],
    upper: Option[Array[Byte]],
    consumed: List[Expr],
    recheck: List[Expr],
    pkComponent: Int,
    detail: String
  )

  /** Try to serve the conjuncts with one index: match an equality per component left to right, then at most one range (or BETWEEN) on the next component, and derive `[lower, upper)` byte bounds.
    */
  private def matchIndex(idx: IndexInfo, constraints: List[Constraint], orderPaths: Option[List[String]]): Option[Candidate] = {
    val pkComponent = idx.toComponents.indexWhere(_.source == IndexComponentSource.PrimaryKey)
    if (pkComponent < 0) None // no way back to the record
    else {
      val comps  = idx.fromComponents
      val widths = comps.map(_.fixedWidth)
      val n      = comps.size

      // Leftmost equality prefix.
      var eqBytes  = List.empty[Array[Byte]]
      var consumed = List.empty[Expr]
      var recheck  = List.empty[Expr]
      var eqLabels = List.empty[String]
      var i        = 0
      var halted   = false
      while (!halted && i < n) {
        val comp = comps(i)
        val hit  = componentPath(comp).flatMap { path =>
          constraints.collectFirst { case ec @ EqC(p, lit, _) if p == path => (ec, lit) }.flatMap { case (ec, lit) =>
            KeyRegistry.encodeComponent(comp.keyId, lit).toOption.map(bytes => (ec, bytes))
          }
        }
        hit match {
          case Some((ec, bytes)) =>
            eqBytes = eqBytes :+ bytes
            consumed = consumed :+ ec.conjunct
            if (needsRecheck(comp)) recheck = recheck :+ ec.conjunct
            eqLabels = eqLabels :+ ec.path
            i += 1
          case None              => halted = true
        }
      }
      val eqCount  = i

      // At most one range (one lower + one upper side) on the next component.
      val rangeComp                          = if (eqCount < n) Some(comps(eqCount)) else None
      var lowerV: Option[(Literal, Boolean)] = None // (literal, inclusive)
      var upperV: Option[(Literal, Boolean)] = None
      var rangeConsumed                      = List.empty[Expr]
      var rangeLabels                        = List.empty[String]
      rangeComp.foreach { comp =>
        componentPath(comp).foreach { path =>
          constraints.foreach {
            case rc @ RangeC(p, op, lit, _) if p == path && KeyRegistry.encodeComponent(comp.keyId, lit).isRight =>
              op match {
                case CmpOp.Ge if lowerV.isEmpty => lowerV = Some((lit, true)); rangeConsumed :+= rc.conjunct; rangeLabels :+= s"$p >="
                case CmpOp.Gt if lowerV.isEmpty => lowerV = Some((lit, false)); rangeConsumed :+= rc.conjunct; rangeLabels :+= s"$p >"
                case CmpOp.Le if upperV.isEmpty => upperV = Some((lit, true)); rangeConsumed :+= rc.conjunct; rangeLabels :+= s"$p <="
                case CmpOp.Lt if upperV.isEmpty => upperV = Some((lit, false)); rangeConsumed :+= rc.conjunct; rangeLabels :+= s"$p <"
                case _                          => ()
              }
            case bc @ BetweenC(p, lo, hi, _)
                if p == path && lowerV.isEmpty && upperV.isEmpty &&
                  KeyRegistry.encodeComponent(comp.keyId, lo).isRight && KeyRegistry.encodeComponent(comp.keyId, hi).isRight =>
              lowerV = Some((lo, true)); upperV = Some((hi, true)); rangeConsumed :+= bc.conjunct; rangeLabels :+= s"$p between"
            case _                                                                                               => ()
          }
          if (rangeConsumed.nonEmpty && needsRecheck(comp)) recheck = recheck ++ rangeConsumed
        }
      }
      val hasRange                           = rangeConsumed.nonEmpty

      // Does the scan order match the ORDER BY? After the pinned equality prefix, entries follow
      // the remaining FROM components then the TO components (DUPSORT duplicates sort by value).
      val orderable = (comps.drop(eqCount) ++ idx.toComponents).map(componentPathForOrdering)
      val ordered   = orderPaths.exists(ps => ps.size <= orderable.size && orderable.take(ps.size).zip(ps).forall { case (c, p) => c.contains(p) })

      if (eqCount == 0 && !hasRange && !ordered) None
      else {
        // [lower, upper) over the index FROM_KEY bytes. `isLast` = the range component is the whole
        // remaining key, where extensions of the encoded value must stay inside the bound.
        val isLast                        = eqCount == n - 1
        def prefix(vs: List[Array[Byte]]) = TupleKeyLayout.prefixBytes(widths, vs)
        def rangePrefix(lit: Literal)     = prefix(eqBytes :+ KeyRegistry.encodeComponent(comps(eqCount).keyId, lit).toOption.get)

        var extraRecheck   = List.empty[Expr]
        val (lower, upper) =
          if (rangeComp.isEmpty || !hasRange) {
            if (eqCount == 0) (None, None)
            else if (eqCount == n) { val p = prefix(eqBytes); (Some(p), Some(plusZero(p))) }
            else { val p = prefix(eqBytes); (Some(p), TupleKeyLayout.byteSuccessor(p)) }
          } else {
            val lo      = lowerV.map { case (lit, inclusive) =>
              val p = rangePrefix(lit)
              if (inclusive) Some(p)
              else if (isLast) Some(plusZero(p))
              else TupleKeyLayout.byteSuccessor(p)
            }
            val hi      = upperV.map { case (lit, inclusive) =>
              val p = rangePrefix(lit)
              if (!inclusive) Some(p)
              else if (isLast) Some(plusZero(p))
              else TupleKeyLayout.byteSuccessor(p)
            }
            // A missing successor (all-0xFF prefix) cannot be bounded: keep the conjunct as a
            // recheck and fall back to the equality-prefix bound on that side.
            val eqLower = if (eqCount == 0) None else Some(prefix(eqBytes))
            val eqUpper = if (eqCount == 0) None else TupleKeyLayout.byteSuccessor(prefix(eqBytes))
            val loB     = lo match {
              case Some(Some(b)) => Some(b)
              case Some(None)    => extraRecheck = extraRecheck ++ rangeConsumed; eqLower
              case None          => eqLower
            }
            val hiB     = hi match {
              case Some(Some(b)) => Some(b)
              case Some(None)    => extraRecheck = extraRecheck ++ rangeConsumed; eqUpper
              case None          => eqUpper
            }
            (loB, hiB)
          }

        val detail =
          (if (eqLabels.nonEmpty) List(s"eq(${eqLabels.mkString(", ")})") else Nil) ++
            (if (rangeLabels.nonEmpty) List(s"range(${rangeLabels.mkString(", ")})") else Nil) ++
            (if (ordered) List("ordered") else Nil)

        Some(
          Candidate(
            idx,
            eqCount,
            hasRange,
            ordered,
            lower,
            upper,
            consumed ++ rangeConsumed,
            (recheck ++ extraRecheck).distinct,
            pkComponent,
            detail.mkString(" ")
          )
        )
      }
    }
  }

  /** Like [[componentPath]] but for order matching: a coalesce component's order is the coalesced value's order, which no single column reproduces, so it never order-matches.
    */
  private def componentPathForOrdering(c: IndexComponent): Option[String] =
    c.source match {
      case IndexComponentSource.Field(p)   => Some(p)
      case IndexComponentSource.PrimaryKey => Some("_key")
      case _                               => None
    }

  /** Choose the access path: primary-key point access, then the most selective declared index (longest equality prefix, then range, then order match), then a primary-key range, then an order-matching index scan, then a full scan.
    */
  def plan(alias: String, info: CollectionInfo, where: Option[Expr], orderBy: List[OrderBy]): ScanPlan = {
    val cj          = where.map(conjunctsOf).getOrElse(Nil)
    val constraints = cj.flatMap(constraintOf(alias, _))
    val orderPaths  = ascendingOrderPaths(alias, orderBy)

    def residualExcept(consumed: List[Expr], keep: List[Expr]): Option[Expr] =
      andAll(cj.filterNot(c => consumed.contains(c) && !keep.contains(c)))

    // 1. `_key =` / `_key IN` — point fetches.
    val pkFetch = info.keyId.flatMap { kid =>
      constraints.collectFirst {
        case EqC("_key", lit, conjunct) if KeyRegistry.encodeComponent(kid, lit).isRight             =>
          ScanPlan.PkFetch(List(KeyRegistry.encodeComponent(kid, lit).toOption.get), residualExcept(List(conjunct), Nil), "eq(_key)")
        case InC("_key", lits, conjunct) if lits.forall(KeyRegistry.encodeComponent(kid, _).isRight) =>
          ScanPlan.PkFetch(lits.map(KeyRegistry.encodeComponent(kid, _).toOption.get), residualExcept(List(conjunct), Nil), s"in(_key, ${lits.size})")
      }
    }

    // 2. Declared indexes (leftmost-prefix).
    def indexPlan(c: Candidate): ScanPlan =
      ScanPlan.IndexScan(
        c.idx.name,
        info.name,
        c.lower,
        c.upper,
        c.idx.toComponents.map(_.fixedWidth),
        c.pkComponent,
        c.ordered,
        residualExcept(c.consumed, c.recheck),
        c.detail
      )
    val candidates                        = info.indexes.flatMap(matchIndex(_, constraints, orderPaths))
    val bestEq                            = candidates.filter(c => c.eqCount > 0 || c.hasRange).sortBy(c => (-c.eqCount, !c.hasRange, !c.ordered)).headOption
    val orderedOnly                       = candidates.filter(c => c.eqCount == 0 && !c.hasRange && c.ordered).headOption

    // 3. `_key` range.
    val pkRange = info.keyId.flatMap { kid =>
      var lower: Option[Array[Byte]] = None
      var upper: Option[Array[Byte]] = None
      var consumed                   = List.empty[Expr]
      var labels                     = List.empty[String]
      constraints.foreach {
        case RangeC("_key", op, lit, conjunct) if KeyRegistry.encodeComponent(kid, lit).isRight =>
          val b = KeyRegistry.encodeComponent(kid, lit).toOption.get
          op match {
            case CmpOp.Ge if lower.isEmpty => lower = Some(b); consumed :+= conjunct; labels :+= "_key >="
            case CmpOp.Gt if lower.isEmpty => lower = Some(plusZero(b)); consumed :+= conjunct; labels :+= "_key >"
            case CmpOp.Le if upper.isEmpty => upper = Some(plusZero(b)); consumed :+= conjunct; labels :+= "_key <="
            case CmpOp.Lt if upper.isEmpty => upper = Some(b); consumed :+= conjunct; labels :+= "_key <"
            case _                         => ()
          }
        case BetweenC("_key", lo, hi, conjunct)
            if lower.isEmpty && upper.isEmpty &&
              KeyRegistry.encodeComponent(kid, lo).isRight && KeyRegistry.encodeComponent(kid, hi).isRight =>
          lower = Some(KeyRegistry.encodeComponent(kid, lo).toOption.get)
          upper = Some(plusZero(KeyRegistry.encodeComponent(kid, hi).toOption.get))
          consumed :+= conjunct; labels :+= "_key between"
        case _                                                                                  => ()
      }
      val ordered                    = orderPaths.contains(List("_key"))
      if (consumed.isEmpty && !ordered) None
      else Some(ScanPlan.PkRange(lower, upper, ordered, residualExcept(consumed, Nil), (labels ++ (if (ordered) List("ordered") else Nil)).mkString(" ")))
    }

    pkFetch
      .orElse(bestEq.map(indexPlan))
      .orElse(pkRange.filter { case ScanPlan.PkRange(l, u, _, _, _) => l.nonEmpty || u.nonEmpty; case _ => false })
      .orElse(orderedOnly.map(indexPlan))
      .orElse(pkRange)
      .getOrElse(ScanPlan.FullScan(where))
  }

  /** A short human rendering of a predicate, for EXPLAIN output. */
  def renderExpr(e: Expr): String =
    e match {
      case Expr.Col(n)                  => n
      case Expr.Lit(Literal.StrLit(s))  => s"'$s'"
      case Expr.Lit(Literal.IntLit(n))  => n.toString
      case Expr.Lit(Literal.DecLit(d))  => d.toString
      case Expr.Lit(Literal.BoolLit(b)) => b.toString
      case Expr.Lit(Literal.NullLit)    => "null"
      case Expr.Cmp(op, l, r)           =>
        val sym = op match { case CmpOp.Eq => "="; case CmpOp.Ne => "<>"; case CmpOp.Lt => "<"; case CmpOp.Le => "<="; case CmpOp.Gt => ">"; case CmpOp.Ge => ">=" }
        s"${renderExpr(l)} $sym ${renderExpr(r)}"
      case Expr.And(l, r)               => s"${renderExpr(l)} and ${renderExpr(r)}"
      case Expr.Or(l, r)                => s"(${renderExpr(l)} or ${renderExpr(r)})"
      case Expr.Not(x)                  => s"not ${renderExpr(x)}"
      case Expr.Like(t, p)              => s"${renderExpr(t)} like '$p'"
      case Expr.IsNull(t, neg)          => s"${renderExpr(t)} is ${if (neg) "not " else ""}null"
      case Expr.In(t, its, neg)         => s"${renderExpr(t)}${if (neg) " not" else ""} in (${its.map(renderExpr).mkString(", ")})"
      case Expr.Between(t, lo, hi, neg) => s"${renderExpr(t)}${if (neg) " not" else ""} between ${renderExpr(lo)} and ${renderExpr(hi)}"
      case Expr.Func(n, as)             => s"$n(${as.map(renderExpr).mkString(", ")})"
      case Expr.Arith(_, _, _)          => "arith"
      case Expr.Case(_, _, _)           => "case"
      case Expr.Aggregate(_, _, _)      => "aggregate"
    }
}
