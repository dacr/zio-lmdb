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
package zio.lmdb.schema

import zio.lmdb.json.JValue
import zio.lmdb.json.JValue.{ListV, MapV, StringV}

import scala.collection.immutable.ListMap
import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror

/** Structural description of a type `T` as a [[JValue]] tree, shaped like a (lightweight) JSON
  * Schema document and derived from the Scala model via `Mirror` — no runtime reflection, no extra
  * dependency.
  *
  *   - leaf types  → `{"type":"string"}` / `{"type":"integer"}` / … (`Instant`, `UUID` add a
  *     `format`)
  *   - case class  → `{"type":"object","properties":{…},"required":[…]}`
  *   - sealed/enum → `{"oneOf":[…]}`
  *   - `Option[T]` → the inner shape, with the field omitted from the enclosing `required` list
  *   - `Seq`/`List`/`Vector`/`Set[T]` → `{"type":"array","items":…}`
  *   - `Map[String, V]` → `{"type":"object","additionalProperties":…}`
  *
  * This is the source the L2A catalog uses when a type opts in via `derives LMDBSchema`. It is
  * deliberately not a spec-complete JSON Schema 2020-12 document (no `$schema`, no `$ref`/`$defs`);
  * it is rich enough for structural drift detection and human inspection.
  *
  * '''Recursion.''' A self-referential model (e.g. `case class Tree(children: Seq[Tree])`) makes
  * the inductive derivation diverge at compile time. Such types must declare an explicit
  * `given LMDBSchema[T]` instead; a `$ref`/`$defs` emitter is left for a later iteration.
  */
trait SchemaShape[T] {
  def shape: JValue
}

object SchemaShape extends SchemaShapeLowPriority {

  def apply[T](using s: SchemaShape[T]): SchemaShape[T] = s

  private def typeOnly[T](name: String): SchemaShape[T] = make(MapV(ListMap("type" -> StringV(name))))

  private def stringFormat[T](format: String): SchemaShape[T] =
    make(MapV(ListMap("type" -> StringV("string"), "format" -> StringV(format))))

  private def array(items: JValue): JValue =
    MapV(ListMap("type" -> StringV("array"), "items" -> items))

  given SchemaShape[String]            = typeOnly("string")
  given SchemaShape[Boolean]           = typeOnly("boolean")
  given SchemaShape[Byte]              = typeOnly("integer")
  given SchemaShape[Short]             = typeOnly("integer")
  given SchemaShape[Int]               = typeOnly("integer")
  given SchemaShape[Long]              = typeOnly("integer")
  given SchemaShape[BigInt]            = typeOnly("integer")
  given SchemaShape[Float]             = typeOnly("number")
  given SchemaShape[Double]            = typeOnly("number")
  given SchemaShape[BigDecimal]        = typeOnly("number")
  given SchemaShape[java.time.Instant] = stringFormat("date-time")
  given SchemaShape[java.util.UUID]    = stringFormat("uuid")

  given optionShape[T](using s: SchemaShape[T]): SchemaShape[Option[T]] = make(s.shape)
  given seqShape[T](using s: SchemaShape[T]): SchemaShape[Seq[T]]       = make(array(s.shape))
  given listShape[T](using s: SchemaShape[T]): SchemaShape[List[T]]     = make(array(s.shape))
  given vectorShape[T](using s: SchemaShape[T]): SchemaShape[Vector[T]] = make(array(s.shape))
  given setShape[T](using s: SchemaShape[T]): SchemaShape[Set[T]]       = make(array(s.shape))
  given mapShape[V](using s: SchemaShape[V]): SchemaShape[Map[String, V]] =
    make(MapV(ListMap("type" -> StringV("object"), "additionalProperties" -> s.shape)))
}

/** The `Mirror`-based product / sum derivation lives at lower priority than the explicit leaf and
  * collection givens above, so a concrete type (e.g. `Option[T]`, which has its own `Mirror`)
  * resolves to its hand-written shape rather than the generic structural one.
  */
trait SchemaShapeLowPriority {

  protected def make[T](jv: JValue): SchemaShape[T] = new SchemaShape[T] { val shape: JValue = jv }

  inline given derived[T](using m: Mirror.Of[T]): SchemaShape[T] =
    inline m match {
      case p: Mirror.ProductOf[T] => make(objectSchema(labelsOf[p.MirroredElemLabels], fieldShapesOf[p.MirroredElemTypes]))
      case s: Mirror.SumOf[T]     => make(oneOfSchema(variantShapesOf[s.MirroredElemTypes]))
    }

  /** Non-inline so the collection combinators below are compiled once rather than duplicated at
    * every derivation site. `fields` pairs each property shape with whether it is optional.
    */
  protected def objectSchema(names: List[String], fields: List[(JValue, Boolean)]): JValue = {
    val props    = ListMap.from(names.zip(fields.map(_._1)))
    val required = names.zip(fields).collect { case (n, (_, false)) => StringV(n) }
    val base     = ListMap[String, JValue]("type" -> StringV("object"), "properties" -> MapV(props))
    MapV(if (required.isEmpty) base else base + ("required" -> ListV(required)))
  }

  protected def oneOfSchema(variants: List[JValue]): JValue = MapV(ListMap("oneOf" -> ListV(variants)))

  private inline def labelsOf[Ts <: Tuple]: List[String] =
    inline erasedValue[Ts] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => constValue[t].asInstanceOf[String] :: labelsOf[ts]
    }

  /** For each element type, its shape paired with whether it is optional (an `Option[_]`). */
  private inline def fieldShapesOf[Ts <: Tuple]: List[(JValue, Boolean)] =
    inline erasedValue[Ts] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => fieldShapeOf[t] :: fieldShapesOf[ts]
    }

  private inline def fieldShapeOf[T]: (JValue, Boolean) =
    inline erasedValue[T] match {
      case _: Option[a] => (summonInline[SchemaShape[a]].shape, true)
      case _            => (summonInline[SchemaShape[T]].shape, false)
    }

  private inline def variantShapesOf[Ts <: Tuple]: List[JValue] =
    inline erasedValue[Ts] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => summonInline[SchemaShape[t]].shape :: variantShapesOf[ts]
    }
}
