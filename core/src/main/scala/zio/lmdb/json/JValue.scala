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
package zio.lmdb.json

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonReader, JsonValueCodec, JsonWriter, readFromArray, writeToArray}

import java.time.Instant
import java.util.UUID
import scala.collection.immutable.ListMap
import scala.collection.mutable

/** Generic, JSON-shaped tagged value.
  *
  * Replaces the role previously played by `zio.json.ast.Json` after the migration to
  * jsoniter-scala. Each variant carries its own `LMDBCodecJson` (derived from the case class
  * definition), so collections can be narrowly typed: `[String, StringV]` for string-only,
  * `[String, LongV]` for integer-only, etc. A `[String, JValue]` collection accepts any variant
  * via the sum-type codec at the bottom of this file.
  *
  * Graph-native variants (`InstantV`, `IdentifierV`) live alongside the JSON primitives so the
  * upcoming L3 layer can express typed property bags without a second hierarchy. L5 will add
  * `IRIV` as an additive case when the semantic overlay loads. See
  * `docs/internal/EVOLUTION_PLAN_ITERATION_6.md` §6.5 for the design rationale.
  *
  * == Why the sum-type codec is hand-rolled ==
  *
  * `JsonCodecMaker.make[JValue]` (with `withAllowRecursiveTypes(true)`) hits a forward-reference
  * emit-order bug in jsoniter-scala 2.38 whenever the variant set contains both recursive cases
  * (`ListV[JValue]`, `MapV[String, JValue]`) AND more than one "special" string-encoded type
  * (`Instant`, `UUID`, `BigDecimal`). Bisection showed each subset works alone, but the full set
  * provokes `d0 is a forward reference extending over the definition of cN`.
  *
  * Reproduced under jsoniter-scala 2.38.14 on:
  *   - Scala 3.3.7    (LTS)
  *   - Scala 3.8.3    (latest stable as of 2026-06)
  *   - Scala 3.8.4-RC2
  *
  * A hand-rolled `JsonValueCodec[JValue]` sidesteps the macro entirely and keeps the same
  * `{"type":"VariantName","value":...}` wire format the macro would have emitted. Per-variant
  * case classes still use `derives LMDBCodecJson` for narrowly-typed collections; only the
  * sum-type entry point is hand-rolled.
  *
  * '''Future check.''' When upgrading Scala or jsoniter-scala, re-test by swapping the
  * `JValueValueCodec` definition below for the one-liner kept as `// MACRO-FUTURE-CHECK`
  * comments after it. If `core / compile` succeeds, the hand-rolled object and its imports
  * (`JsonReader`, `JsonWriter`, `ListMap`, `mutable`) can be deleted in favour of the macro
  * derivation. `JValueCodecSpec` covers the wire format and round-trip surface either way.
  */
sealed trait JValue

object JValue {
  /** A JSON string value. */
  final case class StringV(value: String) extends JValue derives LMDBCodecJson

  /** A 64-bit integer. */
  final case class LongV(value: Long) extends JValue derives LMDBCodecJson

  /** A double-precision floating-point number. */
  final case class DoubleV(value: Double) extends JValue derives LMDBCodecJson

  /** Arbitrary-precision decimal — useful when the application cares about exact decimal
    * arithmetic (money, scientific data). Round-trip preserves precision via jsoniter-scala's
    * `BigDecimal` codec.
    */
  final case class DecimalV(value: BigDecimal) extends JValue derives LMDBCodecJson

  /** A JSON boolean. */
  final case class BoolV(value: Boolean) extends JValue derives LMDBCodecJson

  /** ISO-8601 timestamp — first-class for time-typed properties (L3) and time-range scans (L4). */
  final case class InstantV(value: Instant) extends JValue derives LMDBCodecJson

  /** Reference to another record by UUID identifier (typically a UUIDv7 in the L3 graph layer). */
  final case class IdentifierV(value: UUID) extends JValue derives LMDBCodecJson

  /** A heterogeneous JSON array. */
  final case class ListV(value: Seq[JValue]) extends JValue

  /** A JSON object — string-keyed bag of values. The recursive shape supports nested property
    * bags, e.g. an L3 node whose property is itself a sub-document.
    */
  final case class MapV(value: Map[String, JValue]) extends JValue

  /** The JSON null literal. */
  case object NullV extends JValue

  // ── Hand-rolled sum-type codec ───────────────────────────────────────────────────────────────
  //
  // Wire format (same as `JsonCodecMaker.make`'s default would produce):
  //   {"type":"StringV","value":"abc"}
  //   {"type":"LongV","value":42}
  //   {"type":"ListV","value":[…]}
  //   {"type":"MapV","value":{…}}
  //   {"type":"NullV"}                          (no value field)
  //
  // The encoder writes the discriminator first, then the typed value. The decoder requires
  // "type" as the first key and dispatches on its string value before reading the (optional)
  // "value" field. A depth guard prevents stack overflow on adversarially deep input.

  private val MaxDepth: Int = 256

  private object JValueValueCodec extends JsonValueCodec[JValue] {
    override def nullValue: JValue = NullV

    override def encodeValue(x: JValue, out: JsonWriter): Unit = encodeAt(x, out, MaxDepth)

    private def encodeAt(x: JValue, out: JsonWriter, depth: Int): Unit = {
      if (depth <= 0) throw new IllegalStateException("JValue encode depth limit exceeded")
      out.writeObjectStart()
      out.writeKey("type")
      x match {
        case StringV(v) =>
          out.writeVal("StringV"); out.writeKey("value"); out.writeVal(v)
        case LongV(v) =>
          out.writeVal("LongV"); out.writeKey("value"); out.writeVal(v)
        case DoubleV(v) =>
          out.writeVal("DoubleV"); out.writeKey("value"); out.writeVal(v)
        case DecimalV(v) =>
          out.writeVal("DecimalV"); out.writeKey("value"); out.writeVal(v)
        case BoolV(v) =>
          out.writeVal("BoolV"); out.writeKey("value"); out.writeVal(v)
        case InstantV(v) =>
          out.writeVal("InstantV"); out.writeKey("value"); out.writeVal(v)
        case IdentifierV(v) =>
          out.writeVal("IdentifierV"); out.writeKey("value"); out.writeVal(v)
        case ListV(items) =>
          out.writeVal("ListV"); out.writeKey("value")
          out.writeArrayStart()
          val it = items.iterator
          while (it.hasNext) encodeAt(it.next(), out, depth - 1)
          out.writeArrayEnd()
        case MapV(entries) =>
          out.writeVal("MapV"); out.writeKey("value")
          out.writeObjectStart()
          val it = entries.iterator
          while (it.hasNext) {
            val (k, v) = it.next()
            out.writeKey(k)
            encodeAt(v, out, depth - 1)
          }
          out.writeObjectEnd()
        case NullV =>
          out.writeVal("NullV")
      }
      out.writeObjectEnd()
    }

    override def decodeValue(in: JsonReader, default: JValue): JValue = decodeAt(in, MaxDepth)

    private def decodeAt(in: JsonReader, depth: Int): JValue = {
      if (depth <= 0) in.decodeError("JValue decode depth limit exceeded")
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      if (in.isNextToken('}')) in.decodeError("expected discriminator field 'type'")
      in.rollbackToken()
      val firstKey = in.readKeyAsString()
      if (firstKey != "type") in.decodeError(s"expected first key 'type', got '$firstKey'")
      val tpe = in.readString(null)
      val result: JValue = tpe match {
        case "StringV"      => StringV(readValueAs(in, _.readString(null)))
        case "LongV"        => LongV(readValueAs(in, _.readLong()))
        case "DoubleV"      => DoubleV(readValueAs(in, _.readDouble()))
        case "DecimalV"     => DecimalV(readValueAs(in, _.readBigDecimal(null)))
        case "BoolV"        => BoolV(readValueAs(in, _.readBoolean()))
        case "InstantV"     => InstantV(readValueAs(in, _.readInstant(null)))
        case "IdentifierV"  => IdentifierV(readValueAs(in, _.readUUID(null)))
        case "ListV"        => ListV(readValueAs(in, r => readArray(r, depth - 1)))
        case "MapV"         => MapV(readValueAs(in, r => readMap(r, depth - 1)))
        case "NullV"        => NullV
        case other          => in.decodeError(s"unknown JValue discriminator '$other'"); null
      }
      if (!in.isNextToken('}')) in.decodeError("expected '}'")
      result
    }

    /** Reads the comma, the `"value"` key, and dispatches the body via `read`. */
    private inline def readValueAs[A](in: JsonReader, read: JsonReader => A): A = {
      if (!in.isNextToken(',')) in.decodeError("expected ',' before 'value'")
      val k = in.readKeyAsString()
      if (k != "value") in.decodeError(s"expected key 'value', got '$k'")
      read(in)
    }

    private def readArray(in: JsonReader, depth: Int): Seq[JValue] = {
      if (!in.isNextToken('[')) in.decodeError("expected '['")
      val buf = new mutable.ArrayBuffer[JValue]()
      if (!in.isNextToken(']')) {
        in.rollbackToken()
        buf += decodeAt(in, depth)
        while (in.isNextToken(',')) buf += decodeAt(in, depth)
        in.rollbackToken()
        if (!in.isNextToken(']')) in.decodeError("expected ']' or ','")
      }
      buf.toSeq
    }

    private def readMap(in: JsonReader, depth: Int): Map[String, JValue] = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      val builder = ListMap.newBuilder[String, JValue]
      if (!in.isNextToken('}')) {
        in.rollbackToken()
        val k = in.readKeyAsString()
        builder += k -> decodeAt(in, depth)
        while (in.isNextToken(',')) {
          val k2 = in.readKeyAsString()
          builder += k2 -> decodeAt(in, depth)
        }
        in.rollbackToken()
        if (!in.isNextToken('}')) in.decodeError("expected '}' or ','")
      }
      builder.result()
    }
  }

  /** Raw jsoniter codec exposed in implicit scope so that `JsonCodecMaker.make[T]` reuses it for
    * any `T` that has a `JValue` field, instead of inlining `JValue`'s recursive structure (which
    * would re-trigger the macro forward-reference bug documented above). This is jsoniter-scala's
    * documented "implicitly accessible codec" escape hatch for recursion.
    */
  given jValueValueCodec: JsonValueCodec[JValue] = JValueValueCodec

  given jValueCodec: LMDBCodecJson[JValue] = LMDBCodecJson(JValueValueCodec)

  // ── Plain-JSON view (for the L2C SQL engine) ─────────────────────────────────────────────────
  //
  // The codec above uses the {"type":…,"value":…} envelope. Stored *values* are plain JSON (the
  // jsoniter output of the value type), so the SQL engine reads/writes them as a generic JSON tree.
  // This lenient codec maps JSON object→MapV, array→ListV, string→StringV, integral number→LongV,
  // fractional number→DecimalV (precision-preserving), bool→BoolV, null→NullV. It is intentionally
  // separate from the envelope codec and is not exposed as a given (it would clash with it).

  private object PlainJValueCodec extends JsonValueCodec[JValue] {
    override def nullValue: JValue = NullV

    override def decodeValue(in: JsonReader, default: JValue): JValue = decodePlain(in, MaxDepth)

    private def decodePlain(in: JsonReader, depth: Int): JValue = {
      if (depth <= 0) in.decodeError("plain JValue decode depth limit exceeded")
      if (in.isNextToken('n')) in.readNullOrError(NullV, "expected a JSON value")
      else {
        in.rollbackToken()
        val t = in.nextToken()
        in.rollbackToken()
        t.toChar match {
          case '{'       => decodePlainObject(in, depth)
          case '['       => decodePlainArray(in, depth)
          case '"'       => StringV(in.readString(null))
          case 't' | 'f' => BoolV(in.readBoolean())
          case _         =>
            val bd = in.readBigDecimal(null)
            if (bd.scale <= 0 && bd.isValidLong) LongV(bd.toLong) else DecimalV(bd)
        }
      }
    }

    private def decodePlainArray(in: JsonReader, depth: Int): JValue = {
      if (!in.isNextToken('[')) in.decodeError("expected '['")
      val buf = new mutable.ArrayBuffer[JValue]()
      if (!in.isNextToken(']')) {
        in.rollbackToken()
        buf += decodePlain(in, depth - 1)
        while (in.isNextToken(',')) buf += decodePlain(in, depth - 1)
        in.rollbackToken()
        if (!in.isNextToken(']')) in.decodeError("expected ']' or ','")
      }
      ListV(buf.toSeq)
    }

    private def decodePlainObject(in: JsonReader, depth: Int): JValue = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      val builder = ListMap.newBuilder[String, JValue]
      if (!in.isNextToken('}')) {
        in.rollbackToken()
        builder += in.readKeyAsString() -> decodePlain(in, depth - 1)
        while (in.isNextToken(',')) builder += in.readKeyAsString() -> decodePlain(in, depth - 1)
        in.rollbackToken()
        if (!in.isNextToken('}')) in.decodeError("expected '}' or ','")
      }
      MapV(builder.result())
    }

    override def encodeValue(x: JValue, out: JsonWriter): Unit = encodePlain(x, out, MaxDepth)

    private def encodePlain(x: JValue, out: JsonWriter, depth: Int): Unit = {
      if (depth <= 0) throw new IllegalStateException("plain JValue encode depth limit exceeded")
      x match {
        case StringV(v)     => out.writeVal(v)
        case LongV(v)       => out.writeVal(v)
        case DoubleV(v)     => out.writeVal(v)
        case DecimalV(v)    => out.writeVal(v)
        case BoolV(v)       => out.writeVal(v)
        case InstantV(v)    => out.writeVal(v)
        case IdentifierV(v) => out.writeVal(v)
        case ListV(items)   =>
          out.writeArrayStart()
          val it = items.iterator
          while (it.hasNext) encodePlain(it.next(), out, depth - 1)
          out.writeArrayEnd()
        case MapV(entries)  =>
          out.writeObjectStart()
          val it = entries.iterator
          while (it.hasNext) { val (k, v) = it.next(); out.writeKey(k); encodePlain(v, out, depth - 1) }
          out.writeObjectEnd()
        case NullV          => out.writeNull()
      }
    }
  }

  /** Parse plain JSON bytes (as written by a value codec) into a generic [[JValue]] tree. Integral
    * numbers become `LongV`, fractional numbers `DecimalV` (precision-preserving). Used by the L2C
    * SQL engine to read stored values without their static type.
    */
  def fromPlainJson(bytes: Array[Byte]): Either[String, JValue] =
    try Right(readFromArray(bytes)(PlainJValueCodec))
    catch { case e: Throwable => Left(e.getMessage) }

  /** Serialize a [[JValue]] as plain JSON bytes (used by the SQL engine to write values back). */
  def toPlainJson(value: JValue): Array[Byte] = writeToArray(value)(PlainJValueCodec)

  // ── MACRO-FUTURE-CHECK ───────────────────────────────────────────────────────────────────────
  //
  // Re-test on each Scala / jsoniter-scala upgrade by uncommenting the block below (and adding
  // `import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}` at
  // the top of the file). Delete the hand-rolled path if it compiles.
  //
  // given jValueCodec: LMDBCodecJson[JValue] =
  //   LMDBCodecJson(JsonCodecMaker.make[JValue](CodecMakerConfig.withAllowRecursiveTypes(true)))
}
