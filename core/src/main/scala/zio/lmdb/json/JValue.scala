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

import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

import java.time.Instant
import java.util.UUID

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
  * == Deferred: recursive members (`ListV`, `MapV`) ==
  *
  * The iter‑6 §6.5 sketch includes `case class ListV(value: Vector[JValue])` and
  * `case class MapV(value: Map[String, JValue])` as the JSON-shaped container variants.
  * jsoniter-scala 2.38 + Scala 3.3.7 currently generate macro output with internal forward
  * references (`d0 is a forward reference extending over the definition of c19`) when deriving a
  * sum-type codec over a recursive case. The workaround is either a hand-rolled
  * `JsonValueCodec[JValue]` or the optional `zio-lmdb-codec-json-circe` bridge module from §6.5,
  * neither of which is needed by the current test surface (no test stores `Json.Obj` / `Json.Arr`
  * values).
  *
  * The recursive variants are deferred until either (a) the upstream jsoniter-scala issue is
  * resolved or (b) L3 implementation lands and a hand-rolled codec becomes a required deliverable.
  * Tracking this against open question §14.4 of iter 6.
  */
sealed trait JValue

object JValue {
  /** A JSON string value. */
  case class StringV(value: String) extends JValue derives LMDBCodecJson

  /** A 64-bit integer. */
  case class LongV(value: Long) extends JValue derives LMDBCodecJson

  /** A double-precision floating-point number. */
  case class DoubleV(value: Double) extends JValue derives LMDBCodecJson

  /** Arbitrary-precision decimal — useful when the application cares about exact decimal
    * arithmetic (money, scientific data). Round-trip preserves precision via jsoniter-scala's
    * `BigDecimal` codec.
    */
  case class DecimalV(value: BigDecimal) extends JValue derives LMDBCodecJson

  /** A JSON boolean. */
  case class BoolV(value: Boolean) extends JValue derives LMDBCodecJson

  /** ISO-8601 timestamp — first-class for time-typed properties (L3) and time-range scans (L4). */
  case class InstantV(value: Instant) extends JValue derives LMDBCodecJson

  /** Reference to another record by UUID identifier (typically a UUIDv7 in the L3 graph layer). */
  case class IdentifierV(value: UUID) extends JValue derives LMDBCodecJson

  /** The JSON null literal. */
  case object NullV extends JValue

  // ── Codec for the sum type ───────────────────────────────────────────────────────────────────
  //
  // Single `LMDBCodecJson[JValue]` that handles every variant via the default jsoniter-scala
  // discriminator (`{"type":"LongV","value":42}`). User code that needs a generic collection —
  // `LMDB.collectionCreate[String, JValue](...)` — uses this codec.

  given jValueCodec: LMDBCodecJson[JValue] = LMDBCodecJson(JsonCodecMaker.make[JValue])
}
