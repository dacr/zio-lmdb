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

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonReader, JsonValueCodec, JsonWriter, writeToString}
import zio.lmdb.json.{JValue, LMDBCodecJson}

import java.security.MessageDigest

/** Describes the shape of values stored in a collection. The artifact is persisted alongside the
  * collection metadata (`MetaDataEntry`) and is the source of truth used by L2A's drift detection
  * and by L3 / L5 for higher-level catalog needs.
  *
  * The artifact's [[fingerprint]] is a stable SHA-256 hex digest of its canonical JSON form.
  */
sealed trait SchemaArtifact {
  def fingerprint: String = SchemaArtifact.fingerprintOf(this)
}

object SchemaArtifact {

  /** Schema for codecs derived from the JSON layer: a generic JSON document (a [[JValue]] tree)
    * that downstream tools can inspect (e.g. the L4 GQL REPL surfacing property names and inferred
    * types).
    */
  case class JsonSchema(schema: JValue) extends SchemaArtifact

  /** Protobuf `.proto` source text describing the message used by the codec. */
  case class ProtobufSchema(proto: String) extends SchemaArtifact

  /** Fallback for codecs that cannot expose a schema. The `hint` is a free-form identifier (e.g.
    * the codec class name) used purely for diagnostics; it does not contribute to drift detection
    * in a way callers should rely on across releases.
    */
  case class OpaqueSchema(hint: String) extends SchemaArtifact

  /** Identity of the `KeyCodec` that encodes a collection's key, recorded as the codec's stable
    * `keyId` (e.g. `lmdb:int64`, `lmdb-geo:location/v1`). Keys are order-encoded bytes whose meaning
    * cannot be inferred from the bytes alone, so this names exactly which codec produced them — which
    * is what lets drift detection tell an `Int` key from a `Long` one, and what a reader (the future
    * L2C REPL) needs to decode a key without guessing.
    */
  case class KeySchema(keyId: String) extends SchemaArtifact

  // ── Hand-rolled sum-type codec ───────────────────────────────────────────────────────────────
  //
  // `JsonCodecMaker.make[SchemaArtifact]` cannot be used: jsoniter-scala inlines nested codecs
  // structurally rather than delegating to the in-scope `JsonValueCodec[JValue]`, so the macro
  // would recurse into `JValue` and re-trigger the forward-reference emit-order bug that forced
  // `JValue` itself to be hand-rolled (see `zio.lmdb.json.JValue`). This codec dispatches on a
  // `"type"` discriminator and delegates the `JsonSchema` payload to `JValue`'s own codec, keeping
  // the same `{"type":"VariantName","value":...}` wire format the macro would have produced.

  private val jValueCodec: JsonValueCodec[JValue] = JValue.jValueCodec.valueCodec

  private object SchemaArtifactValueCodec extends JsonValueCodec[SchemaArtifact] {
    override def nullValue: SchemaArtifact = null

    override def encodeValue(x: SchemaArtifact, out: JsonWriter): Unit = {
      out.writeObjectStart()
      out.writeKey("type")
      x match {
        case JsonSchema(schema) =>
          out.writeVal("JsonSchema"); out.writeKey("value"); jValueCodec.encodeValue(schema, out)
        case ProtobufSchema(proto) =>
          out.writeVal("ProtobufSchema"); out.writeKey("value"); out.writeVal(proto)
        case OpaqueSchema(hint) =>
          out.writeVal("OpaqueSchema"); out.writeKey("value"); out.writeVal(hint)
        case KeySchema(keyId) =>
          out.writeVal("KeySchema"); out.writeKey("value"); out.writeVal(keyId)
      }
      out.writeObjectEnd()
    }

    override def decodeValue(in: JsonReader, default: SchemaArtifact): SchemaArtifact = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      val firstKey = in.readKeyAsString()
      if (firstKey != "type") in.decodeError(s"expected first key 'type', got '$firstKey'")
      val tpe = in.readString(null)
      if (!in.isNextToken(',')) in.decodeError("expected ',' before 'value'")
      val valueKey = in.readKeyAsString()
      if (valueKey != "value") in.decodeError(s"expected key 'value', got '$valueKey'")
      val result: SchemaArtifact = tpe match {
        case "JsonSchema"     => JsonSchema(jValueCodec.decodeValue(in, JValue.NullV))
        case "ProtobufSchema" => ProtobufSchema(in.readString(null))
        case "OpaqueSchema"   => OpaqueSchema(in.readString(null))
        case "KeySchema"      => KeySchema(in.readString(null))
        case other            => in.decodeError(s"unknown SchemaArtifact discriminator '$other'"); null
      }
      if (!in.isNextToken('}')) in.decodeError("expected '}'")
      result
    }
  }

  /** Raw jsoniter codec exposed in implicit scope so that `JsonCodecMaker.make[T]` (e.g. for
    * `MetaDataEntry`) reuses it for nested `SchemaArtifact` fields instead of inlining the
    * recursive `JValue` it transitively contains.
    */
  given JsonValueCodec[SchemaArtifact] = SchemaArtifactValueCodec

  given LMDBCodecJson[SchemaArtifact] = LMDBCodecJson(SchemaArtifactValueCodec)

  /** SHA-256 of the canonical JSON form. Stable across JVM versions and platforms because the
    * hand-rolled codec emits fields in a fixed order.
    */
  private def fingerprintOf(a: SchemaArtifact): String = {
    val sha   = MessageDigest.getInstance("SHA-256")
    val bytes = writeToString(a)(SchemaArtifactValueCodec).getBytes("UTF-8")
    val out   = sha.digest(bytes)
    val sb    = new StringBuilder(out.length * 2)
    var i     = 0
    while (i < out.length) {
      sb.append(f"${out(i)}%02x")
      i += 1
    }
    sb.toString
  }
}
