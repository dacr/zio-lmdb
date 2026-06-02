/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package zio.lmdb.schema

import zio.json.*
import zio.json.ast.Json

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

  /** Schema for codecs derived from zio-json: an opaque JSON document that downstream tools can
    * inspect (e.g. the L4 GQL REPL surfacing property names and inferred types).
    */
  case class JsonSchema(schema: Json) extends SchemaArtifact

  /** Protobuf `.proto` source text describing the message used by the codec. */
  case class ProtobufSchema(proto: String) extends SchemaArtifact

  /** Fallback for codecs that cannot expose a schema. The `hint` is a free-form identifier (e.g.
    * the codec class name) used purely for diagnostics; it does not contribute to drift detection
    * in a way callers should rely on across releases.
    */
  case class OpaqueSchema(hint: String) extends SchemaArtifact

  given JsonCodec[SchemaArtifact] = DeriveJsonCodec.gen[SchemaArtifact]

  /** SHA-256 of the canonical JSON form. Stable across JVM versions and platforms because zio-json's
    * compact encoding is order-preserving for case-class fields and `Json` ast objects.
    */
  private def fingerprintOf(a: SchemaArtifact): String = {
    val sha   = MessageDigest.getInstance("SHA-256")
    val bytes = a.toJson.getBytes("UTF-8")
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
