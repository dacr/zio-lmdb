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
import zio.test.*
import zio.test.Assertion.*

object SchemaArtifactSpec extends ZIOSpecDefault {

  val spec = suite("SchemaArtifact")(
    test("JsonSchema fingerprint is stable across calls") {
      val a = SchemaArtifact.JsonSchema(Json.Obj("name" -> Json.Str("Person")))
      val b = SchemaArtifact.JsonSchema(Json.Obj("name" -> Json.Str("Person")))
      assertTrue(a.fingerprint == b.fingerprint, a.fingerprint.length == 64)
    },
    test("changing the JsonSchema content changes the fingerprint") {
      val a = SchemaArtifact.JsonSchema(Json.Obj("name" -> Json.Str("Person")))
      val b = SchemaArtifact.JsonSchema(Json.Obj("name" -> Json.Str("Employee")))
      assertTrue(a.fingerprint != b.fingerprint)
    },
    test("ProtobufSchema fingerprint reflects the .proto source text") {
      val a = SchemaArtifact.ProtobufSchema("message Person { string name = 1; }")
      val b = SchemaArtifact.ProtobufSchema("message Person { string name = 1; }")
      val c = SchemaArtifact.ProtobufSchema("message Person { string name = 2; }")
      assertTrue(a.fingerprint == b.fingerprint, a.fingerprint != c.fingerprint)
    },
    test("OpaqueSchema fingerprint depends on the hint") {
      val a = SchemaArtifact.OpaqueSchema("foo")
      val b = SchemaArtifact.OpaqueSchema("bar")
      assertTrue(a.fingerprint != b.fingerprint)
    },
    test("Fingerprints differ across artifact variants for the same payload") {
      val js = SchemaArtifact.JsonSchema(Json.Str("x"))
      val op = SchemaArtifact.OpaqueSchema("x")
      assertTrue(js.fingerprint != op.fingerprint)
    },
    test("JSON roundtrip preserves the artifact and its fingerprint") {
      val original: SchemaArtifact = SchemaArtifact.JsonSchema(Json.Obj("k" -> Json.Num(BigDecimal(1))))
      val encoded                  = original.toJson
      val decoded                  = encoded.fromJson[SchemaArtifact]
      assertTrue(decoded == Right(original)) &&
      assert(decoded.map(_.fingerprint))(isRight(equalTo(original.fingerprint)))
    }
  )
}
