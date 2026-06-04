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

import zio.lmdb.json.JValue.StringV
import zio.test.*
import zio.test.Assertion.*

object LMDBSchemaSpec extends ZIOSpecDefault {

  case class Foo(x: Int)

  // Explicit schema for Foo — should win over the opaque fallback when both are visible.
  given LMDBSchema[Foo] = LMDBSchema.from(SchemaArtifact.JsonSchema(StringV("Foo")))

  case class Bar(y: String)

  val spec = suite("LMDBSchema")(
    test("explicit given takes precedence over the opaque fallback") {
      val s = LMDBSchema[Foo].artifact
      assert(s)(equalTo(SchemaArtifact.JsonSchema(StringV("Foo")): SchemaArtifact))
    },
    test("opaque fallback applies when no explicit given is in scope") {
      val s = LMDBSchema[Bar].artifact
      s match {
        case SchemaArtifact.OpaqueSchema(_) => assertCompletes
        case other                           => assertNever(s"expected OpaqueSchema, got $other")
      }
    },
    test("from() produces a schema whose fingerprint matches the underlying artifact") {
      val a = SchemaArtifact.OpaqueSchema("Custom")
      val s = LMDBSchema.from[Foo](a)
      assertTrue(s.artifact.fingerprint == a.fingerprint)
    }
  )
}
