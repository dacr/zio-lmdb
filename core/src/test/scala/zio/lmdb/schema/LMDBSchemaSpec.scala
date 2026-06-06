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
import zio.lmdb.keycodecs.KeyCodec
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
    },
    test("every key type resolves to a KeySchema carrying its codec's id") {
      def keyId(s: LMDBSchema[?]): Option[String] = s.artifact match {
        case SchemaArtifact.KeySchema(id) => Some(id)
        case _                            => None
      }
      assertTrue(
        keyId(LMDBSchema[String]).contains("lmdb:str"),
        keyId(LMDBSchema[Long]).contains("lmdb:int64"),
        keyId(LMDBSchema[Int]).contains("lmdb:int32"),
        keyId(LMDBSchema[Short]).contains("lmdb:int16"),
        keyId(LMDBSchema[java.util.UUID]).contains("lmdb:uuid")
      )
    },
    test("byte-compatible but distinct key types get distinct schemas (the gap this closes)") {
      // Int (4 bytes) and Long (8 bytes) collapsed to {"type":"integer"} under the old shape-based
      // schema and shared a fingerprint; now their codec ids keep them apart.
      assertTrue(
        LMDBSchema[Int].artifact != LMDBSchema[Long].artifact,
        LMDBSchema[Int].artifact.fingerprint != LMDBSchema[Long].artifact.fingerprint
      )
    },
    test("a key schema is sourced from the codec's keyId") {
      assertTrue(
        LMDBSchema[Long].artifact == (SchemaArtifact.KeySchema(summon[KeyCodec[Long]].keyId.value): SchemaArtifact)
      )
    },
    test("a tuple key composes its components' ids") {
      LMDBSchema[(String, Int)].artifact match {
        case SchemaArtifact.KeySchema(id) =>
          assertTrue(id == summon[KeyCodec[(String, Int)]].keyId.value, id == "lmdb:tuple(lmdb:str,lmdb:int32)")
        case other                        => assertNever(s"expected a KeySchema, got $other")
      }
    }
  )
}
