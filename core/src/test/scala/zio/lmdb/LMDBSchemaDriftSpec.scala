/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package zio.lmdb

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.json.ast.Json
import zio.lmdb.json.LMDBCodecJson
import zio.lmdb.schema.{LMDBSchema, SchemaArtifact}
import zio.lmdb.StorageUserError.SchemaDrift

object LMDBSchemaDriftSpec extends ZIOSpecDefault with Commons {

  // Two distinct concrete schemas declared via LMDBSchema.from. We register them under
  // different opaque payload types so each test scope can pick which one wins.

  case class DocA(label: String) derives LMDBCodecJson
  case class DocB(label: String) derives LMDBCodecJson

  given LMDBSchema[DocA] = LMDBSchema.from(SchemaArtifact.JsonSchema(Json.Str("DocA/v1")))
  given LMDBSchema[DocB] = LMDBSchema.from(SchemaArtifact.JsonSchema(Json.Str("DocB/v1")))

  val spec = suite("LMDBSchemaDriftSpec")(
    test("collectionGet succeeds when caller's schema matches the persisted one") {
      for {
        _   <- LMDB.collectionCreate[String, DocA]("drift_match")
        got <- LMDB.collectionGet[String, DocA]("drift_match")
      } yield assertTrue(got.name == "drift_match")
    },
    test("collectionGet fails with SchemaDrift when caller's value schema differs") {
      for {
        _    <- LMDB.collectionCreate[String, DocA]("drift_value")
        exit <- LMDB.collectionGet[String, DocB]("drift_value").exit
        side  = exit match {
          case Exit.Failure(cause) => cause.failures.collectFirst { case s: SchemaDrift => s.side }.getOrElse("<other>")
          case _                   => "<success>"
        }
      } yield assert(exit)(failsWithA[SchemaDrift]) &&
        assertTrue(side == "value")
    },
    test("multiGet enforces drift on value schema") {
      for {
        _    <- LMDB.multiCreate[String, DocA]("drift_multi")
        exit <- LMDB.multiGet[String, DocB]("drift_multi").exit
      } yield assert(exit)(failsWithA[SchemaDrift])
    },
    test("indexGet enforces drift on toKey schema (= value-schema slot)") {
      // We use two distinct schemas that resolve to different fingerprints under the same
      // KeyCodec wiring. fromKey stays the same so only the toKey side trips drift.
      given LMDBSchema[String] = LMDBSchema.from(SchemaArtifact.OpaqueSchema("string-default"))
      val fromConcrete         = LMDBSchema.from[String](SchemaArtifact.JsonSchema(Json.Str("FromKey/v1")))
      val toConcreteA          = LMDBSchema.from[String](SchemaArtifact.JsonSchema(Json.Str("ToKey/vA")))
      val toConcreteB          = LMDBSchema.from[String](SchemaArtifact.JsonSchema(Json.Str("ToKey/vB")))
      for {
        // Create with the A pair
        _    <- ZIO.serviceWithZIO[LMDB](_.indexCreate[String, String]("drift_idx")(using summon, summon, fromConcrete, toConcreteA))
        // Re-open with the B pair on the to-key side
        exit <- ZIO.serviceWithZIO[LMDB](_.indexGet[String, String]("drift_idx")(using summon, summon, fromConcrete, toConcreteB)).exit
        side  = exit match {
          case Exit.Failure(cause) => cause.failures.collectFirst { case s: SchemaDrift => s.side }.getOrElse("<other>")
          case _                   => "<success>"
        }
      } yield assert(exit)(failsWithA[SchemaDrift]) &&
        assertTrue(side == "toKey")
    },
    test("opaque caller schema does not trigger drift even against a concrete persisted one") {
      // First create with a concrete value schema, then open with the default opaque fallback.
      // The opaque fallback must be permissive — never raise drift.
      case class DocC(label: String) derives LMDBCodecJson // no explicit LMDBSchema given → opaque
      for {
        _   <- LMDB.collectionCreate[String, DocA]("drift_opaque_caller")
        got <- LMDB.collectionGet[String, DocC]("drift_opaque_caller")
      } yield assertTrue(got.name == "drift_opaque_caller")
    },
    test("opaque persisted schema does not trigger drift against a concrete caller schema") {
      // Untyped allocate leaves keySchema = valueSchema = None on the persisted side.
      // None must not raise drift on subsequent typed get.
      for {
        _   <- LMDB.collectionAllocate("drift_opaque_persisted")
        got <- LMDB.collectionGet[String, DocA]("drift_opaque_persisted")
      } yield assertTrue(got.name == "drift_opaque_persisted")
    }
  ).provideLayerShared(lmdbLayer)
}
