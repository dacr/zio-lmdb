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

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString, writeToString}
import zio.lmdb.json.JValue
import zio.lmdb.json.JValue.{ListV, MapV, StringV}
import zio.test.*

object SchemaShapeSpec extends ZIOSpecDefault {

  final case class Address(street: String, zip: Int)

  final case class Person(
    id: java.util.UUID,
    name: String,
    age: Int,
    nickname: Option[String],
    tags: Seq[String],
    address: Address,
    createdAt: java.time.Instant
  ) derives LMDBSchema

  enum Color derives LMDBSchema {
    case Red, Green, Blue
  }

  private def asObj(jv: JValue): Map[String, JValue]   = jv.asInstanceOf[MapV].value
  private def propsOf(jv: JValue): Map[String, JValue] = asObj(jv)("properties").asInstanceOf[MapV].value
  private def str(name: String): JValue                = MapV(Map("type" -> StringV(name)))

  val spec = suite("SchemaShape")(
    test("case class derives an object schema with typed properties") {
      val s = SchemaShape[Person].shape
      val p = propsOf(s)
      assertTrue(
        asObj(s)("type") == StringV("object"),
        p("name") == str("string"),
        p("age") == str("integer"),
        p("id") == MapV(Map("type" -> StringV("string"), "format" -> StringV("uuid"))),
        p("createdAt") == MapV(Map("type" -> StringV("string"), "format" -> StringV("date-time")))
      )
    },
    test("Seq field becomes an array schema") {
      val p = propsOf(SchemaShape[Person].shape)
      assertTrue(p("tags") == MapV(Map("type" -> StringV("array"), "items" -> str("string"))))
    },
    test("nested case class becomes a nested object schema") {
      val addr = propsOf(SchemaShape[Person].shape)("address")
      assertTrue(asObj(addr)("type") == StringV("object"), propsOf(addr)("zip") == str("integer"))
    },
    test("Option field is present but excluded from required; non-Option fields are required") {
      val s        = SchemaShape[Person].shape
      val p        = propsOf(s)
      val required = asObj(s)("required").asInstanceOf[ListV].value.toSet
      assertTrue(
        p.contains("nickname"),
        p("nickname") == str("string"),
        !required.contains(StringV("nickname")),
        required.contains(StringV("name")),
        required.contains(StringV("age"))
      )
    },
    test("Map[String, V] becomes an object schema with additionalProperties") {
      assertTrue(
        SchemaShape[Map[String, Int]].shape ==
          MapV(Map("type" -> StringV("object"), "additionalProperties" -> str("integer")))
      )
    },
    test("sealed/enum derives a oneOf schema") {
      val s = SchemaShape[Color].shape
      assertTrue(asObj(s).contains("oneOf"), asObj(s)("oneOf").asInstanceOf[ListV].value.size == 3)
    },
    test("derives LMDBSchema yields a JsonSchema artifact, not opaque") {
      LMDBSchema[Person].artifact match {
        case SchemaArtifact.JsonSchema(_) => assertCompletes
        case other                        => assertNever(s"expected JsonSchema, got $other")
      }
    },
    test("a derived schema round-trips through its codec with a stable fingerprint") {
      val artifact = LMDBSchema[Person].artifact
      val codec    = summon[JsonValueCodec[SchemaArtifact]]
      val decoded  = readFromString[SchemaArtifact](writeToString(artifact)(codec))(codec)
      assertTrue(decoded == artifact, decoded.fingerprint == artifact.fingerprint)
    }
  )
}
