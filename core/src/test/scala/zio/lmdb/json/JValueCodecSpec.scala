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

import com.github.plokhotnyuk.jsoniter_scala.core.{readFromString, writeToString}
import zio.lmdb.json.JValue.*
import zio.test.*
import zio.test.Assertion.*

import java.time.Instant
import java.util.UUID

object JValueCodecSpec extends ZIOSpecDefault {

  private final case class Profile(id: String, name: Option[String], age: Option[Int]) derives LMDBCodecJson

  private def roundTrip(v: JValue): JValue = {
    val codec = JValue.jValueCodec.valueCodec
    readFromString[JValue](writeToString[JValue](v)(codec))(codec)
  }

  def spec = suite("JValueCodec")(
    test("StringV round-trips") {
      val v = StringV("hello world")
      assertTrue(roundTrip(v) == v)
    },
    test("LongV round-trips") {
      val v = LongV(42L)
      assertTrue(roundTrip(v) == v)
    },
    test("DoubleV round-trips") {
      val v = DoubleV(3.14)
      assertTrue(roundTrip(v) == v)
    },
    test("DecimalV round-trips") {
      val v = DecimalV(BigDecimal("123456789.987654321"))
      assertTrue(roundTrip(v) == v)
    },
    test("BoolV round-trips") {
      assertTrue(roundTrip(BoolV(true)) == BoolV(true)) &&
      assertTrue(roundTrip(BoolV(false)) == BoolV(false))
    },
    test("InstantV round-trips") {
      val v = InstantV(Instant.parse("2026-06-02T10:00:00Z"))
      assertTrue(roundTrip(v) == v)
    },
    test("IdentifierV round-trips") {
      val v = IdentifierV(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"))
      assertTrue(roundTrip(v) == v)
    },
    test("NullV round-trips") {
      assertTrue(roundTrip(NullV) == NullV)
    },
    test("empty ListV round-trips") {
      val v = ListV(Seq.empty)
      val rt = roundTrip(v)
      assertTrue(rt.asInstanceOf[ListV].value.isEmpty)
    },
    test("ListV with mixed primitives round-trips") {
      val v = ListV(Seq(StringV("a"), LongV(1L), BoolV(true), NullV))
      val rt = roundTrip(v).asInstanceOf[ListV]
      assertTrue(rt.value.toList == List(StringV("a"), LongV(1L), BoolV(true), NullV))
    },
    test("empty MapV round-trips") {
      val v = MapV(Map.empty)
      val rt = roundTrip(v)
      assertTrue(rt.asInstanceOf[MapV].value.isEmpty)
    },
    test("MapV with primitives round-trips") {
      val v = MapV(Map("name" -> StringV("alice"), "age" -> LongV(30L)))
      val rt = roundTrip(v).asInstanceOf[MapV]
      assertTrue(rt.value("name") == StringV("alice")) &&
      assertTrue(rt.value("age") == LongV(30L))
    },
    test("nested ListV inside MapV round-trips") {
      val v = MapV(Map(
        "tags" -> ListV(Seq(StringV("scala"), StringV("zio"))),
        "id"   -> LongV(7L)
      ))
      val rt = roundTrip(v).asInstanceOf[MapV]
      assertTrue(rt.value("tags").asInstanceOf[ListV].value.toList == List(StringV("scala"), StringV("zio"))) &&
      assertTrue(rt.value("id") == LongV(7L))
    },
    test("deeply nested MapV round-trips") {
      val v = MapV(Map(
        "outer" -> MapV(Map(
          "inner" -> MapV(Map(
            "leaf" -> StringV("found")
          ))
        ))
      ))
      val rt = roundTrip(v)
      assertTrue(rt == v)
    },
    test("MapV containing IdentifierV and InstantV round-trips") {
      val id = UUID.fromString("550e8400-e29b-41d4-a716-446655440001")
      val ts = Instant.parse("2026-06-02T12:34:56Z")
      val v  = MapV(Map(
        "id"        -> IdentifierV(id),
        "createdAt" -> InstantV(ts),
        "score"     -> DecimalV(BigDecimal("99.99"))
      ))
      val rt = roundTrip(v).asInstanceOf[MapV]
      assertTrue(rt.value("id") == IdentifierV(id)) &&
      assertTrue(rt.value("createdAt") == InstantV(ts)) &&
      assertTrue(rt.value("score") == DecimalV(BigDecimal("99.99")))
    },
    test("wire format uses {\"type\":\"<Variant>\",\"value\":…}") {
      val codec = JValue.jValueCodec.valueCodec
      val json  = writeToString[JValue](StringV("x"))(codec)
      assertTrue(json == """{"type":"StringV","value":"x"}""")
    },
    test("NullV wire format has no value field") {
      val codec = JValue.jValueCodec.valueCodec
      val json  = writeToString[JValue](NullV)(codec)
      assertTrue(json == """{"type":"NullV"}""")
    },
    test("Option fields: None is omitted, Some is included") {
      val codec = summon[LMDBCodecJson[Profile]].valueCodec
      val json1 = writeToString(Profile("u1", None, None))(codec)
      val json2 = writeToString(Profile("u1", Some("alice"), None))(codec)
      val json3 = writeToString(Profile("u1", Some("alice"), Some(30)))(codec)
      assertTrue(json1 == """{"id":"u1"}""") &&
      assertTrue(json2 == """{"id":"u1","name":"alice"}""") &&
      assertTrue(json3 == """{"id":"u1","name":"alice","age":30}""")
    },
    test("Option fields: round-trip preserves None / Some") {
      val codec = summon[LMDBCodecJson[Profile]].valueCodec
      val p1 = Profile("u1", None, None)
      val p2 = Profile("u1", Some("alice"), Some(30))
      val p3 = readFromString[Profile]("""{"id":"u1"}""")(codec)
      val p4 = readFromString[Profile]("""{"id":"u1","name":"alice","age":30}""")(codec)
      assertTrue(p3 == p1) && assertTrue(p4 == p2)
    },
    test("fromPlainJson parses a generic JSON document (object, integral vs fractional, array, null)") {
      val parsed = JValue.fromPlainJson("""{"name":"Alice","age":30,"price":9.99,"tags":["a","b"],"note":null,"ok":true}""".getBytes("UTF-8"))
      assertTrue(
        parsed == Right(
          MapV(scala.collection.immutable.ListMap(
            "name"  -> StringV("Alice"),
            "age"   -> LongV(30),
            "price" -> DecimalV(BigDecimal("9.99")),
            "tags"  -> ListV(Seq(StringV("a"), StringV("b"))),
            "note"  -> NullV,
            "ok"    -> BoolV(true)
          ))
        )
      )
    },
    test("toPlainJson emits a plain JSON document that fromPlainJson reads back") {
      val tree = MapV(scala.collection.immutable.ListMap("k" -> LongV(1), "s" -> StringV("x")))
      val json = new String(JValue.toPlainJson(tree), "UTF-8")
      assertTrue(json == """{"k":1,"s":"x"}""", JValue.fromPlainJson(json.getBytes("UTF-8")) == Right(tree))
    }
  )
}
