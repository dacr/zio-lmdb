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
package zio.lmdb

import zio.*
import zio.test.*
import zio.lmdb.json.*
import zio.lmdb.json.JValue.{MapV, StringV}
import zio.lmdb.schema.{LMDBSchema, SchemaArtifact}

import java.time.Instant

/** Worked example: define a small domain model, store it in a typed collection, and observe that the collection's value schema is automatically derived from the Scala model and persisted in the metadata catalog — ready for drift detection and
  * inspection.
  *
  * Two `derives` clauses are all the model needs:
  *   - `LMDBCodecJson` — JSON (de)serialization used to read/write records.
  *   - `LMDBSchema` — structural schema captured once, on `collectionCreate`.
  *
  * Nested types (`LineItem`) require no ceremony: the codec inlines them and the schema describes them structurally.
  */
object LMDBDomainModelExampleSpec extends ZIOSpecDefault with Commons {

  final case class LineItem(sku: String, quantity: Int, unitPrice: BigDecimal)

  final case class Order(
    reference: String,
    customer: String,
    items: List[LineItem],
    note: Option[String],
    placedAt: Instant
  ) derives LMDBCodecJson,
        LMDBSchema

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  /** Navigate a derived `JsonSchema` artifact to read one property's JSON `type` tag. */
  private def propertyType(schema: SchemaArtifact, field: String): Option[String] =
    schema match {
      case SchemaArtifact.JsonSchema(MapV(root)) =>
        root
          .get("properties")
          .collect { case MapV(props) => props }
          .flatMap(_.get(field))
          .collect { case MapV(descr) => descr }
          .flatMap(_.get("type"))
          .collect { case StringV(t) => t }
      case _                                     => None
    }

  override def spec = suite("LMDB domain-model usage example")(
    test("store and retrieve a domain model whose schema is auto-derived and persisted") {
      for {
        // 1. Create a typed collection. Order's schema is derived and persisted here.
        orders <- LMDB.collectionCreate[String, Order]("orders")

        // 2. Store a couple of orders (insert refuses an existing key; upsertOverwrite replaces).
        first  = Order(
                   reference = "ORD-1001",
                   customer = "Alice",
                   items = List(LineItem("SKU-1", 2, BigDecimal("9.99")), LineItem("SKU-2", 1, BigDecimal("19.50"))),
                   note = Some("gift wrap"),
                   placedAt = Instant.parse("2026-06-04T10:15:30Z")
                 )
        second = Order(
                   reference = "ORD-1002",
                   customer = "Bob",
                   items = List(LineItem("SKU-3", 5, BigDecimal("4.00"))),
                   note = None,
                   placedAt = Instant.parse("2026-06-04T11:00:00Z")
                 )
        _     <- orders.insert(first.reference, first)
        _     <- orders.upsertOverwrite(second.reference, second)

        // 3. Basic reads: full round-trip, existence, count.
        fetched <- orders.fetch("ORD-1001").some
        exists  <- orders.contains("ORD-1002")
        count   <- orders.size()

        // 4. Inspect the schema the catalog captured for this collection.
        cfg   <- ZIO.config(LMDB.config)
        meta  <- LMDB.collectionGet[String, MetaDataEntry](cfg.metaDataCollectionName)
        entry <- meta.fetch("orders").some

        // The String key uses the permissive opaque fallback; the value carries the real schema.
        keyIsOpaque = entry.keySchema.exists(_.isInstanceOf[SchemaArtifact.OpaqueSchema])
      } yield assertTrue(
        // the model round-trips intact: nested items, Option and Instant included
        fetched == first,
        fetched.items.head.unitPrice == BigDecimal("9.99"),
        fetched.note.contains("gift wrap"),
        exists,
        count == 2L,
        // the persisted value schema is exactly the one derived from the Scala model
        entry.valueSchema.contains(LMDBSchema[Order].artifact),
        // and it is introspectable: each field maps to a JSON type
        propertyType(entry.valueSchema.get, "customer").contains("string"),
        propertyType(entry.valueSchema.get, "items").contains("array"),
        propertyType(entry.valueSchema.get, "placedAt").contains("string"),
        keyIsOpaque
      )
    }
  ).provide(lmdbLayer)
}
