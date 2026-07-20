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

import zio.lmdb.json.LMDBCodecJson
import zio.lmdb.schema.SchemaArtifact

enum CollectionKind derives LMDBCodecJson {
  case Regular
  case Index
  case Multi
}

/** Declarative origin of one component of an index key, persisted so tools (e.g. the SQL query planner) can relate an index to the source records it was built from without access to the extraction function.
  */
enum IndexComponentSource derives LMDBCodecJson {

  /** A dotted JSON path into the source record's value (e.g. `timestamp`, `location.latitude`). */
  case Field(path: String)

  /** The source record's own key. */
  case PrimaryKey

  /** The first non-null of several value field paths (e.g. an explicit id falling back to an inferred one). */
  case Coalesce(paths: List[String])

  /** A component computed by logic that has no declarative form; `hint` is purely documentary. Planners must not use predicates against this component.
    */
  case Opaque(hint: String)
}

/** One component of a (possibly composite) index key: where its value comes from, the `keyId` of the `KeyCodec` that encodes it, and that codec's fixed byte width (`None` = variable width). Recording the width lets byte-level consumers
  * (`TupleKeyLayout`) split and bound composite keys even when a component uses an application-specific codec they cannot resolve by id.
  */
case class IndexComponent(source: IndexComponentSource, keyId: String, fixedWidth: Option[Int] = None) derives LMDBCodecJson

/** Declarative description of an index: which collection it indexes and how each component of its `FROM_KEY` and `TO_KEY` is derived from a source record. Persisted in the index's [[MetaDataEntry]] by `LMDBCollection.withDeclaredIndex`; consumed by
  * the SQL query planner to turn WHERE/ORDER BY clauses into index range scans.
  */
case class IndexMapping(
  sourceCollection: String,
  fromComponents: List[IndexComponent],
  toComponents: List[IndexComponent]
) derives LMDBCodecJson

/** Persistent description of a collection / multi-collection / index.
  *
  * Stored in the metadata sub-collection by `LMDBLive`. Carries the key and value schemas as `SchemaArtifact`s so that L2A's drift detection can compare a caller's expected schema with what was first persisted; later layers (L3 property catalog, L5
  * IRI bindings) re-use the same mechanism for per-property and per-IRI bindings.
  *
  * `layoutVersion` is the version of this metadata record itself. Bumping it allows graceful migrations of the catalog format in future releases.
  */
case class MetaDataEntry(
  collectionName: String,
  collectionKind: CollectionKind,
  layoutVersion: Int,
  keySchema: Option[SchemaArtifact],
  valueSchema: Option[SchemaArtifact],
  indexMapping: Option[IndexMapping] = None
) derives LMDBCodecJson {
  def keyFingerprint: Option[String]   = keySchema.map(_.fingerprint)
  def valueFingerprint: Option[String] = valueSchema.map(_.fingerprint)
}

object MetaDataEntry {

  /** Bump this when the on-disk shape of `MetaDataEntry` itself changes. Version 3 added the optional `indexMapping` — an additive, wire-compatible change (v2 records decode with `None`).
    */
  val CurrentLayoutVersion: Int = 3

  /** Build a metadata entry with no schemas — used by the untyped `collectionAllocate(name)` paths where K/T are not known at the call site.
    */
  def untyped(name: String, kind: CollectionKind): MetaDataEntry =
    MetaDataEntry(name, kind, CurrentLayoutVersion, None, None)

  /** Build a metadata entry from the caller's key and value schema artifacts. */
  def typed(name: String, kind: CollectionKind, keySchema: SchemaArtifact, valueSchema: SchemaArtifact): MetaDataEntry =
    MetaDataEntry(name, kind, CurrentLayoutVersion, Some(keySchema), Some(valueSchema))
}
