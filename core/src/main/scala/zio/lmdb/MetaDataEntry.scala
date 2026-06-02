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

import zio.json._
import zio.lmdb.json.LMDBCodecJson
import zio.lmdb.schema.SchemaArtifact

enum CollectionKind derives JsonCodec {
  case Regular
  case Index
  case Multi
}

/** Persistent description of a collection / multi-collection / index.
  *
  * Stored in the metadata sub-collection by `LMDBLive`. Carries the key and value schemas as
  * `SchemaArtifact`s so that L2A's drift detection can compare a caller's expected schema with
  * what was first persisted; later layers (L3 property catalog, L5 IRI bindings) re-use the same
  * mechanism for per-property and per-IRI bindings.
  *
  * `layoutVersion` is the version of this metadata record itself. Bumping it allows graceful
  * migrations of the catalog format in future releases.
  */
case class MetaDataEntry(
  collectionName: String,
  collectionKind: CollectionKind,
  layoutVersion: Int,
  keySchema: Option[SchemaArtifact],
  valueSchema: Option[SchemaArtifact]
) derives LMDBCodecJson {
  def keyFingerprint: Option[String]   = keySchema.map(_.fingerprint)
  def valueFingerprint: Option[String] = valueSchema.map(_.fingerprint)
}

object MetaDataEntry {

  /** Bump this when the on-disk shape of `MetaDataEntry` itself changes. */
  val CurrentLayoutVersion: Int = 2

  /** Build a metadata entry with no schemas — used by the untyped `collectionAllocate(name)` paths
    * where K/T are not known at the call site.
    */
  def untyped(name: String, kind: CollectionKind): MetaDataEntry =
    MetaDataEntry(name, kind, CurrentLayoutVersion, None, None)

  /** Build a metadata entry from the caller's key and value schema artifacts. */
  def typed(name: String, kind: CollectionKind, keySchema: SchemaArtifact, valueSchema: SchemaArtifact): MetaDataEntry =
    MetaDataEntry(name, kind, CurrentLayoutVersion, Some(keySchema), Some(valueSchema))
}
