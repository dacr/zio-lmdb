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
package zio.lmdb.sql.engine

import zio.*
import zio.lmdb.*
import zio.lmdb.json.JValue
import zio.lmdb.schema.SchemaArtifact
import zio.lmdb.sql.SqlError

/** A value-side column resolved from the value `JsonSchema`. */
final case class ColumnInfo(name: String, typeHint: String)

/** The catalog view of one collection, read from its `MetaDataEntry`: the key's `keyId` (from
  * `KeySchema`) and the value columns (from `JsonSchema`).
  */
final case class CollectionInfo(
  name: String,
  kind: CollectionKind,
  keyId: Option[String],
  columns: List[ColumnInfo],
  keySchema: Option[SchemaArtifact],
  valueSchema: Option[SchemaArtifact]
)

object Catalog {

  private val metaName = LMDBConfig.default.metaDataCollectionName

  /** Resolve a collection by name, or fail with `UnknownCollection`. */
  def lookup(lmdb: LMDB, name: String): IO[SqlError, CollectionInfo] =
    lmdb
      .fetch[String, MetaDataEntry](metaName, name)
      .mapError(e => SqlError.Storage(e.toString))
      .flatMap {
        case Some(entry) => ZIO.succeed(toInfo(entry))
        case None        => ZIO.fail(SqlError.UnknownCollection(name))
      }

  /** All collection metadata entries (for `SHOW`). */
  def list(lmdb: LMDB): IO[SqlError, List[MetaDataEntry]] =
    lmdb
      .streamWithKeys[String, MetaDataEntry](metaName)
      .map(_._2)
      .runCollect
      .map(_.toList)
      .mapError(e => SqlError.Storage(e.toString))

  def toInfo(entry: MetaDataEntry): CollectionInfo =
    CollectionInfo(entry.collectionName, entry.collectionKind, keyIdOf(entry.keySchema), columnsOf(entry.valueSchema), entry.keySchema, entry.valueSchema)

  def keyIdOf(s: Option[SchemaArtifact]): Option[String] =
    s.collect { case SchemaArtifact.KeySchema(id) => id }

  def columnsOf(s: Option[SchemaArtifact]): List[ColumnInfo] =
    s match {
      case Some(SchemaArtifact.JsonSchema(JValue.MapV(root))) =>
        root.get("properties") match {
          case Some(JValue.MapV(props)) => props.toList.map { case (n, jv) => ColumnInfo(n, typeHintOf(jv)) }
          case _                        => Nil
        }
      case _ => Nil
    }

  private def typeHintOf(jv: JValue): String =
    jv match {
      case JValue.MapV(m) => m.get("type").collect { case JValue.StringV(t) => t }.getOrElse("any")
      case _              => "any"
    }
}
