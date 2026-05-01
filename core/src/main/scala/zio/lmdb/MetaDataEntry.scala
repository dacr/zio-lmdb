package zio.lmdb

import zio.json._
import zio.json.ast.Json
import zio.json.internal.{RetractReader, Write}
import zio.lmdb.json.LMDBCodecJson

enum CollectionKind derives JsonCodec {
  case Regular
  case Index
}

case class MetaDataEntry(
  collectionName: String,
  collectionKind: CollectionKind,
  keySchema: Option[Json],
  valueSchema: Option[Json]
) derives LMDBCodecJson
