package zio.lmdb

import zio.*
import zio.stream.*

case class LMDBIndex[FROM_KEY, TO_KEY](
  name: IndexName,
  description: Option[String],
  lmdb: LMDB
)(implicit keyCodec: LMDBKodec[FROM_KEY], toKeyCodec: LMDBKodec[TO_KEY]) {

  def index(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Unit] = lmdb.index[FROM_KEY,TO_KEY](name, key, toKey)

  def indexContains(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.indexContains[FROM_KEY,TO_KEY](name, key, toKey)

  def unindex(
    key: FROM_KEY,
    toKey: TO_KEY
  ): IO[IndexErrors, Boolean] = lmdb.unindex[FROM_KEY,TO_KEY](name, key, toKey)

  def indexed(
    key: FROM_KEY
  ): ZStream[Any, IndexErrors, TO_KEY] = lmdb.indexed[FROM_KEY,TO_KEY](name, key)

}
