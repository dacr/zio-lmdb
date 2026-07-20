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

import zio.lmdb.keycodecs.{KeyCodec, KeyTypeId}

/** One declared component of an index key: the persisted description of where its value comes from ([[IndexComponentSource]] + the component codec's `keyId`) paired with the typed accessor that actually computes it on the write path. The declaration
  * and the accessor live side by side on purpose — the declared path must describe what the accessor reads, and colocating them is what keeps that contract visible at the call site.
  *
  * @tparam K
  *   the source collection key type
  * @tparam T
  *   the source collection value type
  * @tparam C
  *   this component's type
  */
final case class IndexKeyComponent[K, T, C](source: IndexComponentSource, keyId: KeyTypeId, fixedWidth: Option[Int], eval: (K, T) => Option[C]) {
  def toComponent: IndexComponent = IndexComponent(source, keyId.value, fixedWidth)
}

/** A declared index key (`FROM_KEY` or `TO_KEY`): its ordered persisted components and the typed function assembling their values into the key. `eval` yields `None` when any component is absent, in which case the record is simply not indexed (the
  * usual optional-field semantics).
  */
final case class IndexKeySpec[K, T, IK](components: List[IndexComponent], eval: (K, T) => Option[IK])

/** Builders for [[IndexKeyComponent]] / [[IndexKeySpec]], used with `LMDBCollection.withDeclaredIndex`:
  *
  * {{{
  * collection.withDeclaredIndex(byTimestamp)(
  *   from = IdxKey.tuple(IdxKey.field("timestamp")((_, m) => m.timestamp.toInstant), IdxKey.primaryKey),
  *   to   = IdxKey.of(IdxKey.primaryKey)
  * )
  * }}}
  */
object IdxKey {

  /** A component read from a (dotted) value field path that is always present. */
  def field[K, T, C](path: String)(f: (K, T) => C)(using kc: KeyCodec[C]): IndexKeyComponent[K, T, C] =
    IndexKeyComponent(IndexComponentSource.Field(path), kc.keyId, kc.width, (k, t) => Some(f(k, t)))

  /** A component read from an optional value field path; a `None` leaves the record unindexed. */
  def fieldOpt[K, T, C](path: String)(f: (K, T) => Option[C])(using kc: KeyCodec[C]): IndexKeyComponent[K, T, C] =
    IndexKeyComponent(IndexComponentSource.Field(path), kc.keyId, kc.width, f)

  /** A component taking the first non-null of several field paths; the accessor must implement the same fallback order (e.g. `f.identifiedPersonId.orElse(f.inferredIdentifiedPersonId)`).
    */
  def coalesce[K, T, C](paths: String*)(f: (K, T) => Option[C])(using kc: KeyCodec[C]): IndexKeyComponent[K, T, C] =
    IndexKeyComponent(IndexComponentSource.Coalesce(paths.toList), kc.keyId, kc.width, f)

  /** A component with no declarative form (custom computation). Recorded for documentation; query planners will not match predicates against it.
    */
  def opaque[K, T, C](hint: String)(f: (K, T) => Option[C])(using kc: KeyCodec[C]): IndexKeyComponent[K, T, C] =
    IndexKeyComponent(IndexComponentSource.Opaque(hint), kc.keyId, kc.width, f)

  /** The source record's own key. */
  def primaryKey[K, T](using kc: KeyCodec[K]): IndexKeyComponent[K, T, K] =
    IndexKeyComponent(IndexComponentSource.PrimaryKey, kc.keyId, kc.width, (k, _) => Some(k))

  /** A single-component index key. */
  def of[K, T, C](c: IndexKeyComponent[K, T, C]): IndexKeySpec[K, T, C] =
    IndexKeySpec(List(c.toComponent), c.eval)

  /** A two-component index key, encoded with the tuple2 `KeyCodec` (components ordered as given). */
  def tuple[K, T, A, B](a: IndexKeyComponent[K, T, A], b: IndexKeyComponent[K, T, B]): IndexKeySpec[K, T, (A, B)] =
    IndexKeySpec(
      List(a.toComponent, b.toComponent),
      (k, t) => a.eval(k, t).zip(b.eval(k, t))
    )

  /** A three-component index key, encoded with the tuple3 `KeyCodec`. */
  def tuple3[K, T, A, B, C](a: IndexKeyComponent[K, T, A], b: IndexKeyComponent[K, T, B], c: IndexKeyComponent[K, T, C]): IndexKeySpec[K, T, (A, B, C)] =
    IndexKeySpec(
      List(a.toComponent, b.toComponent, c.toComponent),
      (k, t) =>
        for {
          va <- a.eval(k, t)
          vb <- b.eval(k, t)
          vc <- c.eval(k, t)
        } yield (va, vb, vc)
    )

  /** A four-component index key, encoded with the tuple4 `KeyCodec`. */
  def tuple4[K, T, A, B, C, D](
    a: IndexKeyComponent[K, T, A],
    b: IndexKeyComponent[K, T, B],
    c: IndexKeyComponent[K, T, C],
    d: IndexKeyComponent[K, T, D]
  ): IndexKeySpec[K, T, (A, B, C, D)] =
    IndexKeySpec(
      List(a.toComponent, b.toComponent, c.toComponent, d.toComponent),
      (k, t) =>
        for {
          va <- a.eval(k, t)
          vb <- b.eval(k, t)
          vc <- c.eval(k, t)
          vd <- d.eval(k, t)
        } yield (va, vb, vc, vd)
    )
}
