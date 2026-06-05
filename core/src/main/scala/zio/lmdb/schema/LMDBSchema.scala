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

import scala.compiletime.summonInline
import scala.deriving.Mirror

/** Typeclass providing the persisted [[SchemaArtifact]] for a type `T`.
  *
  * The typeclass is intentionally separate from `LMDBCodec[T]`: the codec is concerned with byte
  * round-tripping, the schema is concerned with describing the shape to other tools (catalog,
  * REPL, drift detection). A user wiring a custom codec can declare a matching `LMDBSchema[T]`
  * given without having to retrofit the codec API.
  *
  * A low-priority opaque fallback is provided so that types without an explicit schema still
  * compile; downstream layers that need a real schema (L3, L5) detect the opaque case and surface
  * it as a warning rather than a hard error.
  */
trait LMDBSchema[T] {
  def artifact: SchemaArtifact
}

object LMDBSchema {

  def apply[T](using s: LMDBSchema[T]): LMDBSchema[T] = s

  def from[T](a: SchemaArtifact): LMDBSchema[T] = new LMDBSchema[T] {
    val artifact: SchemaArtifact = a
  }

  /** Derive the schema automatically from `T`'s structure, reached as
    * `case class Foo(...) derives LMDBSchema`. Produces a `JsonSchema` artifact whose payload is
    * the [[SchemaShape]] of `T` (field names, types, `required`). Opt-in per type: a type that does
    * not derive (and has no explicit `given`) still resolves to the permissive [[opaque]] fallback.
    */
  inline def derived[T](using Mirror.Of[T]): LMDBSchema[T] =
    from(SchemaArtifact.JsonSchema(summonInline[SchemaShape[T]].shape))

  /** Lowest-priority fallback. Any more specific `given LMDBSchema[T]` in scope takes precedence.
    *
    * Naming the hint after the concrete type via a manifest would require reflection at the call
    * site; instead, callers wanting a meaningful hint should provide their own
    * `given LMDBSchema[T] = LMDBSchema.from(SchemaArtifact.OpaqueSchema("MyTypeName"))`.
    */
  given opaque[T]: LMDBSchema[T] = from(SchemaArtifact.OpaqueSchema("LMDBSchema.opaque"))
}
