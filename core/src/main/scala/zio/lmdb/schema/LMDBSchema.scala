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

import zio.lmdb.keycodecs.KeyCodec

import scala.compiletime.summonInline
import scala.deriving.Mirror

/** Typeclass providing the persisted [[SchemaArtifact]] for a type `T`.
  *
  * The typeclass is intentionally separate from `LMDBCodec[T]`: the codec is concerned with byte
  * round-tripping, the schema is concerned with describing the shape to other tools (catalog,
  * REPL, drift detection). A user wiring a custom codec can declare a matching `LMDBSchema[T]`
  * given without having to retrofit the codec API.
  *
  * Keys and values are described differently, by what each actually needs:
  *   - a *value* type opts into a structural [[SchemaArtifact.JsonSchema]] via `derives LMDBSchema`
  *     (or a user-declared given); otherwise it falls back to the low-priority opaque schema, which
  *     downstream layers (L3, L5) detect and surface as a warning rather than a hard error.
  *   - a *key* type is described by the identity of its `KeyCodec` — a [[SchemaArtifact.KeySchema]]
  *     carrying the codec's stable `keyId`. Since a typed collection always has a `KeyCodec[K]`, a
  *     key is never opaque, and the id (rather than the raw bytes) is what distinguishes a `Long`
  *     key from a byte-identical geo key, or a `UUID` from a `ULID`.
  */
trait LMDBSchema[T] {
  def artifact: SchemaArtifact
}

object LMDBSchema extends LMDBSchemaLowPriority {

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

  /** The schema for any type with a `KeyCodec` is the codec's own identity — a [[SchemaArtifact.KeySchema]]
    * carrying its stable `keyId`. Because creating a typed collection requires a `KeyCodec[K]`, a
    * *key* is therefore never opaque; and because the id comes from the codec (the single source of
    * truth about the bytes), byte-compatible but semantically different keys stay distinct: `Int`
    * (`lmdb:int32`) vs `Long` (`lmdb:int64`), `UUID` vs `ULID` vs `UUIDv7`, and so on. Tuple keys
    * compose their components' ids. This given is more specific than the `opaque` fallback, so it
    * wins for any key-capable type; a value type with no `KeyCodec` and no `derives` stays opaque.
    */
  given keyCodecSchema[K](using kc: KeyCodec[K]): LMDBSchema[K] =
    from(SchemaArtifact.KeySchema(kc.keyId.value))
}

trait LMDBSchemaLowPriority {

  /** Lowest-priority fallback. Any more specific `given LMDBSchema[T]` in scope — a built-in
    * key-type schema, a `derives LMDBSchema`, or a user-declared given — takes precedence.
    *
    * Naming the hint after the concrete type via a manifest would require reflection at the call
    * site; instead, callers wanting a meaningful hint should provide their own
    * `given LMDBSchema[T] = LMDBSchema.from(SchemaArtifact.OpaqueSchema("MyTypeName"))`.
    */
  given opaque[T]: LMDBSchema[T] = LMDBSchema.from(SchemaArtifact.OpaqueSchema("LMDBSchema.opaque"))
}
