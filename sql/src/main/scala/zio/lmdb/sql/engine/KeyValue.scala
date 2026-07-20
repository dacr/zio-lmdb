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

import zio.lmdb.json.JValue

import java.time.Instant
import java.util.UUID

/** The runtime, type-erased decoding of a key — the dynamic counterpart of the static key type, obtained from the bytes via the collection's recorded `keyId`. Carries a `JValue` projection (so keys flow through the same row machinery as values) and
  * a human render for display.
  */
sealed trait KeyValue {
  def toJValue: JValue
  def render: String
}

object KeyValue {
  final case class KStr(value: String)      extends KeyValue { def toJValue = JValue.StringV(value); def render = value              }
  final case class KLong(value: Long)       extends KeyValue { def toJValue = JValue.LongV(value); def render = value.toString       }
  final case class KUuid(value: UUID)       extends KeyValue { def toJValue = JValue.IdentifierV(value); def render = value.toString }
  final case class KInstant(value: Instant) extends KeyValue { def toJValue = JValue.InstantV(value); def render = value.toString    }

  /** A composite (tuple) key, decoded component by component. */
  final case class KTuple(values: List[KeyValue]) extends KeyValue {
    def toJValue = JValue.ListV(values.map(_.toJValue))
    def render   = values.map(_.render).mkString("(", ", ", ")")
  }

  /** Fallback for keys whose codec is unknown/unloaded or non-invertible (e.g. a UCA sort key). */
  final case class KBytes(value: Array[Byte]) extends KeyValue {
    private def hex = value.iterator.map(b => f"${b & 0xff}%02x").mkString
    def toJValue    = JValue.StringV(s"0x$hex")
    def render      = s"0x$hex"
  }
}
