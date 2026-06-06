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
package zio.lmdb.sql

/** Every failure the SQL pipeline can surface, with a human-readable [[message]] for the REPL. */
sealed trait SqlError {
  def message: String
}

object SqlError {
  final case class Parse(index: Int, detail: String)                 extends SqlError { def message = s"parse error at offset $index: $detail" }
  final case class UnknownCollection(name: String)                   extends SqlError { def message = s"unknown collection '$name'"            }
  final case class UnknownColumn(name: String, collection: String)   extends SqlError { def message = s"unknown column '$name' in '$collection'" }
  final case class TypeMismatch(detail: String)                      extends SqlError { def message = detail                                   }
  final case class Unsupported(detail: String)                       extends SqlError { def message = detail                                   }
  final case class KeyError(detail: String)                          extends SqlError { def message = detail                                   }
  final case class Storage(detail: String)                           extends SqlError { def message = detail                                   }
}
