/*
 * Copyright 2025 David Crosson
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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.{Try, Success, Failure}

/**
 * A codec abstraction for encoding and decoding keys of type `K` into
 * a byte array representation for use with an LMDB database. The trait
 * provides methods for serialization and deserialization, allowing a
 * bidirectional mapping between `K` and its byte representation.
 *
 * @tparam K The type of key to be encoded and decoded.
 */
trait LMDBKodec[K] {
  def encode(key: K): Array[Byte]
  def decode(keyBytes: ByteBuffer): Either[String, K] // TODO Replace String by Throwable
}

object LMDBKodec {

  given LMDBKodec[String] = new LMDBKodec[String] {
    private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

    override def encode(key: String): Array[Byte] = key.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[String, String] =
      Right(charset.decode(keyBytes).toString)
  }

  given LMDBKodec[UUID] = new LMDBKodec[UUID] {
    private val charset = StandardCharsets.UTF_8

    override def encode(key: UUID): Array[Byte] = key.toString.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[String, UUID] =
      Try(UUID.fromString(charset.decode(keyBytes).toString)) match {
        case Success(value) => Right(value)
        case Failure(exception)=> Left(exception.getMessage)
      }
  }


}