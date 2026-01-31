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
package zio.lmdb.keycodecs

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}

/** A codec abstraction for encoding and decoding keys of type `K` into a byte array representation for use with an LMDB database. The trait provides methods for serialization and deserialization, allowing a bidirectional mapping between `K` and its
  * byte representation.
  *
  * @tparam K
  *   The type of key to be encoded and decoded.
  */
trait KeyCodec[K] {

  /** Encodes a key of type `K` into a byte array.
    * @param key
    *   the key to encode
    * @return
    *   the byte array representation
    */
  def encode(key: K): Array[Byte]

  /** Decodes a key of type `K` from a byte buffer.
    * @param keyBytes
    *   the byte buffer containing the encoded key
    * @return
    *   the decoded key or an error message
    */
  def decode(keyBytes: ByteBuffer): Either[String, K] // TODO Replace String by Throwable

  /**
   * The fixed width of the encoded key in bytes, if applicable.
   * @return Some(width) if fixed width, None otherwise.
   */
  def width: Option[Int] = None
}

object KeyCodec {

  given stringKeyCodec: KeyCodec[String] = new KeyCodec[String] {
    private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

    override def encode(key: String): Array[Byte] = key.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[String, String] =
      Right(charset.decode(keyBytes).toString)
      
    override def width: Option[Int] = None
  }

  given uuidKeyCodec: KeyCodec[UUID] = new KeyCodec[UUID] {
    override def encode(key: UUID): Array[Byte] = UUIDTools.uuidToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[String, UUID] = {
      if (keyBytes.remaining() < 16) Left(s"Not enough bytes for UUID, expected 16 but got ${keyBytes.remaining()}")
      else {
        val msb = keyBytes.getLong
        val lsb = keyBytes.getLong
        Right(new UUID(msb, lsb))
      }
    }
    
    override def width: Option[Int] = Some(16)
  }

  given tuple2KeyCodec[A, B](using codecA: KeyCodec[A], codecB: KeyCodec[B]): KeyCodec[(A, B)] = new KeyCodec[(A, B)] {
    override def width: Option[Int] = 
      for {
        wa <- codecA.width
        wb <- codecB.width
      } yield wa + wb

    override def encode(key: (A, B)): Array[Byte] = {
      val (a, b) = key
      val bytesA = codecA.encode(a)
      val bytesB = codecB.encode(b)

      codecA.width match {
        case Some(_) => 
          val out = new Array[Byte](bytesA.length + bytesB.length)
          System.arraycopy(bytesA, 0, out, 0, bytesA.length)
          System.arraycopy(bytesB, 0, out, bytesA.length, bytesB.length)
          out
        case None =>
          // Variable width A: escape 0x00 -> 0x00 0xFF and append 0x00 separator
          val builder = Array.newBuilder[Byte]
          builder.sizeHint(bytesA.length + bytesB.length + 1) // Heuristic
          
          bytesA.foreach {
            case 0 => builder += 0; builder += -1
            case b => builder += b
          }
          builder += 0 // Separator
          builder ++= bytesB
          builder.result()
      }
    }

    override def decode(keyBytes: ByteBuffer): Either[String, (A, B)] = {
      codecA.width match {
        case Some(wa) =>
          if (keyBytes.remaining() < wa) Left(s"Not enough bytes for component A, expected $wa but got ${keyBytes.remaining()}")
          else {
            val limit = keyBytes.limit()
            val position = keyBytes.position()
            
            // Decode A
            keyBytes.limit(position + wa)
            val resA = codecA.decode(keyBytes)
            
            // Restore limit and advance to B
            keyBytes.limit(limit)
            keyBytes.position(position + wa)
            
            for {
              a <- resA
              b <- codecB.decode(keyBytes)
            } yield (a, b)
          }
        case None =>
          val startPos = keyBytes.position()
          val limit = keyBytes.limit()

          @tailrec
          def findSeparator(pos: Int): Either[String, Int] = {
            if (pos >= limit) Left("Separator 0x00 not found for variable width component A")
            else {
              val b = keyBytes.get(pos)
              if (b == 0) {
                // Check for escape sequence 0x00 0xFF
                if (pos + 1 < limit && keyBytes.get(pos + 1) == -1.toByte) findSeparator(pos + 2)
                else Right(pos)
              } else findSeparator(pos + 1)
            }
          }

          findSeparator(startPos).flatMap { separatorPos =>
            // Unescape A
            val lengthA = separatorPos - startPos
            val bytesA = new Array[Byte](lengthA) // Max size
            val bufferA = ByteBuffer.wrap(bytesA) // Write wrapper
            
            @tailrec
            def unescape(pos: Int): Either[String, ByteBuffer] = {
              if (pos >= separatorPos) {
                bufferA.flip()
                Right(bufferA)
              } else {
                val b = keyBytes.get(pos)
                if (b == 0) {
                  if (pos + 1 < limit && keyBytes.get(pos + 1) == -1.toByte) {
                    bufferA.put(0.toByte)
                    unescape(pos + 2)
                  } else Left("Unexpected 0x00 encountered during unescaping") // Should be unreachable given findSeparator logic
                } else {
                  bufferA.put(b)
                  unescape(pos + 1)
                }
              }
            }

            unescape(startPos).flatMap { rawA =>
              codecA.decode(rawA).flatMap { a =>
                // Skip separator
                keyBytes.position(separatorPos + 1)
                codecB.decode(keyBytes).map(b => (a, b))
              }
            }
          }
      }
    }
  }

}