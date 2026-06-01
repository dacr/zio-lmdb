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

import zio.lmdb.keycodecs.KeyCodecError.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.tailrec

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
    *   the decoded key or a structured error
    */
  def decode(keyBytes: ByteBuffer): Either[KeyCodecError, K]

  /** The fixed width of the encoded key in bytes, if applicable.
    * @return
    *   Some(width) if fixed width, None otherwise.
    */
  def width: Option[Int] = None
}

object KeyCodec {

  given stringKeyCodec: KeyCodec[String] = new KeyCodec[String] {
    private val charset = StandardCharsets.UTF_8 // TODO enhance charset support

    override def encode(key: String): Array[Byte] = key.getBytes(charset)

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, String] =
      try {
        Right(charset.decode(keyBytes).toString)
      } catch {
        case e: Exception => Left(InternalFailure(e.getMessage))
      }

    override def width: Option[Int] = None
  }

  given longKeyCodec: KeyCodec[Long] = new KeyCodec[Long] {
    // Big-endian with sign-bit flip so the lexicographic order of the encoded
    // bytes matches the natural numeric order on Long (including negatives).
    private val signBias: Long = Long.MinValue

    override def encode(key: Long): Array[Byte] = {
      val biased = key ^ signBias
      val out    = new Array[Byte](8)
      out(0) = (biased >>> 56).toByte
      out(1) = (biased >>> 48).toByte
      out(2) = (biased >>> 40).toByte
      out(3) = (biased >>> 32).toByte
      out(4) = (biased >>> 24).toByte
      out(5) = (biased >>> 16).toByte
      out(6) = (biased >>> 8).toByte
      out(7) = biased.toByte
      out
    }

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, Long] = {
      if (keyBytes.remaining() < 8) Left(InsufficientBytes(8, keyBytes.remaining()))
      else Right(keyBytes.getLong ^ signBias)
    }

    override def width: Option[Int] = Some(8)
  }

  given intKeyCodec: KeyCodec[Int] = new KeyCodec[Int] {
    // Big-endian with sign-bit flip so the lexicographic order of the encoded
    // bytes matches the natural numeric order on Int (including negatives).
    private val signBias: Int = Int.MinValue

    override def encode(key: Int): Array[Byte] = {
      val biased = key ^ signBias
      val out    = new Array[Byte](4)
      out(0) = (biased >>> 24).toByte
      out(1) = (biased >>> 16).toByte
      out(2) = (biased >>> 8).toByte
      out(3) = biased.toByte
      out
    }

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, Int] = {
      if (keyBytes.remaining() < 4) Left(InsufficientBytes(4, keyBytes.remaining()))
      else Right(keyBytes.getInt ^ signBias)
    }

    override def width: Option[Int] = Some(4)
  }

  given shortKeyCodec: KeyCodec[Short] = new KeyCodec[Short] {
    // Big-endian with sign-bit flip so the lexicographic order of the encoded
    // bytes matches the natural numeric order on Short (including negatives).
    private val signBias: Int = 0x8000

    override def encode(key: Short): Array[Byte] = {
      val biased = (key & 0xffff) ^ signBias
      val out    = new Array[Byte](2)
      out(0) = (biased >>> 8).toByte
      out(1) = biased.toByte
      out
    }

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, Short] = {
      if (keyBytes.remaining() < 2) Left(InsufficientBytes(2, keyBytes.remaining()))
      else Right(((keyBytes.getShort & 0xffff) ^ signBias).toShort)
    }

    override def width: Option[Int] = Some(2)
  }

  given uuidKeyCodec: KeyCodec[UUID] = new KeyCodec[UUID] {
    override def encode(key: UUID): Array[Byte] = UUIDTools.uuidToBytes(key)

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, UUID] = {
      if (keyBytes.remaining() < 16) Left(InsufficientBytes(16, keyBytes.remaining()))
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
        case None    =>
          // Variable width A: escape 0x00 -> 0x00 0x01 (so naked 0x00 is impossible
          // inside escape(bytesA)) and use a two-byte separator 0x00 0x00.
          // Decoder finds the unique first 0x00 0x00 sequence; bytesB may then
          // contain any bytes (including arbitrary 0x00 / 0xFF) without ambiguity.
          val builder = Array.newBuilder[Byte]
          builder.sizeHint(bytesA.length + bytesB.length + 2) // Heuristic

          bytesA.foreach {
            case 0 => builder += 0; builder += 1
            case b => builder += b
          }
          builder += 0 // Separator part 1
          builder += 0 // Separator part 2
          builder ++= bytesB
          builder.result()
      }
    }

    override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, (A, B)] = {
      codecA.width match {
        case Some(wa) =>
          if (keyBytes.remaining() < wa) Left(InsufficientBytes(wa, keyBytes.remaining()))
          else {
            val limit    = keyBytes.limit()
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
        case None     =>
          val startPos = keyBytes.position()
          val limit    = keyBytes.limit()

          @tailrec
          def findSeparator(pos: Int): Either[KeyCodecError, Int] = {
            if (pos >= limit) Left(MissingSeparator(pos))
            else {
              val b = keyBytes.get(pos)
              if (b == 0) {
                if (pos + 1 >= limit) Left(MissingSeparator(pos + 1))
                else {
                  val next = keyBytes.get(pos + 1)
                  if (next == 0) Right(pos)            // Two-byte separator 0x00 0x00
                  else if (next == 1) findSeparator(pos + 2) // Escape sequence 0x00 0x01
                  else Left(UnescapedZero(pos))
                }
              } else findSeparator(pos + 1)
            }
          }

          findSeparator(startPos).flatMap { separatorPos =>
            // Unescape A
            val lengthA = separatorPos - startPos
            val bytesA  = new Array[Byte](lengthA) // Max size
            val bufferA = ByteBuffer.wrap(bytesA)  // Write wrapper

            @tailrec
            def unescape(pos: Int): Either[KeyCodecError, ByteBuffer] = {
              if (pos >= separatorPos) {
                bufferA.flip()
                Right(bufferA)
              } else {
                val b = keyBytes.get(pos)
                if (b == 0) {
                  if (pos + 1 < separatorPos && keyBytes.get(pos + 1) == 1.toByte) {
                    bufferA.put(0.toByte)
                    unescape(pos + 2)
                  } else Left(UnescapedZero(pos))
                } else {
                  bufferA.put(b)
                  unescape(pos + 1)
                }
              }
            }

            unescape(startPos).flatMap { rawA =>
              codecA.decode(rawA).flatMap { a =>
                // Skip the two-byte separator
                keyBytes.position(separatorPos + 2)
                codecB.decode(keyBytes).map(b => (a, b))
              }
            }
          }
      }
    }
  }

  // Tuple3 / Tuple4 codecs are derived by nesting tuple2 on the left. The
  // encoding of (a, b, c) coincides with the encoding of ((a, b), c), so a
  // prefix scan over all triples sharing a given (a, b) becomes a byte-level
  // prefix scan over encode((a, b)) followed by its terminating separator.
  // Same property extends to tuple4 via ((a, b, c), d).

  given tuple3KeyCodec[A, B, C](using KeyCodec[A], KeyCodec[B], KeyCodec[C]): KeyCodec[(A, B, C)] = {
    val inner = summon[KeyCodec[((A, B), C)]]
    new KeyCodec[(A, B, C)] {
      override def width: Option[Int] = inner.width

      override def encode(key: (A, B, C)): Array[Byte] =
        inner.encode(((key._1, key._2), key._3))

      override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, (A, B, C)] =
        inner.decode(keyBytes).map { case ((a, b), c) => (a, b, c) }
    }
  }

  given tuple4KeyCodec[A, B, C, D](using KeyCodec[A], KeyCodec[B], KeyCodec[C], KeyCodec[D]): KeyCodec[(A, B, C, D)] = {
    val inner = summon[KeyCodec[((A, B, C), D)]]
    new KeyCodec[(A, B, C, D)] {
      override def width: Option[Int] = inner.width

      override def encode(key: (A, B, C, D)): Array[Byte] =
        inner.encode(((key._1, key._2, key._3), key._4))

      override def decode(keyBytes: ByteBuffer): Either[KeyCodecError, (A, B, C, D)] =
        inner.decode(keyBytes).map { case ((a, b, c), d) => (a, b, c, d) }
    }
  }

}
