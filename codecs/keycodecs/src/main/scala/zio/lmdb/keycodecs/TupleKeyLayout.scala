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

import scala.annotation.tailrec

/** Byte-level view of the composite-key layout produced by the tuple `KeyCodec`s, for callers that work on raw key bytes without the static tuple type (e.g. a query planner turning predicates on leading components into byte-range scan bounds).
  *
  * The layout contract (see the tuple codecs in [[KeyCodec]]): a tupleN encoding coincides with the left-nested tuple2 encoding (`(a, b, c)` ≡ `((a, b), c)`); a fixed-width left side is concatenated as-is, a variable-width left side is escaped
  * (`0x00` → `0x00 0x01`) and terminated by the two-byte separator `0x00 0x00`. Both transformations preserve lexicographic byte order, which is what makes prefix-based range scans over leading components sound.
  *
  * Components are described positionally by their codec's fixed `width` (`None` = variable width), exactly as reported by `KeyCodec.width`.
  */
object TupleKeyLayout {

  /** `0x00` → `0x00 0x01`, other bytes unchanged (order-preserving, leaves no naked `0x00 0x00`). */
  private def escapeBytes(bytes: Array[Byte]): Array[Byte] = {
    var zeros = 0
    var i     = 0
    while (i < bytes.length) { if (bytes(i) == 0) zeros += 1; i += 1 }
    if (zeros == 0) bytes
    else {
      val out = new Array[Byte](bytes.length + zeros)
      var in  = 0
      var o   = 0
      while (in < bytes.length) {
        val b = bytes(in)
        out(o) = b
        o += 1
        if (b == 0) { out(o) = 1; o += 1 }
        in += 1
      }
      out
    }
  }

  /** Position of the first naked `0x00 0x00` separator, scanning past `0x00 0x01` escapes. */
  private def findSeparator(bytes: Array[Byte]): Either[KeyCodecError, Int] = {
    @tailrec
    def loop(pos: Int): Either[KeyCodecError, Int] =
      if (pos >= bytes.length) Left(MissingSeparator(pos))
      else if (bytes(pos) != 0) loop(pos + 1)
      else if (pos + 1 >= bytes.length) Left(MissingSeparator(pos + 1))
      else
        bytes(pos + 1) match {
          case 0 => Right(pos)
          case 1 => loop(pos + 2)
          case _ => Left(UnescapedZero(pos))
        }
    loop(0)
  }

  /** Reverse [[escapeBytes]] on a region known to contain no naked separator. */
  private def unescapeBytes(bytes: Array[Byte], until: Int): Either[KeyCodecError, Array[Byte]] = {
    val out                                                = Array.newBuilder[Byte]
    out.sizeHint(until)
    @tailrec
    def loop(pos: Int): Either[KeyCodecError, Array[Byte]] =
      if (pos >= until) Right(out.result())
      else {
        val b = bytes(pos)
        if (b != 0) { out += b; loop(pos + 1) }
        else if (pos + 1 < until && bytes(pos + 1) == 1) { out += 0; loop(pos + 2) }
        else Left(UnescapedZero(pos))
      }
    loop(0)
  }

  /** The byte prefix shared by every full composite key (of `widths.size` components) whose first `components.size` components have exactly the given encodings. With all components supplied this is the full key encoding, byte-identical to the tuple
    * codec's `encode`.
    *
    * The result is a valid inclusive lower bound for a scan over that component prefix; pair it with [[byteSuccessor]] for the exclusive upper bound.
    */
  def prefixBytes(widths: List[Option[Int]], components: List[Array[Byte]]): Array[Byte] = {
    require(components.nonEmpty, "at least one component required")
    require(components.size <= widths.size, s"${components.size} components but only ${widths.size} widths")
    val m = components.size
    val n = widths.size

    // E_k for the supplied components: left-nested tuple2 encoding.
    var acc      = components.head
    var accFixed = widths.head.isDefined
    var k        = 1
    while (k < m) {
      acc = if (accFixed) acc ++ components(k) else escapeBytes(acc) ++ Array[Byte](0, 0) ++ components(k)
      accFixed = accFixed && widths(k).isDefined
      k += 1
    }

    // Project E_m through the remaining nesting steps: whenever the left side of a step is
    // variable-width it gets escaped, and exactly at the first step (where acc IS the whole left
    // side, not a strict prefix of it) the terminating separator is part of the shared prefix.
    while (k < n) {
      if (!accFixed) {
        acc = escapeBytes(acc)
        if (k == m) acc = acc ++ Array[Byte](0, 0)
      }
      accFixed = accFixed && widths(k).isDefined
      k += 1
    }
    acc
  }

  /** The smallest byte string strictly greater than every byte string starting with `bytes`: increment the last non-`0xFF` byte and truncate. `None` when unbounded (all `0xFF`).
    */
  def byteSuccessor(bytes: Array[Byte]): Option[Array[Byte]] = {
    var i = bytes.length - 1
    while (i >= 0 && bytes(i) == -1) i -= 1
    if (i < 0) None
    else {
      val out = java.util.Arrays.copyOf(bytes, i + 1)
      out(i) = (out(i) + 1).toByte
      Some(out)
    }
  }

  /** Split a full composite key back into its components' raw (unescaped) encoded bytes, peeling the left-nested structure from the outside in.
    */
  def decodeComponents(widths: List[Option[Int]], keyBytes: Array[Byte]): Either[KeyCodecError, List[Array[Byte]]] = {
    def peel(count: Int, bytes: Array[Byte]): Either[KeyCodecError, List[Array[Byte]]] =
      if (count == 1) Right(List(bytes))
      else {
        val leftWidths = widths.take(count - 1)
        if (leftWidths.forall(_.isDefined)) {
          val w = leftWidths.flatten.sum
          if (bytes.length < w) Left(InsufficientBytes(w, bytes.length))
          else peel(count - 1, bytes.take(w)).map(_ :+ bytes.drop(w))
        } else
          for {
            sepPos <- findSeparator(bytes)
            left   <- unescapeBytes(bytes, sepPos)
            parts  <- peel(count - 1, left)
          } yield parts :+ bytes.drop(sepPos + 2)
      }
    if (widths.isEmpty) Left(InvalidInput("no component widths given"))
    else peel(widths.size, keyBytes)
  }
}
