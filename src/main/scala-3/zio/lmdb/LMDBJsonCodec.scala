package zio.lmdb

import zio.json.*

import java.nio.charset.StandardCharsets
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}
import scala.quoted.{Expr, Quotes, Type}

class LMDBJsonCodec[T](using codec: JsonCodec[T]) extends LMDBCodec[T] {
  private val charset = StandardCharsets.UTF_8

  def encode(that: T): Bytes = {
    that.toJson.getBytes(charset)
  }

  def decode(that: Bytes): Either[String, T] = {
    new String(that, charset).fromJson[T]
  }
}

object LMDBJsonCodec {
  inline given derived[T](using m: Mirror.Of[T]): LMDBJsonCodec[T] = ${ derivedMacro[T] }

  def derivedMacro[T: Type](using Quotes): Expr[LMDBJsonCodec[T]] = {
    ???
  }

  // inline given derived[T](using m: Mirror.Of[T]): LMDBJsonCodec[T] = {
  //  given JsonCodec[T] = JsonCodec.derived[T]
  //  new LMDBJsonCodec[T]()
  //}
}
