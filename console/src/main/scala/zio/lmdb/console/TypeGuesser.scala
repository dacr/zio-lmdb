package zio.lmdb.console

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import zio.json._
import scala.util.Try
import org.jline.utils.AttributedStringBuilder
import org.jline.utils.AttributedStyle

object TypeGuesser {

  private val UuidRegex = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$".r
  private val UlidRegex = "^[0-9A-HJKMNP-TV-Z]{26}$".r
  private val NumberRegex = "^-?\\d+(\\.\\d+)?$".r
  private val BooleanRegex = "^(?i)(true|false)$".r
  private val JsonObjectRegex = "(?s)^\\s*\\{.*\\}\\s*$".r
  private val JsonArrayRegex = "(?s)^\\s*\\[.*\\]\\s*$".r

  def isJson(s: String): Boolean = {
    JsonObjectRegex.matches(s) || JsonArrayRegex.matches(s)
  }

  def guessType(bytes: Array[Byte]): String = {
    if (bytes.isEmpty) "String"
    else {
      val sTry = Try(new String(bytes, StandardCharsets.UTF_8))
      if (sTry.isSuccess) {
        val s = sTry.get
        if (s.nonEmpty && s.forall(c => !java.lang.Character.isISOControl(c) || java.lang.Character.isWhitespace(c))) {
          if (UuidRegex.matches(s)) "UUID String"
          else if (UlidRegex.matches(s)) "ULID String"
          else if (NumberRegex.matches(s)) "Number String"
          else if (BooleanRegex.matches(s)) "Boolean String"
          else if (isJson(s)) "JSON"
          else "String"
        } else if (bytes.length == 8) "Long"
        else if (bytes.length == 4) "Int"
        else if (bytes.length == 16) "UUID/ULID"
        else "**BINARY**"
      } else if (bytes.length == 8) "Long"
      else if (bytes.length == 4) "Int"
      else if (bytes.length == 16) "UUID/ULID"
      else "**BINARY**"
    }
  }

  def formatValue(bytes: Array[Byte]): String = {
    val gType = guessType(bytes)
    gType match {
      case "JSON" | "String" | "UUID String" | "ULID String" | "Number String" | "Boolean String" => 
        new String(bytes, StandardCharsets.UTF_8)
      case "Long" if bytes.length == 8 => ByteBuffer.wrap(bytes).getLong.toString
      case "Int" if bytes.length == 4 => ByteBuffer.wrap(bytes).getInt.toString
      case "UUID/ULID" if bytes.length == 16 => 
        val bb = ByteBuffer.wrap(bytes)
        new UUID(bb.getLong, bb.getLong).toString
      case _ => "**BINARY**"
    }
  }
  
  def formatKey(bytes: Array[Byte]): String = formatValue(bytes)

  def colorize(content: String): String = {
    if (content == "**BINARY**") {
      new AttributedStringBuilder()
        .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.RED).bold())
        .append(content)
        .toAnsi
    } else if (isJson(content)) {
      new AttributedStringBuilder()
        .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN))
        .append(content)
        .toAnsi
    } else if (UuidRegex.matches(content) || UlidRegex.matches(content)) {
      new AttributedStringBuilder()
        .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA))
        .append(content)
        .toAnsi
    } else if (NumberRegex.matches(content) || BooleanRegex.matches(content)) {
      new AttributedStringBuilder()
        .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
        .append(content)
        .toAnsi
    } else {
      content
    }
  }
}
