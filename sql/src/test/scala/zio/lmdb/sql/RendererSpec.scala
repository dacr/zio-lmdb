/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package zio.lmdb.sql

import zio.test.*
import zio.lmdb.json.JValue.*
import zio.lmdb.sql.result.{Column, Format, QueryResult, Renderer}

import scala.collection.immutable.ListMap

object RendererSpec extends ZIOSpecDefault {

  private val result = QueryResult.of(
    List(Column("name", "string"), Column("age", "integer")),
    List(
      MapV(ListMap("name" -> StringV("Alice"), "age" -> LongV(30))),
      MapV(ListMap("name" -> StringV("Bob,Jr"), "age" -> LongV(25)))
    )
  )

  private def lines(format: Format) = Renderer.render(format, result).runCollect.map(_.toList)

  val spec = suite("Renderer")(
    test("JSON renders one plain-JSON object per row") {
      lines(Format.Json).map { ls =>
        assertTrue(ls == List("""{"name":"Alice","age":30}""", """{"name":"Bob,Jr","age":25}"""))
      }
    },
    test("CSV renders a header then escaped rows") {
      lines(Format.Csv).map { ls =>
        assertTrue(ls == List("name,age", "Alice,30", "\"Bob,Jr\",25"))
      }
    },
    test("table renders aligned columns (row count is added by the REPL, not the renderer)") {
      lines(Format.Table).map { ls =>
        assertTrue(
          ls.head == "name   | age",
          ls.contains("Alice  | 30"),
          !ls.exists(_.startsWith("(")) // no footer
        )
      }
    }
  )
}
