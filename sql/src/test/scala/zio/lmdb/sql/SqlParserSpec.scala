/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package zio.lmdb.sql

import zio.test.*
import zio.lmdb.sql.parser.*

object SqlParserSpec extends ZIOSpecDefault {

  private def ok(sql: String): Statement =
    SqlParser.parse(sql) match {
      case Right(s) => s
      case Left(e)  => throw new AssertionError(s"unexpected parse failure for [$sql]: ${e.message}")
    }

  val spec = suite("SqlParser")(
    test("SELECT * with WHERE, ORDER BY, LIMIT") {
      val s = ok("select * from orders where customer = 'Alice' order by _key desc limit 10")
      assertTrue(
        s == Statement.Select(
          Projection.Star,
          "orders",
          Some(Expr.Cmp(CmpOp.Eq, Expr.Col("customer"), Expr.Lit(Literal.StrLit("Alice")))),
          Some(OrderBy("_key", descending = true)),
          Some(10L)
        )
      )
    },
    test("COUNT(*) and COUNT(col) projections; bare 'count' stays a column") {
      assertTrue(
        ok("select count(*) from t")            == Statement.Select(Projection.Count(None), "t", None, None, None),
        ok("SELECT COUNT(*) FROM t WHERE a > 1") == Statement.Select(Projection.Count(None), "t", Some(Expr.Cmp(CmpOp.Gt, Expr.Col("a"), Expr.Lit(Literal.IntLit(1)))), None, None),
        ok("select count(amount) from t")        == Statement.Select(Projection.Count(Some("amount")), "t", None, None, None),
        ok("select count from t")                == Statement.Select(Projection.Columns(List("count")), "t", None, None, None)
      )
    },
    test("projection columns and AND/OR/comparison precedence") {
      val s = ok("SELECT _key, amount FROM t WHERE a > 1 AND b < 2 OR c >= 3")
      val expectedWhere =
        Expr.Or(
          Expr.And(
            Expr.Cmp(CmpOp.Gt, Expr.Col("a"), Expr.Lit(Literal.IntLit(1))),
            Expr.Cmp(CmpOp.Lt, Expr.Col("b"), Expr.Lit(Literal.IntLit(2)))
          ),
          Expr.Cmp(CmpOp.Ge, Expr.Col("c"), Expr.Lit(Literal.IntLit(3)))
        )
      assertTrue(s == Statement.Select(Projection.Columns(List("_key", "amount")), "t", Some(expectedWhere), None, None))
    },
    test("decimal, boolean, null literals; IS NULL and LIKE") {
      assertTrue(
        ok("select * from t where p = 9.99")     == Statement.Select(Projection.Star, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("p"), Expr.Lit(Literal.DecLit(BigDecimal("9.99"))))), None, None),
        ok("select * from t where note is null")  == Statement.Select(Projection.Star, "t", Some(Expr.IsNull(Expr.Col("note"), negated = false)), None, None),
        ok("select * from t where note is not null") == Statement.Select(Projection.Star, "t", Some(Expr.IsNull(Expr.Col("note"), negated = true)), None, None),
        ok("select * from t where name like 'A%'") == Statement.Select(Projection.Star, "t", Some(Expr.Like(Expr.Col("name"), "A%")), None, None)
      )
    },
    test("INSERT / UPDATE / DELETE") {
      assertTrue(
        ok("insert into orders (_key, customer) values ('ORD-1', 'Alice')")
          == Statement.Insert("orders", List("_key", "customer"), List(Literal.StrLit("ORD-1"), Literal.StrLit("Alice"))),
        ok("update orders set customer = 'Bob' where _key = 'ORD-1'")
          == Statement.Update("orders", List(("customer", Literal.StrLit("Bob"))), Some(Expr.Cmp(CmpOp.Eq, Expr.Col("_key"), Expr.Lit(Literal.StrLit("ORD-1"))))),
        ok("delete from orders where _key = 'ORD-1'")
          == Statement.Delete("orders", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("_key"), Expr.Lit(Literal.StrLit("ORD-1")))))
      )
    },
    test("DESCRIBE and SHOW; quoted-string escaping; trailing semicolon") {
      assertTrue(
        ok("describe orders")        == Statement.Describe("orders"),
        ok("show collections;")      == Statement.Show(ShowTarget.Collections),
        ok("show indexes")           == Statement.Show(ShowTarget.Indexes),
        ok("select * from t where s = 'it''s'") == Statement.Select(Projection.Star, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("s"), Expr.Lit(Literal.StrLit("it's")))), None, None)
      )
    },
    test("a malformed statement reports a parse error") {
      assertTrue(SqlParser.parse("select from where").isLeft, SqlParser.parse("not sql at all").isLeft)
    }
  )
}
