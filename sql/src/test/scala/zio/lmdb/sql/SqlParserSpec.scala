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
          false,
          "orders",
          Some(Expr.Cmp(CmpOp.Eq, Expr.Col("customer"), Expr.Lit(Literal.StrLit("Alice")))),
          Nil,
          None,
          Some(OrderBy("_key", descending = true)),
          Some(10L)
        )
      )
    },
    test("COUNT(*) and COUNT(col) projections; bare 'count' stays a column") {
      assertTrue(
        ok("select count(*) from t")            == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, None))), false, "t", None, Nil, None, None, None),
        ok("SELECT COUNT(*) FROM t WHERE a > 1") == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, None))), false, "t", Some(Expr.Cmp(CmpOp.Gt, Expr.Col("a"), Expr.Lit(Literal.IntLit(1)))), Nil, None, None, None),
        ok("select count(amount) from t")        == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, Some("amount")))), false, "t", None, Nil, None, None, None),
        ok("select count from t")                == Statement.Select(Projection.Items(List(SelectItem.Col("count"))), false, "t", None, Nil, None, None, None)
      )
    },
    test("aggregates with GROUP BY, MIN/MAX/SUM/AVG, and SELECT DISTINCT") {
      assertTrue(
        ok("select customer, sum(amount), avg(amount) from orders group by customer")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("customer"), SelectItem.Agg(AggFunc.Sum, Some("amount")), SelectItem.Agg(AggFunc.Avg, Some("amount")))),
            false, "orders", None, List("customer"), None, None, None
          ),
        ok("select min(age), max(age) from people")
          == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Min, Some("age")), SelectItem.Agg(AggFunc.Max, Some("age")))), false, "people", None, Nil, None, None, None),
        ok("select distinct customer from orders")
          == Statement.Select(Projection.Items(List(SelectItem.Col("customer"))), true, "orders", None, Nil, None, None, None),
        ok("select distinct city, country from people order by country limit 5")
          == Statement.Select(Projection.Items(List(SelectItem.Col("city"), SelectItem.Col("country"))), true, "people", None, Nil, None, Some(OrderBy("country", descending = false)), Some(5L))
      )
    },
    test("AS aliases and ORDER BY an alias (standard GROUP BY … ORDER BY order)") {
      assertTrue(
        ok("select cameraName, count(*) as count from originals group by cameraName order by count")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("cameraName"), SelectItem.Agg(AggFunc.Count, None, Some("count")))),
            false, "originals", None, List("cameraName"), None, Some(OrderBy("count", descending = false)), None
          ),
        ok("select name as n from t") == Statement.Select(Projection.Items(List(SelectItem.Col("name", Some("n")))), false, "t", None, Nil, None, None, None)
      )
    },
    test("LENGTH function in WHERE, and HAVING with an aggregate (the full pipeline)") {
      assertTrue(
        ok("select * from t where length(name) > 0")
          == Statement.Select(
            Projection.Star, false, "t",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Func("length", List(Expr.Col("name"))), Expr.Lit(Literal.IntLit(0)))),
            Nil, None, None, None
          ),
        ok("select cameraName, count(*) as count from originals where length(cameraName) > 0 group by cameraName having count(*) > 100 order by count")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("cameraName"), SelectItem.Agg(AggFunc.Count, None, Some("count")))),
            false, "originals",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Func("length", List(Expr.Col("cameraName"))), Expr.Lit(Literal.IntLit(0)))),
            List("cameraName"),
            Some(Expr.Cmp(CmpOp.Gt, Expr.Aggregate(AggFunc.Count, None), Expr.Lit(Literal.IntLit(100)))),
            Some(OrderBy("count", descending = false)),
            None
          )
      )
    },
    test("JOINs: table aliases, qualified columns, INNER and LEFT") {
      assertTrue(
        ok("select s._key, c.name from sales s join customers c on s.customerId = c._key")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("s._key"), SelectItem.Col("c.name"))),
            false, "sales", None, Nil, None, None, None,
            Some("s"),
            List(Join(JoinType.Inner, TableRef("customers", Some("c")), Expr.Cmp(CmpOp.Eq, Expr.Col("s.customerId"), Expr.Col("c._key"))))
          ),
        ok("select * from a left join b on a.x = b._key")
          == Statement.Select(
            Projection.Star, false, "a", None, Nil, None, None, None, None,
            List(Join(JoinType.Left, TableRef("b", None), Expr.Cmp(CmpOp.Eq, Expr.Col("a.x"), Expr.Col("b._key"))))
          )
      )
    },
    test("nested value paths: dotted column paths beyond alias.column") {
      assertTrue(
        ok("select o.location.altitude as alt from originals o")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("o.location.altitude", Some("alt")))),
            false, "originals", None, Nil, None, None, None, Some("o")
          ),
        ok("select location.altitude from originals")
          == Statement.Select(Projection.Items(List(SelectItem.Col("location.altitude"))), false, "originals", None, Nil, None, None, None),
        ok("select * from originals o where o.dimension.width > 1920 order by o.location.altitude desc")
          == Statement.Select(
            Projection.Star, false, "originals",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Col("o.dimension.width"), Expr.Lit(Literal.IntLit(1920)))),
            Nil, None, Some(OrderBy("o.location.altitude", descending = true)), None, Some("o")
          )
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
      assertTrue(s == Statement.Select(Projection.Items(List(SelectItem.Col("_key"), SelectItem.Col("amount"))), false, "t", Some(expectedWhere), Nil, None, None, None))
    },
    test("decimal, boolean, null literals; IS NULL and LIKE") {
      assertTrue(
        ok("select * from t where p = 9.99")     == Statement.Select(Projection.Star, false, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("p"), Expr.Lit(Literal.DecLit(BigDecimal("9.99"))))), Nil, None, None, None),
        ok("select * from t where note is null")  == Statement.Select(Projection.Star, false, "t", Some(Expr.IsNull(Expr.Col("note"), negated = false)), Nil, None, None, None),
        ok("select * from t where note is not null") == Statement.Select(Projection.Star, false, "t", Some(Expr.IsNull(Expr.Col("note"), negated = true)), Nil, None, None, None),
        ok("select * from t where name like 'A%'") == Statement.Select(Projection.Star, false, "t", Some(Expr.Like(Expr.Col("name"), "A%")), Nil, None, None, None)
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
        ok("select * from t where s = 'it''s'") == Statement.Select(Projection.Star, false, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("s"), Expr.Lit(Literal.StrLit("it's")))), Nil, None, None, None)
      )
    },
    test("a malformed statement reports a parse error") {
      assertTrue(SqlParser.parse("select from where").isLeft, SqlParser.parse("not sql at all").isLeft)
    }
  )
}
