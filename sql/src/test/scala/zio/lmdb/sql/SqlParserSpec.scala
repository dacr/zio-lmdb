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
          List(OrderBy(Expr.Col("_key"), descending = true)),
          Some(10L)
        )
      )
    },
    test("COUNT(*) and COUNT(col) projections; bare 'count' stays a column") {
      assertTrue(
        ok("select count(*) from t")            == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, None))), false, "t", None, Nil, None, Nil, None),
        ok("SELECT COUNT(*) FROM t WHERE a > 1") == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, None))), false, "t", Some(Expr.Cmp(CmpOp.Gt, Expr.Col("a"), Expr.Lit(Literal.IntLit(1)))), Nil, None, Nil, None),
        ok("select count(amount) from t")        == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Count, Some(Expr.Col("amount"))))), false, "t", None, Nil, None, Nil, None),
        ok("select count from t")                == Statement.Select(Projection.Items(List(SelectItem.Col("count"))), false, "t", None, Nil, None, Nil, None)
      )
    },
    test("aggregates with GROUP BY, MIN/MAX/SUM/AVG, and SELECT DISTINCT") {
      assertTrue(
        ok("select customer, sum(amount), avg(amount) from orders group by customer")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("customer"), SelectItem.Agg(AggFunc.Sum, Some(Expr.Col("amount"))), SelectItem.Agg(AggFunc.Avg, Some(Expr.Col("amount"))))),
            false, "orders", None, List(Expr.Col("customer")), None, Nil, None
          ),
        ok("select min(age), max(age) from people")
          == Statement.Select(Projection.Items(List(SelectItem.Agg(AggFunc.Min, Some(Expr.Col("age"))), SelectItem.Agg(AggFunc.Max, Some(Expr.Col("age"))))), false, "people", None, Nil, None, Nil, None),
        ok("select distinct customer from orders")
          == Statement.Select(Projection.Items(List(SelectItem.Col("customer"))), true, "orders", None, Nil, None, Nil, None),
        ok("select distinct city, country from people order by country limit 5")
          == Statement.Select(Projection.Items(List(SelectItem.Col("city"), SelectItem.Col("country"))), true, "people", None, Nil, None, List(OrderBy(Expr.Col("country"), descending = false)), Some(5L))
      )
    },
    test("AS aliases and ORDER BY an alias (standard GROUP BY … ORDER BY order)") {
      assertTrue(
        ok("select cameraName, count(*) as count from originals group by cameraName order by count")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("cameraName"), SelectItem.Agg(AggFunc.Count, None, alias = Some("count")))),
            false, "originals", None, List(Expr.Col("cameraName")), None, List(OrderBy(Expr.Col("count"), descending = false)), None
          ),
        ok("select name as n from t") == Statement.Select(Projection.Items(List(SelectItem.Col("name", Some("n")))), false, "t", None, Nil, None, Nil, None)
      )
    },
    test("LENGTH function in WHERE, and HAVING with an aggregate (the full pipeline)") {
      assertTrue(
        ok("select * from t where length(name) > 0")
          == Statement.Select(
            Projection.Star, false, "t",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Func("length", List(Expr.Col("name"))), Expr.Lit(Literal.IntLit(0)))),
            Nil, None, Nil, None
          ),
        ok(
          """SELECT cameraName, count(*) AS count
            |FROM originals
            |WHERE length(cameraName) > 0
            |GROUP BY cameraName
            |HAVING count(*) > 100
            |ORDER BY count""".stripMargin
        )
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("cameraName"), SelectItem.Agg(AggFunc.Count, None, alias = Some("count")))),
            false, "originals",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Func("length", List(Expr.Col("cameraName"))), Expr.Lit(Literal.IntLit(0)))),
            List(Expr.Col("cameraName")),
            Some(Expr.Cmp(CmpOp.Gt, Expr.Aggregate(AggFunc.Count, None, false), Expr.Lit(Literal.IntLit(100)))),
            List(OrderBy(Expr.Col("count"), descending = false)),
            None
          )
      )
    },
    test("date/time functions: extraction, NOW(), and grouping by a function alias") {
      assertTrue(
        ok("select year(timestamp) from t")
          == Statement.Select(Projection.Items(List(SelectItem.Expr(Expr.Func("year", List(Expr.Col("timestamp")))))), false, "t", None, Nil, None, Nil, None),
        ok("select now() from t")
          == Statement.Select(Projection.Items(List(SelectItem.Expr(Expr.Func("now", Nil)))), false, "t", None, Nil, None, Nil, None),
        ok(
          """SELECT year(m.timestamp) AS dy, month(m.timestamp) AS dm, count(*)
            |FROM medias m
            |GROUP BY dy, dm
            |ORDER BY dy, dm""".stripMargin
        )
          == Statement.Select(
            Projection.Items(List(
              SelectItem.Expr(Expr.Func("year", List(Expr.Col("m.timestamp"))), Some("dy")),
              SelectItem.Expr(Expr.Func("month", List(Expr.Col("m.timestamp"))), Some("dm")),
              SelectItem.Agg(AggFunc.Count, None)
            )),
            false, "medias", None,
            List(Expr.Col("dy"), Expr.Col("dm")),
            None,
            List(OrderBy(Expr.Col("dy"), descending = false), OrderBy(Expr.Col("dm"), descending = false)),
            None, Some("m")
          )
      )
    },
    test("GROUP BY a function expression directly, and multi-key ORDER BY with mixed directions") {
      assertTrue(
        ok("select year(ts), count(*) from t group by year(ts)")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Func("year", List(Expr.Col("ts")))), SelectItem.Agg(AggFunc.Count, None))),
            false, "t", None, List(Expr.Func("year", List(Expr.Col("ts")))), None, Nil, None
          ),
        ok("select * from t order by a, b desc, c")
          == Statement.Select(
            Projection.Star, false, "t", None, Nil, None,
            List(OrderBy(Expr.Col("a"), descending = false), OrderBy(Expr.Col("b"), descending = true), OrderBy(Expr.Col("c"), descending = false)),
            None
          )
      )
    },
    test("JOINs: table aliases, qualified columns, INNER and LEFT") {
      assertTrue(
        ok("select s._key, c.name from sales s join customers c on s.customerId = c._key")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("s._key"), SelectItem.Col("c.name"))),
            false, "sales", None, Nil, None, Nil, None,
            Some("s"),
            List(Join(JoinType.Inner, TableRef("customers", Some("c")), Expr.Cmp(CmpOp.Eq, Expr.Col("s.customerId"), Expr.Col("c._key"))))
          ),
        ok("select * from a left join b on a.x = b._key")
          == Statement.Select(
            Projection.Star, false, "a", None, Nil, None, Nil, None, None,
            List(Join(JoinType.Left, TableRef("b", None), Expr.Cmp(CmpOp.Eq, Expr.Col("a.x"), Expr.Col("b._key"))))
          )
      )
    },
    test("nested value paths: dotted column paths beyond alias.column") {
      assertTrue(
        ok("select o.location.altitude as alt from originals o")
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("o.location.altitude", Some("alt")))),
            false, "originals", None, Nil, None, Nil, None, Some("o")
          ),
        ok("select location.altitude from originals")
          == Statement.Select(Projection.Items(List(SelectItem.Col("location.altitude"))), false, "originals", None, Nil, None, Nil, None),
        ok("select * from originals o where o.dimension.width > 1920 order by o.location.altitude desc")
          == Statement.Select(
            Projection.Star, false, "originals",
            Some(Expr.Cmp(CmpOp.Gt, Expr.Col("o.dimension.width"), Expr.Lit(Literal.IntLit(1920)))),
            Nil, None, List(OrderBy(Expr.Col("o.location.altitude"), descending = true)), None, Some("o")
          )
      )
    },
    test("geo functions: GEO_DISTANCE in SELECT/WHERE/ORDER BY, GEO_WITHIN as a predicate") {
      val dist =
        Expr.Func("geo_distance", List(Expr.Col("o.location.latitude"), Expr.Col("o.location.longitude"), Expr.Lit(Literal.DecLit(BigDecimal("48.8566"))), Expr.Lit(Literal.DecLit(BigDecimal("2.3522")))))
      assertTrue(
        ok(
          """SELECT _key, geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) AS dist
            |FROM originals o
            |WHERE geo_distance(o.location.latitude, o.location.longitude, 48.8566, 2.3522) <= 5000
            |ORDER BY dist""".stripMargin
        )
          == Statement.Select(
            Projection.Items(List(SelectItem.Col("_key"), SelectItem.Expr(dist, Some("dist")))),
            false, "originals",
            Some(Expr.Cmp(CmpOp.Le, dist, Expr.Lit(Literal.IntLit(5000)))),
            Nil, None, List(OrderBy(Expr.Col("dist"), descending = false)), None, Some("o")
          ),
        ok("select * from originals o where geo_within(o.location, 48.8566, 2.3522, 5000)")
          == Statement.Select(
            Projection.Star, false, "originals",
            Some(Expr.Func("geo_within", List(Expr.Col("o.location"), Expr.Lit(Literal.DecLit(BigDecimal("48.8566"))), Expr.Lit(Literal.DecLit(BigDecimal("2.3522"))), Expr.Lit(Literal.IntLit(5000))))),
            Nil, None, Nil, None, Some("o")
          )
      )
    },
    test("arithmetic expressions: precedence, and reuse in SELECT / WHERE / ORDER BY") {
      assertTrue(
        ok("select a + b * c from t")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Arith(ArithOp.Add, Expr.Col("a"), Expr.Arith(ArithOp.Mul, Expr.Col("b"), Expr.Col("c")))))),
            false, "t", None, Nil, None, Nil, None
          ),
        ok("select x / 1000 as km from t where x / 1000 <= 10 order by km")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Arith(ArithOp.Div, Expr.Col("x"), Expr.Lit(Literal.IntLit(1000))), Some("km")))),
            false, "t",
            Some(Expr.Cmp(CmpOp.Le, Expr.Arith(ArithOp.Div, Expr.Col("x"), Expr.Lit(Literal.IntLit(1000))), Expr.Lit(Literal.IntLit(10)))),
            Nil, None, List(OrderBy(Expr.Col("km"), descending = false)), None
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
      assertTrue(s == Statement.Select(Projection.Items(List(SelectItem.Col("_key"), SelectItem.Col("amount"))), false, "t", Some(expectedWhere), Nil, None, Nil, None))
    },
    test("decimal, boolean, null literals; IS NULL and LIKE") {
      assertTrue(
        ok("select * from t where p = 9.99")     == Statement.Select(Projection.Star, false, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("p"), Expr.Lit(Literal.DecLit(BigDecimal("9.99"))))), Nil, None, Nil, None),
        ok("select * from t where note is null")  == Statement.Select(Projection.Star, false, "t", Some(Expr.IsNull(Expr.Col("note"), negated = false)), Nil, None, Nil, None),
        ok("select * from t where note is not null") == Statement.Select(Projection.Star, false, "t", Some(Expr.IsNull(Expr.Col("note"), negated = true)), Nil, None, Nil, None),
        ok("select * from t where name like 'A%'") == Statement.Select(Projection.Star, false, "t", Some(Expr.Like(Expr.Col("name"), "A%")), Nil, None, Nil, None)
      )
    },
    test("IN / NOT IN, BETWEEN / NOT BETWEEN, and NOT LIKE predicates") {
      assertTrue(
        ok("select * from t where age in (25, 40)")
          == Statement.Select(Projection.Star, false, "t", Some(Expr.In(Expr.Col("age"), List(Expr.Lit(Literal.IntLit(25)), Expr.Lit(Literal.IntLit(40))), negated = false)), Nil, None, Nil, None),
        ok("select * from t where age not in (25, 40)")
          == Statement.Select(Projection.Star, false, "t", Some(Expr.In(Expr.Col("age"), List(Expr.Lit(Literal.IntLit(25)), Expr.Lit(Literal.IntLit(40))), negated = true)), Nil, None, Nil, None),
        ok("select * from t where age between 18 and 65")
          == Statement.Select(Projection.Star, false, "t", Some(Expr.Between(Expr.Col("age"), Expr.Lit(Literal.IntLit(18)), Expr.Lit(Literal.IntLit(65)), negated = false)), Nil, None, Nil, None),
        ok("select * from t where age not between 18 and 65")
          == Statement.Select(Projection.Star, false, "t", Some(Expr.Between(Expr.Col("age"), Expr.Lit(Literal.IntLit(18)), Expr.Lit(Literal.IntLit(65)), negated = true)), Nil, None, Nil, None),
        ok("select * from t where name not like 'A%'")
          == Statement.Select(Projection.Star, false, "t", Some(Expr.Not(Expr.Like(Expr.Col("name"), "A%"))), Nil, None, Nil, None),
        // BETWEEN's AND binds tighter than a surrounding boolean AND
        ok("select * from t where age between 18 and 65 and active = true")
          == Statement.Select(
            Projection.Star, false, "t",
            Some(Expr.And(
              Expr.Between(Expr.Col("age"), Expr.Lit(Literal.IntLit(18)), Expr.Lit(Literal.IntLit(65)), negated = false),
              Expr.Cmp(CmpOp.Eq, Expr.Col("active"), Expr.Lit(Literal.BoolLit(true)))
            )),
            Nil, None, Nil, None
          )
      )
    },
    test("CASE expressions: searched and simple forms") {
      assertTrue(
        ok("select case when age >= 18 then 'adult' else 'minor' end from t")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Case(
              None,
              List((Expr.Cmp(CmpOp.Ge, Expr.Col("age"), Expr.Lit(Literal.IntLit(18))), Expr.Lit(Literal.StrLit("adult")))),
              Some(Expr.Lit(Literal.StrLit("minor")))
            )))),
            false, "t", None, Nil, None, Nil, None
          ),
        ok("select case status when 1 then 'a' when 2 then 'b' end from t")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Case(
              Some(Expr.Col("status")),
              List((Expr.Lit(Literal.IntLit(1)), Expr.Lit(Literal.StrLit("a"))), (Expr.Lit(Literal.IntLit(2)), Expr.Lit(Literal.StrLit("b")))),
              None
            )))),
            false, "t", None, Nil, None, Nil, None
          )
      )
    },
    test("aggregate arguments may be expressions, and COUNT(DISTINCT col)") {
      assertTrue(
        ok("select sum(a + b), count(distinct customer) from t")
          == Statement.Select(
            Projection.Items(List(
              SelectItem.Agg(AggFunc.Sum, Some(Expr.Arith(ArithOp.Add, Expr.Col("a"), Expr.Col("b"))), false),
              SelectItem.Agg(AggFunc.Count, Some(Expr.Col("customer")), true)
            )),
            false, "t", None, Nil, None, Nil, None
          )
      )
    },
    test("CAST desugars to the cast function with the target type as a string argument") {
      assertTrue(
        ok("select cast(age as integer) from t")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Func("cast", List(Expr.Col("age"), Expr.Lit(Literal.StrLit("integer"))))))),
            false, "t", None, Nil, None, Nil, None
          ),
        ok("select cast(amount as string) as s from t")
          == Statement.Select(
            Projection.Items(List(SelectItem.Expr(Expr.Func("cast", List(Expr.Col("amount"), Expr.Lit(Literal.StrLit("string")))), Some("s")))),
            false, "t", None, Nil, None, Nil, None
          )
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
        ok("select * from t where s = 'it''s'") == Statement.Select(Projection.Star, false, "t", Some(Expr.Cmp(CmpOp.Eq, Expr.Col("s"), Expr.Lit(Literal.StrLit("it's")))), Nil, None, Nil, None)
      )
    },
    test("a malformed statement reports a parse error") {
      assertTrue(SqlParser.parse("select from where").isLeft, SqlParser.parse("not sql at all").isLeft)
    }
  )
}
