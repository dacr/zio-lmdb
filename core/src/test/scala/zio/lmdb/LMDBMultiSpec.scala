package zio.lmdb

import zio._
import zio.test._
import zio.test.Assertion._
import zio.lmdb.json.LMDBCodecJson
import zio.lmdb.keycodecs.KeyCodec

object LMDBMultiSpec extends ZIOSpecDefault with Commons {

  val spec = suite("LMDBMultiSpec")(
    test("create and get multi collection") {
      for {
        _ <- LMDB.multiCreate[String, Person]("people")
        _ <- LMDB.multiGet[String, Person]("people")
      } yield assertCompletes
    },
    test("put and fetch multiple values for the same key") {
      for {
        col <- LMDB.multiCreate[String, Person]("people2")
        _   <- col.put("group1", Person("Alice", 30))
        _   <- col.put("group1", Person("Bob", 25))
        _   <- col.put("group2", Person("Charlie", 40))
        
        group1 <- col.fetch("group1")
        group2 <- col.fetch("group2")
        group3 <- col.fetch("group3")
      } yield assert(group1)(hasSameElements(List(Person("Alice", 30), Person("Bob", 25)))) &&
        assert(group2)(hasSameElements(List(Person("Charlie", 40)))) &&
        assert(group3)(isEmpty)
    },
    test("delete specific value") {
      for {
        col <- LMDB.multiCreate[String, Person]("people3")
        _   <- col.put("group1", Person("Alice", 30))
        _   <- col.put("group1", Person("Bob", 25))
        
        deleted1 <- col.delete("group1", Person("Alice", 30))
        deleted2 <- col.delete("group1", Person("Charlie", 40)) // Not in collection
        
        group1 <- col.fetch("group1")
      } yield assert(deleted1)(isTrue) &&
        assert(deleted2)(isFalse) &&
        assert(group1)(hasSameElements(List(Person("Bob", 25))))
    },
    test("delete all values for a key") {
      for {
        col <- LMDB.multiCreate[String, Person]("people4")
        _   <- col.put("group1", Person("Alice", 30))
        _   <- col.put("group1", Person("Bob", 25))
        _   <- col.put("group2", Person("Charlie", 40))
        
        deleted1 <- col.deleteAll("group1")
        deleted2 <- col.deleteAll("group3") // Not in collection
        
        group1 <- col.fetch("group1")
        group2 <- col.fetch("group2")
      } yield assert(deleted1)(isTrue) &&
        assert(deleted2)(isFalse) &&
        assert(group1)(isEmpty) &&
        assert(group2)(hasSameElements(List(Person("Charlie", 40))))
    },
    test("size and clear") {
      for {
        col <- LMDB.multiCreate[String, Person]("people5")
        _   <- col.put("group1", Person("Alice", 30))
        _   <- col.put("group1", Person("Bob", 25))
        _   <- col.put("group2", Person("Charlie", 40))
        
        size1 <- col.size()
        _     <- col.clear()
        size2 <- col.size()
      } yield assert(size1)(equalTo(3L)) &&
        assert(size2)(equalTo(0L))
    },
    test("transactional readWrite") {
      for {
        col <- LMDB.multiCreate[String, Person]("people6")
        _   <- col.readWrite { ops =>
                 for {
                   _ <- ops.put("group1", Person("Alice", 30))
                   _ <- ops.put("group1", Person("Bob", 25))
                 } yield ()
               }
        group1 <- col.fetch("group1")
      } yield assert(group1)(hasSameElements(List(Person("Alice", 30), Person("Bob", 25))))
    }
  ).provideLayerShared(lmdbLayer)
}