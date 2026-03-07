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

package zio.lmdb.query

import zio._
import zio.test._
import zio.test.Assertion._
import zio.lmdb._
import zio.lmdb.json.LMDBCodecJson.given
import zio.json._
import zio.lmdb.query.QueryBuilder._

object QueryBuilderSpec extends ZIOSpecDefault {

  case class User(id: String, name: String, age: Int, active: Boolean)
  object User {
    implicit val codec: JsonCodec[User] = DeriveJsonCodec.gen[User]
  }

  case class Post(id: String, authorId: String, title: String)
  object Post {
    implicit val codec: JsonCodec[Post] = DeriveJsonCodec.gen[Post]
  }

  case class Comment(id: String, postId: String, text: String)
  object Comment {
    implicit val codec: JsonCodec[Comment] = DeriveJsonCodec.gen[Comment]
  }

  val users = List(
    User("1", "Alice", 25, true),
    User("2", "Bob", 30, false),
    User("3", "Charlie", 35, true),
    User("4", "Diana", 28, true),
    User("5", "Eve", 40, false)
  )

  val posts = List(
    Post("p1", "1", "Alice's first post"),
    Post("p2", "1", "Alice's second post"),
    Post("p3", "3", "Charlie's post"),
    Post("p4", "4", "Diana's post")
  )

  val comments = List(
    Comment("c1", "p1", "Great post!"),
    Comment("c2", "p1", "I agree"),
    Comment("c3", "p3", "Nice one Charlie"),
    Comment("c4", "p4", "Hello Diana")
  )

  def spec = suite("QueryBuilderSpec")(
    test("can query all records") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionCreate[String, User]("users", failIfExists = false)
        _    <- ZIO.foreachDiscard(users)(u => col.upsertOverwrite(u.id, u))

        results <- col.query.toList
      } yield assertTrue(results.size == 5)
    },
    test("can filter by value") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereValue(_.age > 30).toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Charlie", "Eve")
      )
    },
    test("can filter by key") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereKey(k => k == "1" || k == "3").toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Alice", "Charlie")
      )
    },
    test("can combine filters") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query
                     .whereValue(_.active)
                     .whereValue(_.age < 30)
                     .toList
      } yield assertTrue(
        results.size == 2,
        results.map(_.name).toSet == Set("Alice", "Diana")
      )
    },
    test("can limit results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.limit(2).toList
      } yield assertTrue(results.size == 2)
    },
    test("can reverse results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.reverse().toList
      } yield assertTrue(
        results.head.id == "5",
        results.last.id == "1"
      )
    },
    test("can stream results") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.whereValue(_.active).toStream.runCollect
      } yield assertTrue(
        results.size == 3,
        results.map(_.name).toSet == Set("Alice", "Charlie", "Diana")
      )
    },
    test("can stream with keys") {
      for {
        lmdb <- ZIO.service[LMDB]
        col  <- lmdb.collectionGet[String, User]("users")

        results <- col.query.limit(2).toStreamWithKeys.runCollect
      } yield assertTrue(
        results.size == 2,
        results.head._1 == "1",
        results.head._2.name == "Alice"
      )
    },
    test("can join by key (many-to-one)") {
      for {
        lmdb     <- ZIO.service[LMDB]
        usersCol <- lmdb.collectionGet[String, User]("users")
        postsCol <- lmdb.collectionCreate[String, Post]("posts", failIfExists = false)
        _        <- ZIO.foreachDiscard(posts)(p => postsCol.upsertOverwrite(p.id, p))

        // Find all posts and join with their authors
        results <- postsCol.query
                     .joinByKey(usersCol)(_.authorId)
                     .toList
      } yield assertTrue(
        results.size == 4,
        results.exists { case (post, user) => post.id == "p1" && user.name == "Alice" },
        results.exists { case (post, user) => post.id == "p3" && user.name == "Charlie" }
      )
    },
    test("can join by index (one-to-many)") {
      for {
        lmdb     <- ZIO.service[LMDB]
        usersCol <- lmdb.collectionGet[String, User]("users")
        postsCol <- lmdb.collectionGet[String, Post]("posts")

        // Create an index mapping authorId to postId
        authorToPostIdx <- lmdb.indexCreate[String, String]("author_to_post", failIfExists = false)
        _               <- ZIO.foreachDiscard(posts)(p => authorToPostIdx.index(p.authorId, p.id))

        // Find active users and join with their posts
        results <- usersCol.query
                     .whereValue(_.active)
                     .joinByIndex(postsCol, authorToPostIdx)(_.id)
                     .toList
      } yield assertTrue(
        results.size == 4, // Alice has 2, Charlie has 1, Diana has 1
        results.count { case (user, _) => user.name == "Alice" } == 2,
        results.count { case (user, _) => user.name == "Charlie" } == 1,
        results.count { case (user, _) => user.name == "Diana" } == 1,
        results.count { case (user, _) => user.name == "Bob" } == 0 // Bob is inactive
      )
    },
    test("can chain joins by key (3 collections)") {
      for {
        lmdb        <- ZIO.service[LMDB]
        usersCol    <- lmdb.collectionGet[String, User]("users")
        postsCol    <- lmdb.collectionGet[String, Post]("posts")
        commentsCol <- lmdb.collectionCreate[String, Comment]("comments", failIfExists = false)
        _           <- ZIO.foreachDiscard(comments)(c => commentsCol.upsertOverwrite(c.id, c))

        // Find all comments, join with their posts, then join with the post authors
        results <- commentsCol.query
                     .joinByKey(postsCol)(_.postId)
                     .joinByKey(usersCol) { case (_, post) => post.authorId }
                     .toList
      } yield assertTrue(
        results.size == 4,
        results.exists { case ((comment, post), user) => comment.id == "c1" && post.id == "p1" && user.name == "Alice" },
        results.exists { case ((comment, post), user) => comment.id == "c3" && post.id == "p3" && user.name == "Charlie" }
      )
    },
    test("can chain joins by index (3 collections)") {
      for {
        lmdb        <- ZIO.service[LMDB]
        usersCol    <- lmdb.collectionGet[String, User]("users")
        postsCol    <- lmdb.collectionGet[String, Post]("posts")
        commentsCol <- lmdb.collectionGet[String, Comment]("comments")

        authorToPostIdx  <- lmdb.indexGet[String, String]("author_to_post")
        postToCommentIdx <- lmdb.indexCreate[String, String]("post_to_comment", failIfExists = false)
        _                <- ZIO.foreachDiscard(comments)(c => postToCommentIdx.index(c.postId, c.id))

        // Find active users, join with their posts, then join with the post comments
        results <- usersCol.query
                     .whereValue(_.active)
                     .joinByIndex(postsCol, authorToPostIdx)(_.id)
                     .joinByIndex(commentsCol, postToCommentIdx) { case (_, post) => post.id }
                     .toList
      } yield assertTrue(
        results.size == 4, // Alice has 2 comments on p1, Charlie has 1 on p3, Diana has 1 on p4
        results.count { case ((user, _), _) => user.name == "Alice" } == 2,
        results.count { case ((user, _), _) => user.name == "Charlie" } == 1,
        results.count { case ((user, _), _) => user.name == "Diana" } == 1,
        results.exists { case ((_, post), comment) => post.id == "p1" && comment.text == "Great post!" }
      )
    }
  ).provideShared(
    Scope.default,
    LMDB.liveWithDatabaseName("query-builder-test-db")
  ) @@ TestAspect.sequential
}
