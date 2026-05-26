---
title: Query DSL
nav_order: 8
---

# Query DSL
{: .no_toc }

The `query-dsl` module provides a **fluent query and join API** on top of `LMDBCollection` and `LMDBIndex`.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Dependency

```scala
libraryDependencies += "fr.janalyse" %% "query-dsl" % "2.8.6"
```

```scala
//> using dep fr.janalyse::query-dsl:2.8.6
```

---

## Imports

```scala
import zio.lmdb.query.QueryBuilder.*
```

---

## Collection queries

Call `.query` on any `LMDBCollection` to start building a query:

```scala
val q = myCollection.query   // QueryBuilder[K, T]
```

### Filtering

```scala
// Filter by value predicate
q.whereValue(_.active == true)

// Filter by key predicate
q.whereKey(_.startsWith("2026-"))

// Both predicates can be chained
q.whereKey(_.startsWith("2026-")).whereValue(_.active)
```

### Limiting results

```scala
q.limit(100)          // at most 100 results
q.offset(50)          // skip first 50
q.limit(25).offset(0) // pagination: page 1
```

### Ordering

```scala
q.backward            // reverse key order (descending)
```

### Executing

```scala
// Collect into a List
val results: IO[..., List[T]] = q.toList

// Stream lazily
val stream: ZStream[..., T] = q.toStream

// Count matching records
val count: IO[..., Long] = q.count
```

---

## Cross-collection joins

### joinByKey (Many-to-One)

Look up a related record in another collection using a foreign key stored in the current record.

```scala
case class Post(id: String, authorId: String, title: String) derives LMDBCodecJson
case class User(id: String, name: String)                     derives LMDBCodecJson

// Find all posts and join with their author
val postsWithAuthors: IO[..., List[(Post, Option[User])]] =
  posts.query
    .joinByKey(users)(post => post.authorId)
    .toList
```

`joinByKey` performs a left join: if the referenced key is not found in `users`, the result is `(post, None)`.

### joinByIndex (One-to-Many via LMDBIndex)

Look up related records using a secondary index.

```scala
// Find active users and all their posts
val usersWithPosts: IO[..., List[(User, List[Post])]] =
  users.query
    .whereValue(_.active)
    .joinByIndex(posts, authorToPostIndex)(user => user.id)
    .toList
```

The index `authorToPostIndex: LMDBIndex[String, String]` maps `authorId → postId`. For each user, all matching post IDs are fetched from the index and the corresponding post records are loaded.

---

## Index queries

Start a query from an `LMDBIndex` to query the index directly:

```scala
// Query the index for a specific FROM_KEY
val q = myIndex.query("electronics")   // IndexQueryBuilder[FROM_KEY, TO_KEY]
```

### Filter on target key

```scala
q.whereTargetKey(_.startsWith("p"))
```

### Join with the target collection

```scala
// Resolve the TO_KEY values into actual records
val electronics: IO[..., List[Product]] =
  categoryIndex.query("electronics")
    .join(products)
    .toList
```

---

## Full example

```scala
import zio.*, zio.json.*, zio.lmdb.*, zio.lmdb.json.LMDBCodecJson.given
import zio.lmdb.query.QueryBuilder.*

case class User(id: String, name: String, active: Boolean) derives JsonCodec
case class Post(id: String, authorId: String, title: String) derives JsonCodec

val example = for {
  lmdb     <- ZIO.service[LMDB]
  usersCol <- lmdb.collectionCreate[String, User]("users", failIfExists = false)
  postsCol <- lmdb.collectionCreate[String, Post]("posts", failIfExists = false)

  // Seed data
  _ <- usersCol.upsertOverwrite("u1", User("u1", "Alice", true))
  _ <- usersCol.upsertOverwrite("u2", User("u2", "Bob",   false))

  // 1. Simple filter
  activeUsers <- usersCol.query.whereValue(_.active).toList
  _ <- Console.printLine(s"Active users: $activeUsers")

  // 2. Many-to-One join: posts → author
  postsWithAuthors <- postsCol.query
                        .joinByKey(usersCol)(_.authorId)
                        .toList
  _ <- Console.printLine(s"Posts with authors: $postsWithAuthors")

  // 3. One-to-Many join via index
  authorToPostIdx <- lmdb.indexCreate[String, String]("author_to_post", failIfExists = false)
  postsWithIdx     = postsCol.withIndex(authorToPostIdx)(post => List(post.authorId))

  _ <- postsWithIdx.upsertOverwrite("p1", Post("p1", "u1", "Alice's first post"))
  _ <- postsWithIdx.upsertOverwrite("p2", Post("p2", "u1", "Alice's second post"))

  usersWithPosts <- usersCol.query
                     .whereValue(_.active)
                     .joinByIndex(postsWithIdx, authorToPostIdx)(_.id)
                     .toList
  _ <- Console.printLine(s"Users with posts: $usersWithPosts")

  // 4. Start query from the index
  alicePosts <- authorToPostIdx.query("u1")
                  .whereTargetKey(_.startsWith("p"))
                  .join(postsCol)
                  .toList
  _ <- Console.printLine(s"Alice's posts via index: $alicePosts")
} yield ()
```
