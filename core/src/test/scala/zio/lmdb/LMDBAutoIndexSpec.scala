package zio.lmdb

import zio.*
import zio.test.*
import zio.test.TestAspect.*

object LMDBAutoIndexSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Auto Indexing")(
    test("automatically update index on upsertOverwrite, update, delete, and clear") {
      for {
        userIndex <- LMDB.indexCreate[String, String]("users_by_name")
        usersRaw  <- LMDB.collectionCreate[String, TxnUser]("users_auto_idx_test")
        
        users = usersRaw.withIndex(userIndex)(u => Some(u.name))

        userId1   = "user1"
        userName1 = "Alice"
        userId2   = "user2"
        userName2 = "Bob"

        // 1. Test upsertOverwrite
        _ <- users.upsertOverwrite(userId1, TxnUser(userName1))
        _ <- users.upsertOverwrite(userId2, TxnUser(userName2))

        idxAlice1 <- userIndex.indexed(userName1).runCollect
        idxBob1   <- userIndex.indexed(userName2).runCollect

        _ <- assertTrue(
               idxAlice1 == Chunk((userName1, userId1)),
               idxBob1 == Chunk((userName2, userId2))
             )

        // 2. Test update (change name)
        newUserName1 = "Alicia"
        _ <- users.update(userId1, _ => TxnUser(newUserName1))

        idxAlice2  <- userIndex.indexed(userName1).runCollect
        idxAlicia2 <- userIndex.indexed(newUserName1).runCollect

        _ <- assertTrue(
               idxAlice2.isEmpty,
               idxAlicia2 == Chunk((newUserName1, userId1))
             )

        // 3. Test delete
        _ <- users.delete(userId2)

        idxBob3 <- userIndex.indexed(userName2).runCollect

        _ <- assertTrue(
               idxBob3.isEmpty
             )

        // 4. Test clear
        _ <- users.clear()

        idxAlicia4 <- userIndex.indexed(newUserName1).runCollect

        _ <- assertTrue(
               idxAlicia4.isEmpty
             )

      } yield assertCompletes
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
