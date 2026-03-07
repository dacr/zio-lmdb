package zio.lmdb

import zio.*
import zio.test.*
import zio.test.TestAspect.*

object LMDBAutoIndexRebuildSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  override def spec = suite("LMDB Auto Indexing Rebuild")(
    test("rebuild index from existing collection") {
      for {
        usersRaw  <- LMDB.collectionCreate[String, TxnUser]("users_rebuild_idx_test")
        
        userId1   = "user1"
        userName1 = "Alice"
        userId2   = "user2"
        userName2 = "Bob"

        // Populate collection before index exists
        _ <- usersRaw.upsertOverwrite(userId1, TxnUser(userName1))
        _ <- usersRaw.upsertOverwrite(userId2, TxnUser(userName2))

        userIndex <- LMDB.indexCreate[String, String]("users_by_name_rebuild")
        
        users = usersRaw.withIndex(userIndex)(u => Some(u.name))

        // Rebuild index
        _ <- users.rebuildIndexes()

        idxAlice <- userIndex.indexed(userName1).runCollect
        idxBob   <- userIndex.indexed(userName2).runCollect

        _ <- assertTrue(
               idxAlice == Chunk((userName1, userId1)),
               idxBob == Chunk((userName2, userId2))
             )

      } yield assertCompletes
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
