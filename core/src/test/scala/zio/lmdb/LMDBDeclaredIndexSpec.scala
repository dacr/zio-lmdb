package zio.lmdb

import zio.*
import zio.lmdb.json.LMDBCodecJson
import zio.test.*
import zio.test.TestAspect.*

case class DeclaredIndexEvent(label: String, priority: Long, tag: Option[String]) derives LMDBCodecJson

object LMDBDeclaredIndexSpec extends ZIOSpecDefault with Commons {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] = logger >>> testEnvironment

  private val metaName = LMDBConfig.default.metaDataCollectionName

  override def spec = suite("LMDB Declared Index")(
    test("maintains the index like withIndexFull and persists the mapping") {
      for {
        byName   <- LMDB.indexCreate[String, String]("declared_users_by_name")
        usersRaw <- LMDB.collectionCreate[String, SimpleUser]("declared_users")
        users    <- usersRaw.withDeclaredIndex(byName)(
                      from = IdxKey.of(IdxKey.field("name")((_, u: SimpleUser) => u.name)),
                      to = IdxKey.of(IdxKey.primaryKey)
                    )
        _        <- users.upsertOverwrite("user1", SimpleUser("Alice"))
        _        <- users.upsertOverwrite("user2", SimpleUser("Bob"))
        indexed  <- byName.indexed("Alice").runCollect
        _        <- users.update("user1", _ => SimpleUser("Alicia"))
        reold    <- byName.indexed("Alice").runCollect
        renew    <- byName.indexed("Alicia").runCollect
        meta     <- LMDB.fetch[String, MetaDataEntry](metaName, "declared_users_by_name")
      } yield assertTrue(
        indexed == Chunk(("Alice", "user1")),
        reold.isEmpty,
        renew == Chunk(("Alicia", "user1")),
        meta
          .flatMap(_.indexMapping)
          .contains(
            IndexMapping(
              sourceCollection = "declared_users",
              fromComponents = List(IndexComponent(IndexComponentSource.Field("name"), "lmdb:str")),
              toComponents = List(IndexComponent(IndexComponentSource.PrimaryKey, "lmdb:str"))
            )
          )
      )
    },
    test("tuple keys and optional fields: absent component leaves the record unindexed") {
      for {
        byPriority <- LMDB.indexCreate[(Long, String), String]("declared_events_by_priority")
        byTag      <- LMDB.indexCreate[String, String]("declared_events_by_tag")
        eventsRaw  <- LMDB.collectionCreate[String, DeclaredIndexEvent]("declared_events")
        events0    <- eventsRaw.withDeclaredIndex(byPriority)(
                        from = IdxKey.tuple(IdxKey.field("priority")((_, e: DeclaredIndexEvent) => e.priority), IdxKey.primaryKey),
                        to = IdxKey.of(IdxKey.primaryKey)
                      )
        events     <- events0.withDeclaredIndex(byTag)(
                        from = IdxKey.of(IdxKey.fieldOpt("tag")((_, e: DeclaredIndexEvent) => e.tag)),
                        to = IdxKey.of(IdxKey.primaryKey)
                      )
        _          <- events.upsertOverwrite("e1", DeclaredIndexEvent("first", 10L, Some("hot")))
        _          <- events.upsertOverwrite("e2", DeclaredIndexEvent("second", 5L, None))
        byPrio     <- byPriority.indexed((5L, "e2")).runCollect
        tagged     <- byTag.indexed("hot").runCollect
        untagged   <- byTag.indexed("cold").runCollect
        meta       <- LMDB.fetch[String, MetaDataEntry](metaName, "declared_events_by_priority")
        mapping     = meta.flatMap(_.indexMapping)
      } yield assertTrue(
        byPrio == Chunk(((5L, "e2"), "e2")),
        tagged == Chunk(("hot", "e1")),
        untagged.isEmpty,
        mapping
          .map(_.fromComponents)
          .contains(
            List(
              IndexComponent(IndexComponentSource.Field("priority"), "lmdb:int64", Some(8)),
              IndexComponent(IndexComponentSource.PrimaryKey, "lmdb:str", None)
            )
          )
      )
    },
    test("mapping survives a subsequent indexCreate refresh and redeclaration overwrites it") {
      for {
        byName  <- LMDB.indexCreate[String, String]("declared_survive_idx")
        colRaw  <- LMDB.collectionCreate[String, SimpleUser]("declared_survive_col")
        _       <- colRaw.withDeclaredIndex(byName)(
                     from = IdxKey.of(IdxKey.field("name")((_, u: SimpleUser) => u.name)),
                     to = IdxKey.of(IdxKey.primaryKey)
                   )
        _       <- LMDB.indexCreate[String, String]("declared_survive_idx", failIfExists = false)
        kept    <- LMDB.fetch[String, MetaDataEntry](metaName, "declared_survive_idx")
        _       <- colRaw.withDeclaredIndex(byName)(
                     from = IdxKey.of(IdxKey.coalesce("name", "alias")((_, u: SimpleUser) => Some(u.name))),
                     to = IdxKey.of(IdxKey.primaryKey)
                   )
        updated <- LMDB.fetch[String, MetaDataEntry](metaName, "declared_survive_idx")
      } yield assertTrue(
        kept.flatMap(_.indexMapping).exists(_.fromComponents.head.source == IndexComponentSource.Field("name")),
        updated.flatMap(_.indexMapping).exists(_.fromComponents.head.source == IndexComponentSource.Coalesce(List("name", "alias")))
      )
    },
    test("declaring against a missing index fails with IndexNotFound") {
      for {
        result <- LMDB.indexDeclare("declared_missing_idx", IndexMapping("whatever", Nil, Nil)).either
      } yield assertTrue(result == Left(StorageUserError.IndexNotFound("declared_missing_idx")))
    }
  ).provide(lmdbLayer) @@ withLiveClock @@ withLiveRandom @@ timed
}
