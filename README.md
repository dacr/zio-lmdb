# Lightning Memory Database (LMDB) for ZIO

## Usage example

```scala
test("basic usage")(
  for {
    _        <- LMDB.databaseCreate("example")
    record    = Record("John Doe", 42)
    recordId <- Random.nextUUID.map(_.toString)
    _        <- LMDB.upsertOverwrite("example", recordId, record)
    gotten   <- LMDB.fetch[Record]("example", recordId).some
    _        <- LMDB.delete("example", recordId)
    deleted  <- LMDB.fetch[Record]("example", recordId)
  } yield assertTrue(
    gotten == record,
    deleted.isEmpty
  )
)
```

## Requirements

When LVMDB is used for as persistence store with recent JVM it requires JVM some options :

```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```