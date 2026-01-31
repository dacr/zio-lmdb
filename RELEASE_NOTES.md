# ZIO-LMDB RELEASE NOTES

## 2.3 - 2025-02-01

- generic `keycodecs` module added 
- add specialized `keycodecs` for
  - latitude/longitude codec using Morton coding (8 bytes)
  - timestamps codec using nanoseconds precision (12 bytes)
  - UUIDv7 codec (16 bytes)
  - ULID codec (16 bytes)
- `LMDBKodec` renamed to `KeyCodec` and moved to the `keycodecs` module

## 2.2 - 2025-01-30

- transactions official support added
- index collections support added
- optimized storage for UUID keys provided
- improved performance
- switch to multi-module project structure

## 2.1 - 2025-07-18

- add support for customizable key types within the API
- introduce KeyCodec type class for key serialization/deserialization
- refactor API to use generic key type parameter instead of String only
- maintain backward compatibility with String keys
- enhance type safety and flexibility for key handling

## 2.0 - 2025-04-07

- support custom serialization layer using type class
- provide default json serialization layer using zio json
- support derivation for serialization auto-configuration
- enhance streaming internals
- drop scala 2.13 support

## 1.8 - 2024-01-21

- dependency updates
- add update operation
  - `def update(key: RecordKey, modifier: T => T): IO[UpdateErrors, Option[T]]`
  - will return None if no record was found
- change upsert method signature to return the updated/inserted record (instead of Unit previously)
  - `def upsert(key: RecordKey, modifier: Option[T] => T): IO[UpsertErrors, T]`
  - now the updated or inserted record is returned

## 1.7 - 2024-01-01

- upgrade to lmdb-java 1.9.0
- update dependencies
- add collectionDrop operation to delete a collection
- add the failIfExists parameter to collectionCreate
  - simplify API usage for various use cases
- enhance collect / stream / streamWithKey (#19)
  - in forward or backward key ordering
  - start after/before a given key
- do not display logs during unit test execution
- add more unit tests

## 1.5 - 2023-09-24

- add collection head, previous, next, last record operations (#18)
- update scala releases
- update dependencies

## 1.4 - 2023-08-25

- Add stream operations (#13)

## 1.3 - 2023-08-05

- `UpsertOverwrite` now doesn't care about the json definition of the previous stored value (#6)
- Change `upsert` & `upsertOverwrite` return type (#12)
    - `Unit` instead of `UpsertState`
    - `UpsertState` data type has been removed
- Add collection `contains` key operation

## 1.2 - 2023-06-17

- Add collection `clear` all content operation (#7)
