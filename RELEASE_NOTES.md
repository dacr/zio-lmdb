# ZIO-LMDB RELEASE NOTES

## 1.6.0 - 2023-XX-YY

- upgrade to lmdb-java 1.9.0
- update dependencies
- enhance collect / stream / streamWithKey (#19)
  - forward or backward
  - start after a given key
- do not display logs during unit test execution

## 1.5.0 - 2023-09-24

- add collection head, previous, next, last record operations (#18)
- update scala releases
- update dependencies

## 1.4.3 - 2023-09-03

- add streamWithKeys operation (#14)

## 1.4.2 - 2023-09-03

- Enhance scaladoc (#15)
- Add missing LMDB.databasePath operation

## 1.4.1 - 2023-08-25

- Update dependencies

## 1.4.0 - 2023-08-25

- Add stream operations (#13)

## 1.3.0 - 2023-08-05

- `UpsertOverwrite` now doesn't care about the json definition of the previous stored value (#6)
- Change `upsert` & `upsertOverwrite` return type (#12)
    - `Unit` instead of `UpsertState`
    - `UpsertState` data type has been removed
- Add collection `contains` key operation

## 1.2.1 - 2023-07-29

- Update dependencies

## 1.2.0 - 2023-06-17

- Add collection `clear` all content operation (#7)
