# ZIO-LMDB RELEASE NOTES

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
