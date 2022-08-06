# Lightning Memory Database (LMDB) for ZIO

## Requirements

When LVMDB is used for as persistence store with recent JVM it requires JVM some options :
```
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```