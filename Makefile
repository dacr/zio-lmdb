all: readme-test unit-tests

dependency-check:
	sbt dependencyUpdates

unit-tests:
	sbt test

readme-test:
	scala-cli README.md

sql-assembly:
	sbt sql/assembly

sql-console:
	java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -jar sql/target/scala-*/zio-lmdb-sql.jar

performance-test:
	sbt "testOnly zio.lmdb.LMDBPerformanceProtoBufSpec zio.lmdb.LMDBPerformanceJsonSpec"
