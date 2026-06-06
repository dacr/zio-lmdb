all: readme-test unit-tests

dependency-check:
	sbt dependencyUpdates

unit-tests:
	sbt test

readme-test:
	scala-cli README.md

sql-assembly:
	sbt sql/assembly

performance-test:
	sbt "testOnly zio.lmdb.LMDBPerformanceProtoBufSpec zio.lmdb.LMDBPerformanceJsonSpec"
