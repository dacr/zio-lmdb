all: readme-test unit-tests

dependency-check:
	sbt dependencyUpdates

unit-tests:
	sbt test

readme-test:
	scala-cli README.md

console-assembly:
	sbt console/assembly

performance-test:
	sbt "testOnly zio.lmdb.LMDBPerformanceProtoBufSpec zio.lmdb.LMDBPerformanceJsonSpec"
