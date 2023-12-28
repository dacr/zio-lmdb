all: readme-test unit-tests

dependency-check:
	sbt dependencyUpdates

unit-tests:
	sbt test

readme-test:
	scala-cli README.md
