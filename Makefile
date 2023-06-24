all: readme-test unit-tests

unit-tests:
	sbt test

readme-test:
	scala-cli README.md
