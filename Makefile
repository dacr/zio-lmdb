all: test

test:
	sbt test
	scala-cli README.md
