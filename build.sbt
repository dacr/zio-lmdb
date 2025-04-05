name         := "zio-lmdb"
organization := "fr.janalyse"
description  := "Lightning Memory Database (LMDB) for scala ZIO"

licenses += "NON-AI-APACHE2" -> url(s"https://github.com/non-ai-licenses/non-ai-licenses/blob/main/NON-AI-APACHE2")

scalaVersion       := "3.3.5" // FOR LIBS USE SCALA LTS
crossScalaVersions := Seq("2.13.16", "3.3.5")

lazy val versions = new {
  val zio        = "2.1.17"
  val zionio     = "2.0.2"
  val ziojson    = "0.7.39"
  val zioconfig  = "4.0.4"
  val ziologging = "2.5.0"
  val lmdb       = "0.9.1"
}

libraryDependencies ++= Seq(
  "dev.zio"     %% "zio"                 % versions.zio,
  "dev.zio"     %% "zio-streams"         % versions.zio,
  "dev.zio"     %% "zio-json"            % versions.ziojson,
  "dev.zio"     %% "zio-config"          % versions.zioconfig,
  "org.lmdbjava" % "lmdbjava"            % versions.lmdb,
  "dev.zio"     %% "zio-test"            % versions.zio        % Test,
  "dev.zio"     %% "zio-logging"         % versions.ziologging % Test,
  "dev.zio"     %% "zio-test-sbt"        % versions.zio        % Test,
  "dev.zio"     %% "zio-test-scalacheck" % versions.zio        % Test,
  "dev.zio"     %% "zio-nio"             % versions.zionio     % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

excludeDependencies += "org.scala-lang.modules" % "scala-collection-compat_2.13"

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq("--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")

homepage   := Some(new URL("https://github.com/dacr/zio-lmdb"))
scmInfo    := Some(ScmInfo(url(s"https://github.com/dacr/zio-lmdb.git"), s"git@github.com:dacr/zio-lmdb.git"))
developers := List(
  Developer(
    id = "dacr",
    name = "David Crosson",
    email = "crosson.david@gmail.com",
    url = url("https://github.com/dacr")
  )
)
