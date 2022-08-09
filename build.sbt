organization := "fr.janalyse"
name         := "zio-lmdb"
homepage     := Some(new URL("https://github.com/dacr/zio-lmdb"))

licenses += "Apache 2" -> url(s"https://www.apache.org/licenses/LICENSE-2.0.txt")

scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/dacr/zio-lmdb.git"),
    s"git@github.com:dacr/zio-lmdb.git"
  )
)

scalaVersion := "3.1.3"

lazy val versions = new {
  val zio     = "2.0.0"
  val zionio  = "2.0.0"
  val ziojson = "0.3.0-RC10"
  val lmdb    = "0.8.2"
}

libraryDependencies ++= Seq(
  "dev.zio"     %% "zio"                 % versions.zio,
  "dev.zio"     %% "zio-streams"         % versions.zio,
  "dev.zio"     %% "zio-json"            % versions.ziojson,
  "org.lmdbjava" % "lmdbjava"            % versions.lmdb,
  "dev.zio"     %% "zio-test"            % versions.zio    % Test,
  "dev.zio"     %% "zio-test-junit"      % versions.zio    % Test,
  "dev.zio"     %% "zio-test-sbt"        % versions.zio    % Test,
  "dev.zio"     %% "zio-test-scalacheck" % versions.zio    % Test,
  "dev.zio"     %% "zio-nio"             % versions.zionio % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

excludeDependencies += "org.scala-lang.modules" % "scala-collection-compat_2.13"

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq("--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")
