organization := "fr.janalyse"
name         := "zio-lmdb"
homepage     := Some(new URL("https://github.com/dacr/zio-lmdb"))

licenses += "NON-AI-APACHE2" -> url(s"https://github.com/non-ai-licenses/non-ai-licenses/blob/main/NON-AI-APACHE2")

scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/dacr/zio-lmdb.git"),
    s"git@github.com:dacr/zio-lmdb.git"
  )
)

scalaVersion       := "3.3.0"
crossScalaVersions := Seq("2.13.11", "3.3.0")

lazy val versions = new {
  val zio       = "2.0.15"
  val zionio    = "2.0.1"
  val ziojson   = "0.6.0"
  val zioconfig = "4.0.0-RC16"
  val lmdb      = "0.8.3"
}

libraryDependencies ++= Seq(
  "dev.zio"     %% "zio"                 % versions.zio,
  "dev.zio"     %% "zio-streams"         % versions.zio,
  "dev.zio"     %% "zio-json"            % versions.ziojson,
  "dev.zio"     %% "zio-config"          % versions.zioconfig,
  "org.lmdbjava" % "lmdbjava"            % versions.lmdb,
  "dev.zio"     %% "zio-test"            % versions.zio    % Test,
  // "dev.zio"     %% "zio-test-junit"      % versions.zio    % Test,
  "dev.zio"     %% "zio-test-sbt"        % versions.zio    % Test,
  "dev.zio"     %% "zio-test-scalacheck" % versions.zio    % Test,
  "dev.zio"     %% "zio-nio"             % versions.zionio % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

excludeDependencies += "org.scala-lang.modules" % "scala-collection-compat_2.13"

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq("--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")
