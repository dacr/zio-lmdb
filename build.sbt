ThisBuild / scalaVersion := "3.3.7"

lazy val versions = new {
  val zio        = "2.1.24"
  val zionio     = "2.0.2"
  val ziojson    = "0.8.0"
  val zioconfig  = "4.0.6"
  val ziologging = "2.5.3"
  val lmdb       = "0.9.2"
}

lazy val commonSettings = Seq(
  organization                 := "fr.janalyse",
  licenses += "NON-AI-APACHE2" -> url(s"https://github.com/non-ai-licenses/non-ai-licenses/blob/main/NON-AI-APACHE2"),
  homepage                     := Some(new URL("https://github.com/dacr/zio-lmdb")),
  scmInfo                      := Some(ScmInfo(url(s"https://github.com/dacr/zio-lmdb.git"), s"git@github.com:dacr/zio-lmdb.git")),
  developers                   := List(
    Developer(
      id = "dacr",
      name = "David Crosson",
      email = "crosson.david@gmail.com",
      url = url("https://github.com/dacr")
    )
  ),
  fork                         := true,
  javaOptions ++= Seq("--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name           := "zio-lmdb-root",
    publish / skip := true
  )
  .aggregate(core, keycodecs, keycodecsUlid, keycodecsUuidv7, keycodecsGeo, keycodecsTimestamp)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name        := "zio-lmdb",
    description := "Lightning Memory Database (LMDB) for scala ZIO",
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
  )
  .dependsOn(keycodecs)

lazy val keycodecs = (project in file("keycodecs"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs",
    description := "Key manipulation tools for ZIO LMDB",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )

lazy val keycodecsGeo = (project in file("keycodecs-geo"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-geo",
    description := "Geo location tools for ZIO LMDB",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val keycodecsTimestamp = (project in file("keycodecs-timestamp"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-timestamp",
    description := "Timestamp tools for ZIO LMDB",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val keycodecsUlid = (project in file("keycodecs-ulid"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-ulid",
    description := "ULID support for ZIO LMDB",
    libraryDependencies ++= Seq(
      "org.wvlet.airframe" %% "airframe-ulid" % "2025.1.27",
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val keycodecsUuidv7 = (project in file("keycodecs-uuidv7"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-uuidv7",
    description := "UUIDv7 support for ZIO LMDB",
    libraryDependencies ++= Seq(
      "com.github.f4b6a3" % "uuid-creator" % "6.1.1",
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

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
