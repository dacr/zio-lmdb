import scala.sys.process._

// Safely try to find a system-installed protoc (provided by our flake)
val localProtoc = try {
  Some("which protoc".!!.trim).filter(_.nonEmpty)
} catch {
  case _: Exception => None
}

ThisBuild / scalaVersion := "3.3.7"

lazy val versions = new {
  val zio        = "2.1.26"
  val zionio     = "2.0.2"
  val zioconfig  = "4.0.7"
  val ziologging = "2.5.3"
  val lmdb       = "0.9.3"
  val airframe   = "2026.1.6"
  val scalapbrt  = scalapb.compiler.Version.scalapbVersion
  val jsoniter   = "2.38.14"
}

lazy val commonSettings = Seq(
  organization                 := "fr.janalyse",
  licenses += "NON-AI-APACHE2" -> url(s"https://github.com/non-ai-licenses/non-ai-licenses/blob/main/NON-AI-APACHE2"),
  homepage                     := Some(url("https://github.com/dacr/zio-lmdb")),
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
  .aggregate(
    core,
    keycodecs,
    keycodecsUlid,
    keycodecsUuidv7,
    keycodecsGeo,
    keycodecsTimestamp,
    keycodecsUca,
    queryDsl,
    vectorSearch,
    sql
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name                       := "zio-lmdb",
    description                := "Lightning Memory Database (LMDB) for scala ZIO",
    libraryDependencies ++= Seq(
      "dev.zio"                               %% "zio"                     % versions.zio,
      "dev.zio"                               %% "zio-streams"             % versions.zio,
      "dev.zio"                               %% "zio-config"              % versions.zioconfig,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"     % versions.jsoniter,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros"   % versions.jsoniter,
      "org.lmdbjava"                           % "lmdbjava"                % versions.lmdb,
      "dev.zio"                               %% "zio-test"                % versions.zio        % Test,
      "dev.zio"                               %% "zio-logging"             % versions.ziologging % Test,
      "dev.zio"                               %% "zio-test-sbt"            % versions.zio        % Test,
      "dev.zio"                               %% "zio-test-scalacheck"     % versions.zio        % Test,
      "dev.zio"                               %% "zio-nio"                 % versions.zionio     % Test,
      "com.thesamet.scalapb"                  %% "scalapb-runtime"         % versions.scalapbrt  % "protobuf,test"
    ),
    Test / PB.targets          := Seq(
      scalapb.gen() -> (Test / sourceManaged).value / "scalapb"
    ),
    Test / PB.protocExecutable := localProtoc.map(file).getOrElse(PB.protocExecutable.value)
  )
  .dependsOn(keycodecs)
  .dependsOn(keycodecsUuidv7 % Test)

lazy val keycodecs = (project in file("codecs/keycodecs"))
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

lazy val keycodecsGeo = (project in file("codecs/keycodecs-geo"))
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

lazy val keycodecsTimestamp = (project in file("codecs/keycodecs-timestamp"))
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

lazy val keycodecsUca = (project in file("codecs/keycodecs-uca"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-uca",
    description := "UCA Sort Key tools for ZIO LMDB",
    libraryDependencies ++= Seq(
      "com.ibm.icu" % "icu4j"               % "78.3",
      "dev.zio"    %% "zio-test"            % versions.zio % Test,
      "dev.zio"    %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio"    %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val keycodecsUlid = (project in file("codecs/keycodecs-ulid"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-ulid",
    description := "ULID support for ZIO LMDB",
    libraryDependencies ++= Seq(
      "org.wvlet.airframe" %% "airframe-ulid"       % versions.airframe,
      "dev.zio"            %% "zio-test"            % versions.zio % Test,
      "dev.zio"            %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio"            %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val keycodecsUuidv7 = (project in file("codecs/keycodecs-uuidv7"))
  .settings(commonSettings)
  .settings(
    name        := "keycodecs-uuidv7",
    description := "UUIDv7 support for ZIO LMDB",
    libraryDependencies ++= Seq(
      "com.github.f4b6a3" % "uuid-creator"        % "6.1.1",
      "dev.zio"          %% "zio-test"            % versions.zio % Test,
      "dev.zio"          %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio"          %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(keycodecs)

lazy val queryDsl = (project in file("query-dsl"))
  .settings(commonSettings)
  .settings(
    name        := "query-dsl",
    description := "Query DSL for ZIO LMDB",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(core)

lazy val vectorSearch = (project in file("vector-search"))
  .settings(commonSettings)
  .settings(
    name        := "zio-lmdb-vector",
    description := "Vector similarity search for ZIO LMDB",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"            % versions.zio % Test,
      "dev.zio" %% "zio-test-sbt"        % versions.zio % Test,
      "dev.zio" %% "zio-test-scalacheck" % versions.zio % Test
    )
  )
  .dependsOn(core)

lazy val sql = (project in file("sql"))
  .settings(commonSettings)
  .settings(
    name                             := "zio-lmdb-sql",
    description                      := "SQL REPL for ZIO LMDB",
    libraryDependencies ++= Seq(
      "org.jline"  % "jline"               % "4.1.3",
      "com.lihaoyi" %% "fastparse"          % "3.1.1",
      "dev.zio"   %% "zio"                  % versions.zio,
      "dev.zio"   %% "zio-streams"          % versions.zio,
      "dev.zio"   %% "zio-logging"          % versions.ziologging,
      "dev.zio"   %% "zio-test"             % versions.zio % Test,
      "dev.zio"   %% "zio-test-sbt"         % versions.zio % Test,
      "dev.zio"   %% "zio-test-scalacheck"  % versions.zio % Test
    ),
    assembly / mainClass             := Some("zio.lmdb.sql.repl.Main"),
    assembly / assemblyJarName       := "zio-lmdb-sql.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      case x                             =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
  .dependsOn(core, queryDsl, keycodecsUlid, keycodecsUuidv7, keycodecsGeo, keycodecsTimestamp, keycodecsUca)

homepage   := Some(url("https://github.com/dacr/zio-lmdb"))
scmInfo    := Some(ScmInfo(url(s"https://github.com/dacr/zio-lmdb.git"), s"git@github.com:dacr/zio-lmdb.git"))
developers := List(
  Developer(
    id = "dacr",
    name = "David Crosson",
    email = "crosson.david@gmail.com",
    url = url("https://github.com/dacr")
  )
)
