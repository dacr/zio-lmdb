pomIncludeRepository := { _ => false }

releaseCrossBuild := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
publishMavenStyle := true
Test / publishArtifact := false
publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging)

Global / PgpKeys.useGpg := true      // workaround with pgp and sbt 1.2.x
pgpSecretRing := pgpPublicRing.value // workaround with pgp and sbt 1.2.x

pomExtra in Global := {
  <developers>
    <developer>
      <id>dacr</id>
      <name>David Crosson</name>
      <url>https://github.com/dacr</url>
    </developer>
  </developers>
}

releaseTagComment := s"Releasing ${(ThisBuild / version).value}"
releaseCommitMessage := s"Setting version to ${(ThisBuild / version).value}"
releaseNextCommitMessage := s"[ci skip] Setting version to ${(ThisBuild / version).value}"

import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  //runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges
)
