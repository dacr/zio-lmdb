pomIncludeRepository   := { _ => false }
publishMavenStyle      := true
Test / publishArtifact := false
releaseCrossBuild      := true
versionScheme          := Some("semver-spec")

// -----------------------------------------------------------------------------
ThisBuild / sonatypeCredentialHost := Sonatype.sonatypeCentralHost

ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

ThisBuild / credentials ++= (for {
  username <- sys.env.get("SONATYPE_USERNAME")
  password <- sys.env.get("SONATYPE_PASSWORD")
} yield Credentials("Sonatype Nexus Repository Manager", "central.sonatype.org", username, password))

// -----------------------------------------------------------------------------
releasePublishArtifactsAction := PgpKeys.publishSigned.value

// -----------------------------------------------------------------------------
releaseTagComment        := s"Releasing ${(ThisBuild / version).value}"
releaseCommitMessage     := s"Setting version to ${(ThisBuild / version).value}"
releaseNextCommitMessage := s"[ci skip] Setting version to ${(ThisBuild / version).value}"

// -----------------------------------------------------------------------------
import ReleaseTransformations.*
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  //releaseStepCommand("publishSigned"),
  releaseStepCommand("sonaRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)
