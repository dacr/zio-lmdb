ThisBuild / pomIncludeRepository   := { _ => false }
ThisBuild / publishMavenStyle      := true
ThisBuild / Test / publishArtifact := false
ThisBuild / releaseCrossBuild      := true
ThisBuild / versionScheme          := Some("semver-spec")

// -----------------------------------------------------------------------------
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

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
  releaseStepCommand("publishSigned"),
  releaseStepCommand("sonaRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)