ThisBuild / pomIncludeRepository   := { _ => false }
ThisBuild / publishMavenStyle      := true
ThisBuild / Test / publishArtifact := false
ThisBuild / releaseCrossBuild      := true
ThisBuild / versionScheme          := Some("semver-spec")

// -----------------------------------------------------------------------------
ThisBuild / sonatypeCredentialHost := Sonatype.sonatypeCentralHost

ThisBuild / publishTo := sonatypePublishToBundle.value

ThisBuild / credentials ++= (for {
  username <- sys.env.get("SONATYPE_USERNAME")
  password <- sys.env.get("SONATYPE_PASSWORD")
} yield Credentials("Sonatype Nexus Repository Manager", "central.sonatype.com", username, password))

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
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)