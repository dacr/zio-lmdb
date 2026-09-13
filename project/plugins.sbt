addSbtPlugin("com.github.sbt"   % "sbt-release"         % "1.5.0")
addSbtPlugin("com.github.sbt"   % "sbt-pgp"             % "2.3.2")
addSbtPlugin("com.timushev.sbt" % "sbt-updates"         % "0.7.0")
addSbtPlugin("com.github.sbt"   % "sbt-native-packager" % "1.11.7")
addSbtPlugin("ch.epfl.scala"    % "sbt-scalafix"        % "0.14.8")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"        % "2.6.2")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"        % "2.5.0")
addSbtPlugin("com.thesamet"     % "sbt-protoc"          % "1.0.8")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.20"
