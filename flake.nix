{
  description = "AI Coding Environment with ClaudeCode or OpenCode";

  inputs = {
    nixstable.url      = "github:NixOS/nixpkgs/nixos-26.05";
    nixunstable.url    = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url    = "github:numtide/flake-utils";
  };
  outputs = { self, nixstable, nixunstable, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        stable = import nixstable {
          inherit system;
          config.allowUnfree = true;
        };
        unstable = import nixunstable {
          inherit system;
          config.allowUnfree = true;
        };

        jdk = stable.jdk21;

        sbt = stable.sbt.override {
          jre = jdk;
        };
        scl = unstable.scala-cli.override {
          jre = jdk;
        };
        mvn = stable.maven.override {
          jdk_headless = jdk;
        };
        mill = unstable.mill.override {
          jre = jdk;
        };
      in
      {
        devShells.default = stable.mkShell {
          packages = [
            unstable.opencode
            unstable.claude-code

            # Scala Development
            jdk              # Java Runtime
            sbt           # Build Tool
            mill          # Build Tool
            scl              # Build Tool
            stable.scalafmt  # Formatter
            stable.protobuf  # Provides native protoc compiler
          ];

          shellHook = ''
            echo "🤖 Dev Environment Loaded"
          '';
        };
      }
    );
}