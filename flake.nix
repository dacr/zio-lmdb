{
  description = "AI Coding Environment with Gemini or OpenCode";

  inputs = {
    nixstable.url      = "github:NixOS/nixpkgs/nixos-25.11";
    nixunstable.url    = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url    = "github:numtide/flake-utils";
  };
  outputs = { self, nixstable, nixunstable, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        stable = nixstable.legacyPackages.${system};
        unstable = nixunstable.legacyPackages.${system};

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
            unstable.opencode      # The AI Agent
            unstable.gemini-cli    # The Auth Bridge
            stable.nodejs_22       # Required for the auth plugin

            # Scala Development
            jdk              # Java Runtime
            sbt           # Build Tool
            mill          # Build Tool
            scl              # Build Tool
            stable.scalafmt  # Formatter
          ];

          shellHook = ''
            echo "🤖 Dev Environment Loaded"
            echo "Run 'gemini' or 'OpenCode' to sync your Pro subscription if not already logged in."
            echo "   (opencode auth login)"
          '';
        };
      }
    );
}