{
  description = "AI Coding Environment with Gemini 3";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux"; # Change to "aarch64-darwin" for Apple Silicon
      pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          opencode      # The AI Agent
          gemini-cli    # The Auth Bridge
          nodejs_22     # Required for the auth plugin
          
          # Scala Development
          jdk21         # Java Runtime
          sbt           # Build Tool
          mill          # Build Tool
          scalafmt      # Formatter
        ];

        shellHook = ''
          echo "🤖 Gemini 3 Dev Environment Loaded"
          echo "Run 'gemini' to sync your Pro subscription if not already logged in."
        '';
      };
    };
}