{
  description = "go-etcd-cron";

  inputs.nixpkgs.url = "nixpkgs/nixos-unstable";
  inputs.utils.url = "github:numtide/flake-utils";
  inputs.gomod2nix = {
    url = "github:nix-community/gomod2nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, utils, gomod2nix }:
  let
    targetSystems = with utils.lib.system; [
      x86_64-linux
      x86_64-darwin
      aarch64-linux
      aarch64-darwin
    ];

    repo = ./.;

    # We only source go files to have better cache hits when actively working
    # on non-go files.
    src = nixpkgs.lib.sourceFilesBySuffices ./. [ ".go" "go.mod" "go.sum" "gomod2nix.toml" ];

    version = "v0.1.0";

  in utils.lib.eachSystem targetSystems (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ gomod2nix.overlays.default ];
      };

      ci = import ./nix/ci.nix {
        inherit src repo pkgs;
        gomod2nix = (gomod2nix.packages.${system}.default);
      };

    in {
      apps = ci.apps;

      devShells.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          go
          gopls
          gotools
          go-tools
          gomod2nix.packages.${system}.default
        ];
      };
  });
}

