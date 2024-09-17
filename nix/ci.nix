{
pkgs,
gomod2nix,
src,
repo,
}:

let
  checkgomod2nix = pkgs.writeShellApplication {
    name = "check-gomod2nix";
    runtimeInputs = [ gomod2nix ];
    text = ''
      tmpdir=$(mktemp -d)
      trap 'rm -rf -- "$tmpdir"' EXIT
      gomod2nix --dir . --outdir "$tmpdir"
      if ! diff -q "$tmpdir/gomod2nix.toml" "./gomod2nix.toml"; then
        echo ">> gomod2nix.toml is not up to date. Please run:"
        echo ">> $ nix run .#update"
        exit 1
      fi
      echo ">> gomod2nix.toml is up to date"
    '';
  };

  update = pkgs.writeShellApplication {
    name = "update";
    runtimeInputs = with pkgs; [
      git
      gomod2nix
      protobuf
      go-protobuf
    ];
    text = ''
      cd "$(git rev-parse --show-toplevel)"
      echo ">> updating gomod2nix.toml"
      gomod2nix
      echo ">> updating proto files"
      protoc --go_out=. \
        --go_opt=module=github.com/diagridio/go-etcd-cron \
        --go-grpc_out=. \
        --go-grpc_opt=require_unimplemented_servers=false,module=github.com/diagridio/go-etcd-cron \
        ./proto/*.proto
      echo ">> Updated. Please commit the changes."
    '';
  };

  test = pkgs.writeShellApplication {
    name = "test";
    runtimeInputs = with pkgs; [
      git
      golangci-lint
      go
      checkgomod2nix
    ];
    text = ''
      cd "$(git rev-parse --show-toplevel)"
      echo ">> running check-gomod2nix ${repo}"
      check-gomod2nix ${repo}
      echo ">> running golangci-lint"
      golangci-lint run --enable-all
      echo ">> running go test --race -v -count 1 ./..."
      go test --race -v -count 1 ./...
    '';
  };

in {
  apps = {
    update = {type = "app"; program = "${update}/bin/update";};
    test = {type = "app"; program = "${test}/bin/test";};
  };
}

