{ pkgs, ... }:
let
  version = "0.1.0";
in
pkgs.buildGoModule {
  pname = "octoflow";
  inherit version;
  ldflags = [ "-X main.version=${version}" ];
  src = pkgs.lib.fileset.toSource {
    root = ./.;
    fileset = pkgs.lib.fileset.unions [
      ./go.mod
      ./go.sum
      ./cmd
      ./internal
      ./pkg
      ./web
    ];
  };
  vendorHash = "sha256-/t+5DGpX7YeCuNvuF+E1hRNvqtW/jHkgb/MZBDrAg24=";
  subPackages = [ "cmd/octoflow" ];
  env.CGO_ENABLED = 0;
  # subPackages limits the default checkPhase to the binary; test everything.
  checkPhase = ''
    runHook preCheck
    go test ./...
    runHook postCheck
  '';
  postInstall = ''
    mkdir -p $out/share/octoflow
    cp -r web $out/share/octoflow/
  '';
  meta.mainProgram = "octoflow";
}
