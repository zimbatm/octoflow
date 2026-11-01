{ pkgs, ... }:
pkgs.buildGoModule {
  pname = "octoflow";
  version = "0.1.0";
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
  subPackages = [ "cmd/..." ];
  env.CGO_ENABLED = 0;
  # subPackages limits the default checkPhase to cmd/, which has no tests.
  checkPhase = ''
    runHook preCheck
    go test ./internal/... ./pkg/...
    runHook postCheck
  '';
  postInstall = ''
    mkdir -p $out/share/octoflow
    cp -r web $out/share/octoflow/
  '';
  meta.mainProgram = "webhook-receiver";
}
