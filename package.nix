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
  vendorHash = "sha256-vBINr4CD0AXLkm+BsyLckaCXkvmmQOlKmIGcNUL7y6E=";
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
