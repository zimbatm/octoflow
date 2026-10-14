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
  vendorHash = pkgs.lib.fakeHash;
  subPackages = [ "cmd/..." ];
  env.CGO_ENABLED = 0;
  postInstall = ''
    mkdir -p $out/share/octoflow
    cp -r web $out/share/octoflow/
  '';
  meta.mainProgram = "webhook-receiver";
}
