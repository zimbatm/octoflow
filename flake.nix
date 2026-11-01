{
  description = "octoflow: GitHub events into NATS JetStream, S3 and SSE";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-unstable";
    blueprint.url = "github:numtide/blueprint";
    blueprint.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = inputs: 
    let
      # checks are built by nixbot.numtide.com, which has x86_64-linux
      # builders; the VM test needs Linux anyway
      blueprintOutputs = inputs.blueprint {
        inherit inputs;
        systems = [ "x86_64-linux" ];
      };
    in
    blueprintOutputs // {
      devShells = inputs.nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ] (system:
        let
          pkgs = inputs.nixpkgs.legacyPackages.${system};
        in
        {
          default = import ./devshell.nix { inherit pkgs inputs; };
        }
      );
    };
}
