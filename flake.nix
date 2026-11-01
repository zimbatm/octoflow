{
  description = "octoflow: GitHub events into NATS JetStream, S3 and SSE";

  # Add all your dependencies here
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-unstable";
    blueprint.url = "github:numtide/blueprint";
    blueprint.inputs.nixpkgs.follows = "nixpkgs";
  };

  # Load the blueprint
  outputs = inputs: 
    let
      blueprintOutputs = inputs.blueprint { inherit inputs; };
    in
    blueprintOutputs // {
      devShells = inputs.nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ] (system:
        let
          pkgs = inputs.nixpkgs.legacyPackages.${system};
        in
        {
          default = import ./devshell.nix { inherit pkgs inputs; };
        }
      );
    };
}
