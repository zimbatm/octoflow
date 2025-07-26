{ pkgs, inputs }:
pkgs.mkShell {
  # Add build dependencies
  packages = [
    pkgs.go
    pkgs.golangci-lint
    pkgs.gopls
    pkgs.nixfmt-rfc-style

    pkgs.natscli
    pkgs.nats-server
    pkgs.hivemind
    pkgs.cloudflared
    inputs.gorefresh.packages.${pkgs.system}.default
  ];

  # Add environment variables
  env = { };

  # Load custom bash code
  shellHook = ''
    echo "GitHub Organization Events System Development Shell"
    echo ""
    echo "Available commands:"
    echo "  make run                - Run all services with hivemind"
    echo "  make build              - Build all binaries"
    echo "  make test               - Run tests"
    echo "  make fmt                - Format code with go fmt"
    echo "  make lint               - Run golangci-lint"
    echo ""

    # Load .env file if it exists
    if [ -f .env ]; then
      echo "Loading environment from .env file..."
      set -a
      source .env
      set +a
    fi
  '';
}
