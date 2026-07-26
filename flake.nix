{
  description = "nntpbench - mock NNTP server/client benchmark tool";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    nixpkgs,
    flake-utils,
    rust-overlay,
    ...
  }: let
    cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
    rustToolchainToml = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
    rustVersion = rustToolchainToml.toolchain.channel;
  in
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [(import rust-overlay)];
      };

      rustToolchain = pkgs.rust-bin.stable.${rustVersion}.default.override {
        extensions = [
          "clippy"
          "llvm-tools-preview"
          "rust-analyzer"
          "rust-src"
          "rustfmt"
        ];
      };

      rustPlatform = pkgs.makeRustPlatform {
        cargo = rustToolchain;
        rustc = rustToolchain;
      };

      package = pkgs.callPackage ./nix/package.nix {
        inherit cargoToml rustPlatform;
      };
    in {
      apps.default = {
        type = "app";
        program = "${package}/bin/${cargoToml.package.name}";
        meta.description = "Run nntpbench";
      };

      formatter = pkgs.alejandra;

      devShells.default = pkgs.mkShell {
        packages =
          [
            rustToolchain
            pkgs.cargo-llvm-cov
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
            pkgs.heaptrack
            pkgs.inferno
            pkgs.perf
            pkgs.strace
            pkgs.valgrind
          ];

        RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";

        shellHook = ''
          echo "nntpbench Rust environment"
          echo "  rustc: $(rustc --version)"
          echo "  cargo: $(cargo --version)"
          echo ""
          echo "Useful commands:"
          echo "  cargo build"
          echo "  cargo test"
          echo "  cargo fmt --check"
          echo "  cargo clippy --all-targets -- -D warnings"
          echo "  cargo llvm-cov --fail-under-lines 100 --summary-only"
          echo "  cargo bench --bench server_roundtrip"
          echo "  cargo bench --bench server_callgrind"
          echo "  ./scripts/profile.sh"
          echo "  ./scripts/profile-mem.sh"
          echo "  ./scripts/profile-latency.sh strace"
        '';
      };

      packages.default = package;
      packages.${cargoToml.package.name} = package;
    });
}
