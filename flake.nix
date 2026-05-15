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

      rustToolchainBase =
        if rustVersion == "nightly"
        then pkgs.rust-bin.nightly.latest.default
        else pkgs.rust-bin.stable.${rustVersion}.default;

      rustToolchain = rustToolchainBase.override {
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

      package = rustPlatform.buildRustPackage {
        pname = cargoToml.package.name;
        version = cargoToml.package.version;
        src = ./.;

        cargoLock.lockFile = ./Cargo.lock;
        cargoBuildFlags = ["--bin" cargoToml.package.name];

        meta = with pkgs.lib; {
          description = "Mock NNTP benchmark server and client";
          license = licenses.mit;
          mainProgram = cargoToml.package.name;
          platforms = platforms.unix;
        };
      };
    in {
      apps.default = {
        type = "app";
        program = "${package}/bin/${cargoToml.package.name}";
        meta.description = "Run nntpbench";
      };

      formatter = pkgs.alejandra;

      devShells.default = pkgs.mkShell {
        packages = [
          rustToolchain
          pkgs.cargo-llvm-cov
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
        '';
      };

      packages.default = package;
      packages.${cargoToml.package.name} = package;
    });
}
