{
  lib,
  rustPlatform,
  cargoToml ? builtins.fromTOML (builtins.readFile ../Cargo.toml),
}:
rustPlatform.buildRustPackage {
  pname = cargoToml.package.name;
  version = cargoToml.package.version;
  src = lib.cleanSource ./..;

  cargoLock.lockFile = ../Cargo.lock;
  cargoBuildFlags = ["--bin" cargoToml.package.name];

  meta = {
    description = "Mock NNTP benchmark server and client";
    license = lib.licenses.mit;
    mainProgram = cargoToml.package.name;
    platforms = lib.platforms.unix;
  };
}
