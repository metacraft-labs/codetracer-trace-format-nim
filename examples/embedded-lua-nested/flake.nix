{
  description = "N3 spike — native C host embedding the Lua VM, producing a nested materialized CTFS trace via Lua's own per-line hook (lua_sethook), joined to the host's native trace. No engine fork.";

  # Per the workspace policy ("declare tooling in flake.nix, not ad-hoc nix
  # shell"), all build tooling — the Lua VM the host embeds, the C compiler,
  # libzstd (the CTFS writer's runtime dep), pkg-config and python3 (the
  # verifier) — is declared here rather than pulled from a bare `nix shell`.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.clang # the native host compiler (host.c)
            pkgs.lua5_4 # the embedded scripting VM (headers + liblua + lua5.4.pc)
            pkgs.zstd # the CTFS writer (libcodetracer_trace_writer.a) links libzstd
            pkgs.pkg-config # resolves lua/zstd include+lib dirs for the build
            pkgs.python3 # runs scripts/verify_n3.py
          ];

          # Make pkg-config find lua5.4.pc and libzstd.pc from the store.
          shellHook = ''
            export PKG_CONFIG_PATH="${pkgs.lua5_4}/lib/pkgconfig:${pkgs.zstd.dev}/lib/pkgconfig''${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
            export N3_IN_SHELL=1
          '';
        };
      }
    );
}
