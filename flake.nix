{
  description = "CTFS (CodeTracer File System) container format — Nim implementation";

  inputs = {
    # Same toolchain source as the sibling Rust `codetracer-trace-format`
    # repo, so the Nim/Rust pair that make up the trace-format layer are
    # built with one pinned compiler set rather than two.
    codetracer-toolchains.url = "github:metacraft-labs/nix-codetracer-toolchains";
    nixpkgs.follows = "codetracer-toolchains/nixpkgs";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    pre-commit-hooks.url = "github:cachix/git-hooks.nix";

    # The Nim packages `codetracer_trace_format.nimble` declares as
    # `requires`, plus `unittest2` (a transitive `requires` of stew).  They
    # are provided as sources here and seeded into a project-local
    # `NIMBLE_DIR` by the dev shell, so `nimble test` resolves its
    # dependencies without reaching for the network on a clean machine.
    nim-stew = {
      url = "github:status-im/nim-stew";
      flake = false;
    };
    nim-results = {
      url = "github:arnetheduck/nim-results";
      flake = false;
    };
    nim-unittest2 = {
      url = "github:status-im/nim-unittest2";
      flake = false;
    };
    # nimble reads `<NIMBLE_DIR>/packages_official.json` to resolve package
    # names.  Seeding it from the pinned index keeps a first `nimble test`
    # on a fresh checkout offline.
    nim-packages-index = {
      url = "github:nim-lang/packages";
      flake = false;
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      perSystem =
        { pkgs, system, ... }:
        let
          toolchainsPkgs = inputs.codetracer-toolchains.packages.${system};

          nim = toolchainsPkgs.nim-2_2;
          nimble = toolchainsPkgs.nimble;

          # `--path:` arguments that make the .nimble's `requires` importable
          # from a bare `nim c` (the package build below, and the FFI static
          # lib the Rust sibling's build.rs compiles).
          nimDepPaths = [
            "${inputs.nim-stew}"
            "${inputs.nim-results}"
            "${inputs.nim-unittest2}"
          ];

          # A ready-made `NIMBLE_DIR` payload.  nimble accepts any
          # `pkgs2/<name>-<version>-<40 hex chars>` directory that carries the
          # package's `.nimble`; the checksum component is not verified when
          # the package is already present, so a stable placeholder is used
          # (the real pin is the flake input's revision).
          nimbleSeed = pkgs.runCommand "codetracer-trace-format-nim-nimble-seed" { } ''
            mkdir -p $out/pkgs2

            seed() {
              local src="$1" name="$2" digits="$3"
              local ver
              ver=$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
                      "$src/$name.nimble" | head -n1)
              if [ -z "$ver" ]; then ver=0.0.1; fi
              local dst="$out/pkgs2/$name-$ver-$digits"
              cp -r "$src" "$dst"
              chmod -R u+w "$dst"
            }

            seed ${inputs.nim-stew}      stew      0000000000000000000000000000000000000001
            seed ${inputs.nim-results}   results   0000000000000000000000000000000000000002
            seed ${inputs.nim-unittest2} unittest2 0000000000000000000000000000000000000003

            cp ${inputs.nim-packages-index}/packages.json $out/packages_official.json
          '';

          preCommit = inputs.pre-commit-hooks.lib.${system}.run {
            src = ./.;
            hooks = {
              lint = {
                enable = true;
                name = "Lint";
                entry = "just lint";
                language = "system";
                pass_filenames = false;
              };
            };
          };
        in
        {
          checks.pre-commit-check = preCommit;

          devShells.default = pkgs.mkShell {
            packages = [
              # Nim toolchain (the .nimble requires nim >= 2.2.0).
              nim
              nimble

              # The C back-end `nim c` shells out to.
              pkgs.gcc

              # `codetracer_ctfs` links libzstd; several tests resolve its
              # include/lib dirs through pkg-config.
              pkgs.pkg-config
              pkgs.zstd
              pkgs.zstd.dev

              # `tests/test_path_filter.nim` uses `std/re`, which dlopens
              # libpcre at run time.
              pkgs.pcre

              # The three `test_nim_*_crossread` tests shell out to
              # `cargo test` in the sibling `codetracer-trace-format`
              # checkout to prove a Nim-written bundle is readable by the
              # canonical Rust reader.  Without a Rust toolchain here they
              # would take their SKIP arm and the cross-read proof would
              # silently stop running.  capnproto is needed by that
              # sibling's `codetracer_trace_format_capnp` crate.
              toolchainsPkgs.rust-stable
              pkgs.capnproto

              # Build automation, formatters and hook runner.
              pkgs.just
              pkgs.nixfmt-rfc-style
              pkgs.prek
              pkgs.git
            ]
            ++ preCommit.enabledPackages;

            PKG_CONFIG_PATH = "${pkgs.zstd.dev}/lib/pkgconfig";

            # Consumed by `repro.nim`'s `test_path_filter` edge, which links
            # pcre directly instead of relying on a run-time library search.
            CT_PCRE_LIB_DIR = "${pkgs.pcre.out}/lib";

            shellHook = preCommit.shellHook + ''
              # Seed a project-local, writable NIMBLE_DIR from the pinned
              # sources above.  nimble needs to write (nimbledata2.json), so
              # the store copy is materialised once into `.nimble/` rather
              # than used in place.  Honour an NIMBLE_DIR the caller already
              # set (CI may point at a shared cache).
              if [ -z "''${NIMBLE_DIR:-}" ]; then
                export NIMBLE_DIR="$PWD/.nimble"
              fi
              if [ ! -d "$NIMBLE_DIR/pkgs2" ]; then
                mkdir -p "$NIMBLE_DIR"
                cp -r ${nimbleSeed}/. "$NIMBLE_DIR/"
                chmod -R u+w "$NIMBLE_DIR"
              fi

              # `std/re` dlopens libpcre; the dev shell is the only thing that
              # knows where it lives.
              export LD_LIBRARY_PATH="${pkgs.pcre.out}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              export DYLD_FALLBACK_LIBRARY_PATH="${pkgs.pcre.out}/lib''${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"
            '';
          };

          # `ct-print` — the `.ct` container inspector every recorder in the
          # CodeTracer subtree shells out to, and the repo's shipping
          # artifact.  Built with a bare `nim c` against the pinned dependency
          # sources so the derivation needs no network.
          packages.default = pkgs.stdenv.mkDerivation {
            pname = "codetracer-ct-print";
            version = "0.1.0";
            src = ./.;

            nativeBuildInputs = [
              nim
              pkgs.pkg-config
            ];
            buildInputs = [
              pkgs.zstd
              pkgs.pcre
            ];

            buildPhase = ''
              runHook preBuild
              export HOME=$TMPDIR
              nim c -d:release --mm:arc --hints:off \
                --nimcache:$TMPDIR/nimcache \
                -p:src ${pkgs.lib.concatMapStringsSep " " (p: "--path:${p}") nimDepPaths} \
                -o:ct-print src/codetracer_ct_print.nim
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/bin
              cp ct-print $out/bin/ct-print
              runHook postInstall
            '';

            meta = {
              description = "CodeTracer .ct container inspector";
              mainProgram = "ct-print";
            };
          };
        };
    };
}
