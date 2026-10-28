# The reader role for consumers whose pinned replay engine reads meta.dat 6.
# Keep its source and ABI separate from the current schema-7 writer/reader.
{ pkgs }:
let
  sourceRev = "051efd22b00ec3b88676fb07755a65c5a4ca8fde";
  source = pkgs.fetchFromGitHub {
    name = "nim6";
    owner = "metacraft-labs";
    repo = "codetracer-trace-format-nim";
    rev = sourceRev;
    hash = "sha256-+3Q3qbwb2/EiLjzLynPGkXLXMwZBLEV8//KD/OIuAaM=";
  };
  stew = pkgs.fetchFromGitHub {
    owner = "status-im";
    repo = "nim-stew";
    rev = "1a5d0b99209f50ff055d9b5216849ba0365f8cf5";
    hash = "sha256-OoKTs5Y5ps0oL43benYf3W6j/KerFZ6xAxaf9mGcaOw=";
  };
  results = pkgs.fetchFromGitHub {
    owner = "arnetheduck";
    repo = "nim-results";
    rev = "b319652e98a198fa881ca70a76754c2dd6f09804";
    hash = "sha256-chvmxuQw7gxRxXelxxBCXHQGk1LXmg63+VysnJhlxmE=";
  };
  paths = "-p:src --path:${stew} --path:${results}";
in
pkgs.stdenv.mkDerivation {
  pname = "codetracer-ct-print-schema6";
  version = "0.1.0";
  src = source;
  patches = [ ./backports/ct-print-container-profiles.patch ];
  nativeBuildInputs = [
    pkgs.nim
    pkgs.pkg-config
  ];
  buildInputs = [
    pkgs.zstd
    pkgs.pcre
  ];
  buildPhase = ''
    runHook preBuild
    nim c -d:release --mm:arc --hints:off --nimcache:$TMPDIR/nimcache \
      ${paths} -o:ct-print src/codetracer_ct_print.nim
    runHook postBuild
  '';
  installPhase = ''
    runHook preInstall
    mkdir -p $out/bin $out/share/ct-print
    cp ct-print $out/bin/ct-print
    printf '%s\n' '{"sourceRev":"${sourceRev}","metaDatVersion":6,"backport":"container-profiles"}' \
      > $out/share/ct-print/provenance.json
    runHook postInstall
  '';
  doInstallCheck = true;
  # The suite is compiled from a copy under its own name: interpolated as a
  # Nix path it lands in the store as `<hash>-ct_print_profiles.nim`, and
  # Nim refuses that as a module name, which failed every build of this
  # reader.
  installCheckPhase = ''
    cp ${./tests/ct_print_profiles.nim} $TMPDIR/ct_print_profiles.nim
    nim c -r -d:release --hints:off --nimcache:$TMPDIR/check-cache \
      ${paths} -o:$TMPDIR/check-profiles $TMPDIR/ct_print_profiles.nim $out/bin/ct-print
  '';
  passthru = {
    inherit
      sourceRev
      source
      stew
      results
      ;
  };
}
