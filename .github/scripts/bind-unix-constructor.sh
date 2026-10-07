#!/usr/bin/env bash
set -euo pipefail

# Acquire committed source before the owning shell can run its guarded installer.
# No working-tree flake, release fallback, host config write or token receipt.
source_ref='git+https://github.com/metacraft-labs/reprobuild?rev=76659f5730ecf698b1963c656494d2cb66eb256d&shallow=1'
expected_revision='76659f5730ecf698b1963c656494d2cb66eb256d'
proof_root="$GITHUB_WORKSPACE/.repro/constructor-binding"
test ! -L "$GITHUB_WORKSPACE/.repro"
test ! -L "$proof_root"
mkdir -p "$GITHUB_WORKSPACE/.repro"
mkdir "$proof_root"
mkdir "$proof_root/guard-stderr"
printf '%s\n' 'Committed-source constructor prerequisite; platform tests and monitoring are separate.' > "$proof_root/scope.txt"

if test -n "${GH_TOKEN:-}"; then
  export NIX_CONFIG="${NIX_CONFIG-}${NIX_CONFIG:+$'\n'}access-tokens = github.com=$GH_TOKEN"
fi
nix flake metadata --no-write-lock-file --json "$source_ref" > "$proof_root/source-metadata.json"
actual_revision="$(nix eval --raw --expr "(builtins.getFlake \"$source_ref\").sourceInfo.rev")"
test "$actual_revision" = "$expected_revision"
system="$(nix eval --impure --raw --expr builtins.currentSystem)"
nix build --no-link --no-write-lock-file --json "$source_ref#reprobuild" > "$proof_root/package.json"
package="$(nix eval --no-write-lock-file --raw "$source_ref#packages.$system.reprobuild.outPath")"
case "$package" in /nix/store/*) ;; *) echo 'Constructor output is outside the Nix store' >&2; exit 1 ;; esac
test -x "$package/bin/repro"
test -x "$package/bin/.repro-wrapped"
test -x "$package/bin/reprobuild-nix-daemon"
test -x "$package/bin/.reprobuild-nix-daemon-wrapped"
test -x "$package/libexec/reprobuild-nix-daemon"
printf '%s\n' "$actual_revision" > "$proof_root/source-revision.txt"
printf '%s\n' "$package" > "$proof_root/package-output.txt"
nix path-info --derivation "$package" > "$proof_root/package-derivation.txt"
for executable in repro .repro-wrapped reprobuild-nix-daemon .reprobuild-nix-daemon-wrapped; do
  nix hash file --type sha256 --base16 "$package/bin/$executable" > "$proof_root/$executable.sha256"
done
nix hash file --type sha256 --base16 "$package/libexec/reprobuild-nix-daemon" > "$proof_root/default-libexec-daemon.sha256"
"$package/bin/repro" hooks protocol --require=2 > "$proof_root/protocol.txt"
printf 'REPROBUILD_REPRO=%s/bin/repro\n' "$package" >> "$GITHUB_ENV"
printf '%s/bin\n' "$package" >> "$GITHUB_PATH"
printf 'TRACE_NIM_GUARD_DIAGNOSTIC_ROOT=%s/guard-stderr\n' "$proof_root" >> "$GITHUB_ENV"
