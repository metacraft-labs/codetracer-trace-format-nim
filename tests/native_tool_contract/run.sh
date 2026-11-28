#!/usr/bin/env bash
# Regression test: a sibling recipe that builds ct-print through
# `build_writer_artifacts.buildCtPrint` gets the C compiler and zstd's
# headers and library from the graph, not from the shell it runs in.
#
# The defect: a recorder's `repro test`, run outside a dev shell as CI
# runs it, failed at the ct-print edge with `zstd.h: No such file or
# directory`. The edge named no zstd, and a consumer could not supply it,
# because a tool ref the consuming package never declared in `uses:` was
# dropped without a diagnostic.
#
# Two cases, both against a copy of this checkout laid out beside a fixture
# consumer exactly as a recorder sits beside it
# (`<scratch>/codetracer-trace-format-nim`, `<scratch>/<consumer>`):
#
#   1. `consumer_undeclared.repro.nim` omits `zstd-dev` from `uses:`.
#      `repro graph` must refuse it, naming `zstd-dev`. Before the fix the
#      graph evaluated and the ref vanished silently.
#   2. `consumer_declared.repro.nim` declares what a recorder declares
#      (`CtPrintToolSelectors`). `repro build` must compile ct-print, and the
#      binary must run. Before the fix this failed at `zstd.h`.
#      It also reads the ct-print edge's argv from `repro graph` and checks
#      that `--noNimblePath` precedes the dependency `--path:` entries, so
#      the compile takes Nim packages only from `nimDependencyDirs`.
#
# Why `--noNimblePath` is checked on the argv and not by a build. A third
# case used to plant a copy of results on Nim's default nimble path
# (`NIMBLE_DIR`) and expect a consumer that leaves results out of
# `nimDependencyDirs` to fail on it. Reprobuild now runs every `nim c` edge
# with `HOME` and `NIMBLE_DIR` set to a scratch directory of the edge's own
# under reprobuild's cache, and refuses an edge that reads the host's
# `NIMBLE_DIR`. So the plant never reached the compiler, and that build
# failed on results whether or not `buildCtPrint` passed `--noNimblePath`
# (measured with the flag removed: the case still passed). It tested
# reprobuild's isolation, not this repository. No plant this script controls
# can reach the edge's nimble path instead: with `NIMBLE_DIR` set, Nim
# replaces every `nimblePath` a configuration file or `config.nims` names
# with `$NIMBLE_DIR/pkgs2` and `$NIMBLE_DIR/pkgs` (`compiler/commands.nim`,
# the `nimblepath` switch), so under the isolation every nimble path is in
# the edge's own scratch directory, which only reprobuild writes. The flag's
# effect cannot be observed through `repro build`, and the case was retired;
# the argv check is what fails when `buildCtPrint` drops the flag.
#
# The edge's monitor record is not checked for nimble path listings: in this
# stripped environment the graph provisions a statically linked `nim`, which
# the monitor cannot inject into (the record says so, "uninjectable image"),
# so the record holds none of the compiler's own reads either way.
#
# All run `repro` in an environment emptied of everything but the directories
# of `repro` and `nix` (and a short allowlist of non-tool variables), and the
# script first proves that no C compiler is reachable there. So whatever
# compiles ct-print came from the graph. Nothing is mocked: the real `repro`,
# real path provisioning through Nix, the real compiler and the real zstd.
#
# Run from the owning dev shell (`just test-native-tool-contract`): it needs
# `repro` and `nix` on PATH, and forwards CT_NIM_STEW_SRC / CT_NIM_RESULTS_SRC
# so the compile finds the pinned Nim sources a recorder gets from its cargo
# build's `nimble install` instead. POSIX only, because it provisions through
# Nix. `buildCtPrint`'s Windows contract is the same (`gcc` and `zstd-dev`,
# realized there as a MinGW static `libzstd.a`), and the recorders' Windows CI
# legs are what exercise it; `buildSharedLib` still takes zstd through
# explicit paths on Windows.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checkout="$(cd "$here/../.." && pwd)"

fail() { echo "native-tool-contract: FAIL: $*" >&2; exit 1; }

repro_exe="$(command -v repro)" || fail "repro is not on PATH"
nix_exe="$(command -v nix)" || fail "nix is not on PATH"
command -v python3 >/dev/null || fail "python3 is not on PATH"
: "${CT_NIM_STEW_SRC:?run from the owning dev shell (CT_NIM_STEW_SRC unset)}"
: "${CT_NIM_RESULTS_SRC:?run from the owning dev shell (CT_NIM_RESULTS_SRC unset)}"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/ct-native-tool-contract.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

# The inputs `buildCtPrint` declares, and nothing else: no `.repro`, no
# prebuilt `ct-print`, no nimcache from an earlier build in this checkout.
producer="$scratch/codetracer-trace-format-nim"
mkdir -p "$producer"
for entry in src include nim.cfg config.nims codetracer_trace_format.nimble \
             build_writer_artifacts.nim build_ffi_flags.nim build_ffi.nims; do
  cp -R "$checkout/$entry" "$producer/"
done
mkdir -p "$scratch/consumer" "$scratch/consumer-without-zstd-dev"
cp "$here/consumer_declared.repro.nim" "$scratch/consumer/repro.nim"
cp "$here/consumer_undeclared.repro.nim" \
  "$scratch/consumer-without-zstd-dev/repro.nim"

stripped_path="$(dirname "$repro_exe"):$(dirname "$nix_exe")"
stripped_env=(env -i "PATH=$stripped_path" "HOME=$HOME"
  "CT_NIM_STEW_SRC=$CT_NIM_STEW_SRC" "CT_NIM_RESULTS_SRC=$CT_NIM_RESULTS_SRC")
# Not tools: identity, temp/cache locations, Nix daemon/TLS settings, and the
# variables that select reprobuild's own helpers (REPRO_BOOTSTRAP_CC compiles
# the recipe provider, never an action; see the workspace AGENTS.md), and the
# RUNQUOTA_* pair that names the lease coordinator `repro build` asks for slots.
#
# Nor are the sibling-first source trees of reprobuild itself (SHM_QUEUE_SRC
# ... CONFUTILS_SRC, the engine's `SiblingFirstPackageEnvNames`): Nim source
# the engine compiles every recipe's provider against, never on an action's
# PATH. An engine run from a reprobuild git checkout (CI's source-built
# `.reprobuild-src/build/bin/repro`) takes each from a `../<sibling>` checkout
# at the revision that checkout pins or from `$<NAME>`, and refuses the tree
# compiled into it ("no acceptable source for SHM_QUEUE_SRC"); the Nix
# package's wrapper sets them itself, so a store-installed repro needs none.
# Measured in linux-x64 job 140767131442. RUNQUOTAD_BIN is the same kind of
# variable: the engine's lease daemon, which it starts itself when none is
# reachable; the package wrapper sets it too, and setup-reprobuild exports it
# for a source-built engine.
for name in USER LOGNAME TMPDIR XDG_CACHE_HOME XDG_RUNTIME_DIR NIX_REMOTE \
            NIX_SSL_CERT_FILE SSL_CERT_FILE REPRO_BOOTSTRAP_CC \
            REPRO_FOREIGN_ENV_BASH REPROBUILD_NIX_DAEMON_BIN RUNQUOTAD_BIN \
            RUNQUOTA_SOCKET RUNQUOTA_ENDPOINT_SCOPE \
            SHM_QUEUE_SRC SHM_GSET_SRC CLI_SPEC_SRC ENTRYPOINTS_SRC CONFUTILS_SRC; do
  if [ -n "${!name+x}" ]; then stripped_env+=("$name=${!name}"); fi
done

# Precondition: the stripped environment genuinely has no C compiler. If one
# of the kept directories carries one, this test cannot tell the graph's
# compiler from the ambient one, so it fails rather than passing vacuously.
for compiler in cc gcc clang c99; do
  if found="$("${stripped_env[@]}" "$BASH" -c "command -v $compiler")"; then
    fail "the stripped PATH ($stripped_path) still reaches $compiler at $found"
  fi
done

echo "native-tool-contract: case 1: a consumer without zstd-dev is refused"
set +e
refusal="$(cd "$scratch/consumer-without-zstd-dev" &&
  "${stripped_env[@]}" "$repro_exe" graph default --view=actions \
    --format=json 2>&1)"
status=$?
set -e
expected="buildCtPrint: package 'ct_print_consumer_without_zstd_dev' does not declare \"zstd-dev\" in its uses: block"
if [ "$status" -eq 0 ] || ! grep -qF "$expected" <<<"$refusal"; then
  printf '%s\n' "$refusal" | tail -n 40 >&2
  fail "expected repro graph to refuse with: $expected (exit $status)"
fi

grep -F "$expected" <<<"$refusal" | head -n 1
echo "native-tool-contract: case 2: a declaring consumer builds ct-print"
(cd "$scratch/consumer" &&
  "${stripped_env[@]}" "$repro_exe" build --daemon=off --force-rebuild \
    --progress=line \
    --action-output=failed) || fail "repro build of the declaring consumer failed"
[ -x "$producer/ct-print" ] || fail "repro build did not produce ct-print"
"${stripped_env[@]}" "$producer/ct-print" --help >/dev/null ||
  fail "the built ct-print does not run"
# The ct-print edge's argv turns the nimble path off before it names the
# dependency directories.
graph="$(cd "$scratch/consumer" &&
  "${stripped_env[@]}" "$repro_exe" graph default --view=actions \
    --format=json)" || fail "repro graph of the declaring consumer failed"
python3 -c '
import json, sys
stew, results = sys.argv[1], sys.argv[2]
actions = {a["id"]: a for a in json.load(sys.stdin)["actions"]}
argv = actions["codetracer-trace-format-nim.ct-print.nim-c"]["argv"]
flag = argv.index("--noNimblePath")
assert flag < argv.index("--path:" + stew) and flag < argv.index("--path:" + results), argv
' "$CT_NIM_STEW_SRC" "$CT_NIM_RESULTS_SRC" <<<"$graph" ||
  fail "the ct-print edge does not pass --noNimblePath before its --path: entries"

echo "native-tool-contract: OK"
