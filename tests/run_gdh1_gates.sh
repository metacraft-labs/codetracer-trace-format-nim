#!/usr/bin/env bash
# GDH-M1 falsifier-arm driver.
#
# The four gates in `tests/test_gdh1_path_versions.nim` are only worth
# their assertions if the mutations their milestone entries NAME actually
# turn them red. This script runs each one.
#
# Rules it enforces, each of them a trap this campaign has already been
# bitten by:
#
#   * COMPILE and RUN are separate steps. A falsifier that fails to
#     compile also exits non-zero, and counting that as "the gate went
#     red" is a self-pass — the compile must succeed (rc 0) before the run
#     is allowed to mean anything.
#   * A hang is rc 124 and nothing else. `timeout` is used, and 124 is a
#     FAILURE of the arm, never a red gate.
#   * The arm must go red IN THE GATE IT IS AIMED AT. The binary takes a
#     gate selector and every failure carries a `GDH1-FAIL[<gate>]`
#     prefix, so "exited non-zero" is never accepted on its own.
#   * The unmutated build must be GREEN first. An arm that is red because
#     the harness is broken proves nothing.
#
# Usage:  tests/run_gdh1_gates.sh
# Exit:   0 iff the green run passes AND every arm goes red in its gate.

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

WORK="${GDH1_WORK:-$(mktemp -d)}"
mkdir -p "$WORK"
TIMEOUT="${GDH1_TIMEOUT:-300}"
NIMFLAGS=(-d:release -p:src --hints:off --warnings:off)

# zstd is linked by the container layer. The dev shell puts it on the
# default search path; outside it, GDH1_ZSTD_INC / GDH1_ZSTD_LIB point at
# it. Missing headers must FAIL the script, not silently skip the arms.
if [[ -n "${GDH1_ZSTD_INC:-}" ]]; then
  NIMFLAGS+=("--passC:-I${GDH1_ZSTD_INC}")
fi
if [[ -n "${GDH1_ZSTD_LIB:-}" ]]; then
  NIMFLAGS+=("--passL:-L${GDH1_ZSTD_LIB} -lzstd")
fi

SRC=tests/test_gdh1_path_versions.nim

fail_script() { echo "DRIVER-FAIL: $*" >&2; exit 1; }

build() {  # build <outbin> <nimcache> [extra defines...]
  local out="$1"; shift
  local cache="$1"; shift
  nim c "${NIMFLAGS[@]}" --nimcache:"$cache" -o:"$out" "$@" "$SRC" \
    >"$out.build.log" 2>&1
}

pass=0
redarms=0
failures=0

# ---------------------------------------------------------------------------
# 1. The unmutated build must be green.
# ---------------------------------------------------------------------------
echo "== green run (no falsifier arm) =="
if ! build "$WORK/green" "$WORK/nc-green"; then
  tail -30 "$WORK/green.build.log" >&2
  fail_script "the unmutated gate suite does not COMPILE"
fi
timeout "$TIMEOUT" "$WORK/green" all >"$WORK/green.out" 2>&1
rc=$?
cat "$WORK/green.out"
if [[ $rc -eq 124 ]]; then
  fail_script "the unmutated gate suite HUNG (rc 124)"
fi
if [[ $rc -ne 0 ]]; then
  fail_script "the unmutated gate suite is RED (rc $rc); no arm below can mean anything"
fi
if ! grep -q "gdh1_harness_inertness (no falsifier arm compiled in)" "$WORK/green.out"; then
  fail_script "the green run did not assert its own inertness"
fi
pass=1
echo "green run OK (rc 0)"
echo

# ---------------------------------------------------------------------------
# 2. Every named falsifier arm must turn ITS gate red.
# ---------------------------------------------------------------------------
# arm-define : gate-selector : expected GDH1-FAIL gate name
ARMS=(
  "gdh1FalsifyDedup:two_versions:gdh1_two_versions_one_path_round_trip"
  "gdh1FalsifyDefaultSlot:two_versions:gdh1_two_versions_one_path_round_trip"
  "gdh1FalsifyNoMirror:two_versions:gdh1_two_versions_one_path_round_trip"
  "gdh1FalsifyMangle:same_string:gdh1_same_string_different_index"
  "gdh1FalsifyOverwrite:insertion:gdh1_insertion_above_does_not_shift_v1"
  "gdh1FalsifyPrepend:insertion:gdh1_insertion_above_does_not_shift_v1"
  "gdh1FalsifyNoBoundsCheck:step_past:gdh1_step_past_a_version_line_count_is_refused"
  "gdh1FalsifyGlobalCurrent:two_versions:gdh1_two_versions_one_path_round_trip"
  "gdh1FalsifyVersionWithoutTable:step_past:gdh1_step_past_a_version_line_count_is_refused"
)

for spec in "${ARMS[@]}"; do
  IFS=: read -r armdef gate wantgate <<<"$spec"
  echo "== arm $armdef -> gate $gate =="
  bin="$WORK/arm-$armdef"
  if ! build "$bin" "$WORK/nc-$armdef" -d:gdh1FalsifierArms "-d:$armdef"; then
    tail -30 "$bin.build.log" >&2
    echo "ARM-FAIL: $armdef did not COMPILE; a compile error is not a red gate" >&2
    failures=$((failures + 1))
    continue
  fi
  timeout "$TIMEOUT" "$bin" "$gate" >"$bin.out" 2>&1
  rc=$?
  if [[ $rc -eq 124 ]]; then
    echo "ARM-FAIL: $armdef HUNG (rc 124). A hang is a hang, never a red gate" >&2
    failures=$((failures + 1))
    continue
  fi
  if [[ $rc -eq 0 ]]; then
    echo "ARM-FAIL: $armdef exited 0 — the mutation did NOT turn $gate red" >&2
    tail -20 "$bin.out" >&2
    failures=$((failures + 1))
    continue
  fi
  if ! grep -q "ARMED: $armdef" "$bin.out"; then
    echo "ARM-FAIL: $armdef ran a binary that does not report itself armed" >&2
    failures=$((failures + 1))
    continue
  fi
  if ! grep -q "GDH1-FAIL\[$wantgate\]" "$bin.out"; then
    echo "ARM-FAIL: $armdef exited $rc but not with GDH1-FAIL[$wantgate];" \
         "it went red for some other reason:" >&2
    tail -20 "$bin.out" >&2
    failures=$((failures + 1))
    continue
  fi
  echo "RED (rc $rc): $(grep -o 'GDH1-FAIL\[.*' "$bin.out" | head -1 | cut -c1-160)"
  redarms=$((redarms + 1))
  echo
done

# ---------------------------------------------------------------------------
# 3. The INSTRUMENT: the shipped `ct-print` must surface the version
#    ordinal, without linking the reader (GDH-M1 deliverable 4).
# ---------------------------------------------------------------------------
echo "== ct-print instrument =="
CTP="$WORK/ct-print"
if ! nim c "${NIMFLAGS[@]}" --mm:arc --nimcache:"$WORK/nc-ctprint" \
     -o:"$CTP" src/codetracer_ct_print.nim >"$WORK/ctprint.build.log" 2>&1; then
  tail -30 "$WORK/ctprint.build.log" >&2
  echo "INSTRUMENT-FAIL: ct-print did not build" >&2
  failures=$((failures + 1))
else
  CT="$WORK/two_versions.ct"
  if ! timeout "$TIMEOUT" "$WORK/green" dump "$CT" >"$WORK/dump.out" 2>&1; then
    cat "$WORK/dump.out" >&2
    echo "INSTRUMENT-FAIL: could not write the two-version container" >&2
    failures=$((failures + 1))
  else
    instrument_ok=1
    "$CTP" --summary "$CT" >"$WORK/ctp.summary" 2>&1
    "$CTP" --events  "$CT" >"$WORK/ctp.events"  2>&1
    "$CTP" --full    "$CT" >"$WORK/ctp.full"    2>&1

    # Anti-vacuity: the dumps must be NON-EMPTY before any "must contain"
    # check is allowed to mean anything (trap 4 — a scanner that finds
    # nothing passes every such check).
    for f in ctp.summary ctp.events ctp.full; do
      if [[ ! -s "$WORK/$f" ]]; then
        echo "INSTRUMENT-FAIL: $f is EMPTY; every grep below would pass vacuously" >&2
        instrument_ok=0
      fi
    done

    need() {  # need <file> <pattern> <why>
      if ! grep -qF -- "$2" "$WORK/$1"; then
        echo "INSTRUMENT-FAIL: $1 does not report '$2' ($3)" >&2
        instrument_ok=0
      fi
    }
    need ctp.summary "versioned paths: 2 entries" "--summary must name the versioned entries"
    need ctp.summary "v0 of 2, 40 lines  res://gdh1/probe.gd" "--summary must show v1's ordinal and size"
    need ctp.summary "v1 of 2, 63 lines  res://gdh1/probe.gd" "--summary must show v2's ordinal and size"
    need ctp.events  '"path_version_ordinal":0' "--events must carry the ordinal of a v1 step"
    need ctp.events  '"path_version_ordinal":1' "--events must carry the ordinal of a v2 step"
    need ctp.full    '"has_line_count_table": true' \
      "ct-print must report meta.dat bit 14 — the import gap this milestone closed"
    need ctp.full    '"version_ordinal": 1' "--full must carry the path_versions table"
    need ctp.full    '"recorded_line_count": 63' "--full must carry each version's own size"

    if [[ $instrument_ok -eq 1 ]]; then
      echo "ct-print instrument OK"
      grep -A4 "^versioned paths" "$WORK/ctp.summary"
    else
      failures=$((failures + 1))
    fi
  fi
fi
echo

echo "======================================================"
echo "green run:      $pass"
echo "arms gone red:  $redarms of ${#ARMS[@]}"
echo "arm failures:   $failures"
echo "work dir:       $WORK"
if [[ $failures -ne 0 || $redarms -ne ${#ARMS[@]} ]]; then
  exit 1
fi
echo "GDH-M1: green run passed and every falsifier arm went red in its own gate"
