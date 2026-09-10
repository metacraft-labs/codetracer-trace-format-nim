#!/usr/bin/env bash
# GDH-M3 gate driver — the C ABI can mint a version, and a host stops
# re-deriving path ids.
#
# What runs here:
#
#   1. `gdh3_refused_registration_reaches_the_c_caller`, entirely in C,
#      unmutated — must be GREEN.
#   2. `gdh3_fork_uses_the_writers_own_path_id`, in two arms: the
#      SINGLE-version control (where a mirror counter and the writer agree,
#      so the gate must PASS) and the TWO-version arm.
#   3. Every named falsifier arm, each of which must turn ITS gate red.
#   4. The header check: the two entry points GDH-M1 found missing must be
#      DECLARED in include/codetracer_trace_writer.h, matched by syntax
#      rather than by vocabulary (trap 4d — a pattern that matches the
#      module's own prose is satisfied by prose).
#
# Rules it enforces (see codetracer-specs/Testing/Verification-Harness-Traps.md):
#
#   * COMPILE and RUN are separate steps; a compile error is never a red gate.
#   * A hang is rc 124 and NOTHING ELSE.
#   * An arm must go red IN THE GATE IT IS AIMED AT — every failure carries a
#     `GDH3-FAIL[<gate>]` prefix and the driver requires the right one.
#   * The unmutated run must be GREEN first.
#
# Usage:  tests/run_gdh3_gates.sh
# Exit:   0 iff every gate is green and every arm is red in its own gate.

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

WORK="${GDH3_WORK:-$(mktemp -d)}"
mkdir -p "$WORK"
TIMEOUT="${GDH3_TIMEOUT:-300}"
NIMFLAGS=(-d:release -p:src --hints:off --warnings:off)
HEADER=include/codetracer_trace_writer.h
CSRC=tests/test_gdh3_fork_path_ids.c
LIB=libcodetracer_trace_writer.a

fail_script() { echo "DRIVER-FAIL: $*" >&2; exit 1; }

failures=0
green=0
redarms=0

# ---------------------------------------------------------------------------
# 0. The header. This is the gap GDH-M1 found and GDH-M3 was told to FIX, not
#    merely to extend: the fork vendors this file, and until now it declared
#    neither entry point, so the fork could not reach bit-14 mode at all.
# ---------------------------------------------------------------------------
echo "== header declarations =="
header_ok=1
[[ -f "$HEADER" ]] || fail_script "$HEADER is missing"
# Anti-vacuity for the scan itself: a positive control through the SAME
# pattern shape, over a declaration that has been in the header for
# years. If the scan is blind, this goes red first (trap 4a — the pairing
# is the control).
if ! grep -qE '^void trace_writer_register_step\(' "$HEADER"; then
  echo "HEADER-FAIL: the positive control did not match, so the scan is not" \
       "reading the header and every check below would pass vacuously" >&2
  header_ok=0
fi
# Anchored to `^<return type> <name>(` — syntax only. A prose mention of
# the name in a comment cannot satisfy it, which is the whole point:
# the header's own comments talk about these functions at length.
for decl in \
  '^int trace_writer_enable_line_count_table\(' \
  '^int trace_writer_register_path_with_line_count\(' \
  '^uint64_t trace_writer_register_path_version\(' \
  '^uint64_t trace_writer_current_path_id\(' \
  '^void trace_writer_clear_last_error\(' ; do
  if ! grep -qE "$decl" "$HEADER"; then
    echo "HEADER-FAIL: no declaration matching /$decl/ in $HEADER" >&2
    header_ok=0
  fi
done
if ! grep -qE '^#define CT_TW_INVALID_PATH_ID' "$HEADER"; then
  echo "HEADER-FAIL: CT_TW_INVALID_PATH_ID is not defined in $HEADER" >&2
  header_ok=0
fi
if [[ $header_ok -eq 1 ]]; then
  echo "header OK: 5 declarations + the failure sentinel, matched by syntax"
else
  failures=$((failures + 1))
fi
echo

# ---------------------------------------------------------------------------
# 1. Build the real static library, then the C program against the real header.
# ---------------------------------------------------------------------------
build_lib() {  # build_lib <tag> [extra defines...]
  local tag="$1"; shift
  nim c --app:staticlib --mm:arc --noMain -d:release \
    --nimMainPrefix:codetracerTraceWriter --passC:"-fPIC" -p:src \
    --hints:off --warnings:off \
    --nimcache:"$WORK/nc-lib-$tag" \
    "$@" -o:"$WORK/lib-$tag.a" src/codetracer_trace_writer_ffi.nim \
    >"$WORK/lib-$tag.build.log" 2>&1
}

build_c() {  # build_c <outbin> <lib> [extra cflags...]
  local out="$1"; shift
  local lib="$1"; shift
  gcc -o "$out" "$CSRC" "$lib" -lzstd -lm -I include "$@" \
    >"$out.build.log" 2>&1
}

echo "== build =="
if ! build_lib plain; then
  tail -30 "$WORK/lib-plain.build.log" >&2
  fail_script "the static library does not build"
fi
if ! build_c "$WORK/gdh3" "$WORK/lib-plain.a"; then
  tail -30 "$WORK/gdh3.build.log" >&2
  fail_script "tests/test_gdh3_fork_path_ids.c does not compile against the header"
fi
if ! nim c "${NIMFLAGS[@]}" --nimcache:"$WORK/nc-verify" \
     -o:"$WORK/gdh3_verify" tests/gdh3_verify.nim >"$WORK/verify.build.log" 2>&1; then
  tail -30 "$WORK/verify.build.log" >&2
  fail_script "tests/gdh3_verify.nim does not compile"
fi
echo "built: static lib, C producer, Nim verifier"
echo

# ---------------------------------------------------------------------------
# 2. gdh3_refused_registration_reaches_the_c_caller (green).
# ---------------------------------------------------------------------------
echo "== gdh3_refused_registration_reaches_the_c_caller =="
mkdir -p "$WORK/refusal"
timeout "$TIMEOUT" "$WORK/gdh3" refusal "$WORK/refusal" >"$WORK/refusal.out" 2>&1
rc=$?
cat "$WORK/refusal.out"
if [[ $rc -eq 124 ]]; then fail_script "the refusal gate HUNG (rc 124)"; fi
if [[ $rc -ne 0 ]]; then
  echo "GATE-FAIL: the refusal gate is RED (rc $rc)" >&2
  failures=$((failures + 1))
elif ! grep -q "^PASS: gdh3_refused_registration_reaches_the_c_caller" "$WORK/refusal.out"; then
  echo "GATE-FAIL: the refusal gate exited 0 without printing its PASS line" >&2
  failures=$((failures + 1))
else
  green=$((green + 1))
fi
echo

# ---------------------------------------------------------------------------
# 3. gdh3_fork_uses_the_writers_own_path_id — control arm, then the real one.
# ---------------------------------------------------------------------------
run_bundle() {  # run_bundle <bin> <mode> <tag>
  local bin="$1" mode="$2" tag="$3"
  local dir="$WORK/$tag"
  rm -rf "$dir"; mkdir -p "$dir"
  timeout "$TIMEOUT" "$bin" "$mode" "$dir" >"$WORK/$tag.out" 2>&1
  echo $?
}

read_id() {  # read_id <tag> <key>
  grep -oE "^$2=[0-9]+" "$WORK/$1.out" | head -1 | cut -d= -f2
}

verify_bundle() {  # verify_bundle <tag> <mode>
  local tag="$1" mode="$2"
  local ct="$WORK/$tag/gdh3_probe.ct"
  if [[ ! -s "$ct" ]]; then
    echo "  (no container at $ct)" >&2
    return 1
  fi
  local v1 oid v2 bv2
  v1=$(read_id "$tag" writer_v1_id)
  oid=$(read_id "$tag" writer_other_id)
  if [[ -z "$v1" || -z "$oid" ]]; then
    echo "  (the producer did not print its ids)" >&2
    return 1
  fi
  if [[ "$mode" == "single" ]]; then
    timeout "$TIMEOUT" "$WORK/gdh3_verify" single "$ct" "$v1" "$oid"
    return $?
  fi
  v2=$(read_id "$tag" writer_v2_id)
  bv2=$(read_id "$tag" bundled_v2_under)
  if [[ -z "$v2" || -z "$bv2" ]]; then
    echo "  (the producer did not print the reload ids)" >&2
    return 1
  fi
  timeout "$TIMEOUT" "$WORK/gdh3_verify" two "$ct" "$v1" "$oid" "$v2" "$bv2"
  return $?
}

echo "== gdh3_fork_uses_the_writers_own_path_id — CONTROL ARM (one version) =="
rc=$(run_bundle "$WORK/gdh3" bundle-single ctrl)
if [[ "$rc" == "124" ]]; then fail_script "the control arm HUNG (rc 124)"; fi
if [[ "$rc" != "0" ]]; then
  cat "$WORK/ctrl.out" >&2
  echo "GATE-FAIL: the single-version producer exited $rc" >&2
  failures=$((failures + 1))
elif verify_bundle ctrl single; then
  green=$((green + 1))
else
  echo "GATE-FAIL: the control arm did not verify" >&2
  failures=$((failures + 1))
fi
echo

echo "== gdh3_fork_uses_the_writers_own_path_id — TWO versions =="
rc=$(run_bundle "$WORK/gdh3" bundle two)
if [[ "$rc" == "124" ]]; then fail_script "the two-version arm HUNG (rc 124)"; fi
if [[ "$rc" != "0" ]]; then
  cat "$WORK/two.out" >&2
  echo "GATE-FAIL: the two-version producer exited $rc" >&2
  failures=$((failures + 1))
elif verify_bundle two two; then
  green=$((green + 1))
else
  echo "GATE-FAIL: the two-version arm did not verify" >&2
  failures=$((failures + 1))
fi
echo

# ---------------------------------------------------------------------------
# 4. Falsifier arms.
# ---------------------------------------------------------------------------

# ARM 1 — reinstate the fork's mirror counter (a C-side define).
#   With ONE version the mirror and the writer agree, so this arm must
#   still PASS the control; with TWO it must go red.  A single-version arm
#   cannot distinguish the mirror from the real thing, and that is exactly
#   why the defect is latent today.
echo "-- arm gdh3FalsifyMirrorCounter (C: -DGDH3_FALSIFY_MIRROR_COUNTER)"
if ! build_c "$WORK/gdh3-mirror" "$WORK/lib-plain.a" -DGDH3_FALSIFY_MIRROR_COUNTER; then
  tail -20 "$WORK/gdh3-mirror.build.log" >&2
  echo "ARM-FAIL: the mirror arm did not COMPILE; a compile error is not a red gate" >&2
  failures=$((failures + 1))
else
  rc=$(run_bundle "$WORK/gdh3-mirror" bundle-single mirror-ctrl)
  if [[ "$rc" == "124" ]]; then
    echo "ARM-FAIL: the mirror arm's control HUNG (rc 124)" >&2
    failures=$((failures + 1))
  elif [[ "$rc" != "0" ]] || ! verify_bundle mirror-ctrl single >/dev/null 2>&1; then
    echo "ARM-FAIL: the mirror arm went red on the SINGLE-version control." \
         "It must pass there — a mirror and the writer agree with one" \
         "version, and an arm that fails everywhere has not been shown to" \
         "discriminate" >&2
    failures=$((failures + 1))
  else
    echo "   control (one version): PASSES under the mirror, as it must"
    rc=$(run_bundle "$WORK/gdh3-mirror" bundle mirror-two)
    if [[ "$rc" == "124" ]]; then
      echo "ARM-FAIL: the mirror arm HUNG (rc 124)" >&2
      failures=$((failures + 1))
    else
      verify_bundle mirror-two two >"$WORK/mirror-two.verify" 2>&1
      vrc=$?
      if [[ $vrc -eq 0 ]]; then
        echo "ARM-FAIL: the mirror arm PASSED the two-version gate; the" \
             "mutation did not turn it red" >&2
        failures=$((failures + 1))
      elif ! grep -q "GDH3-FAIL\[gdh3_fork_uses_the_writers_own_path_id\]" \
             "$WORK/mirror-two.verify"; then
        echo "ARM-FAIL: the mirror arm exited $vrc but not with" \
             "GDH3-FAIL[gdh3_fork_uses_the_writers_own_path_id]:" >&2
        cat "$WORK/mirror-two.verify" >&2
        failures=$((failures + 1))
      else
        echo "   RED (rc $vrc): $(head -1 "$WORK/mirror-two.verify" | cut -c1-170)"
        redarms=$((redarms + 1))
      fi
    fi
  fi
fi
echo

# ARM 3 (added by review) — the bundler's sentinel branch is taken for a path
#   the writer KNOWS.  `path_id_for_bundle` answers CT_TW_INVALID_PATH_ID after
#   the reload and the producer does what the real fork does with that answer:
#   returns, silently.  The reloaded version's text is simply never bundled;
#   no call fails, no error is set, the container closes cleanly and opens
#   cleanly.  Only a verifier that counts the views can see it — which is why
#   `gdh3_verify.nim` asserts an EXACT expected count and not merely `> 0`.
#   Like the mirror arm, it must still PASS the single-version control: with
#   one version there is no reload, the branch is never reached, and an arm
#   that failed everywhere would not have been shown to discriminate.
echo "-- arm gdh3FalsifySilentSkipReload (C: -DGDH3_FALSIFY_SILENT_SKIP_RELOAD)"
if ! build_c "$WORK/gdh3-skip" "$WORK/lib-plain.a" \
       -DGDH3_FALSIFY_SILENT_SKIP_RELOAD; then
  tail -20 "$WORK/gdh3-skip.build.log" >&2
  echo "ARM-FAIL: the silent-skip arm did not COMPILE" >&2
  failures=$((failures + 1))
else
  rc=$(run_bundle "$WORK/gdh3-skip" bundle-single skip-ctrl)
  if [[ "$rc" == "124" ]]; then
    echo "ARM-FAIL: the silent-skip arm's control HUNG (rc 124)" >&2
    failures=$((failures + 1))
  elif [[ "$rc" != "0" ]] || ! verify_bundle skip-ctrl single >/dev/null 2>&1; then
    echo "ARM-FAIL: the silent-skip arm went red on the SINGLE-version" \
         "control. It must pass there — with no reload the sentinel branch" \
         "is never reached" >&2
    failures=$((failures + 1))
  else
    echo "   control (one version): PASSES under the silent skip, as it must"
    rc=$(run_bundle "$WORK/gdh3-skip" bundle skip-two)
    if [[ "$rc" == "124" ]]; then
      echo "ARM-FAIL: the silent-skip arm HUNG (rc 124)" >&2
      failures=$((failures + 1))
    elif [[ "$rc" != "0" ]]; then
      echo "ARM-FAIL: the silent-skip arm's PRODUCER failed (rc $rc). The" \
           "whole point of this arm is that the producer notices nothing;" \
           "a producer-side failure means the arm is testing the wrong thing" >&2
      failures=$((failures + 1))
    else
      verify_bundle skip-two two >"$WORK/skip-two.verify" 2>&1
      vrc=$?
      if [[ $vrc -eq 0 ]]; then
        echo "ARM-FAIL: the silent-skip arm PASSED the two-version gate; a" \
             "reload whose source was never bundled was reported as fine" >&2
        failures=$((failures + 1))
      elif ! grep -q "GDH3-FAIL\[gdh3_fork_uses_the_writers_own_path_id\]" \
             "$WORK/skip-two.verify"; then
        echo "ARM-FAIL: the silent-skip arm exited $vrc but not with" \
             "GDH3-FAIL[gdh3_fork_uses_the_writers_own_path_id]:" >&2
        cat "$WORK/skip-two.verify" >&2
        failures=$((failures + 1))
      else
        echo "   RED (rc $vrc): $(head -1 "$WORK/skip-two.verify" | cut -c1-170)"
        redarms=$((redarms + 1))
      fi
    fi
  fi
fi
echo

# ARM 2 — the entry point discards the writer's Result and returns a
#   plausible id.  The gate must go red on last_error being EMPTY, which is
#   the defect the writer's own history records for three other void entry
#   points.
echo "-- arm gdh3FalsifyDiscardResult (Nim: -d:gdh3FalsifierArms -d:gdh3FalsifyDiscardResult)"
if ! build_lib discard -d:gdh3FalsifierArms -d:gdh3FalsifyDiscardResult; then
  tail -30 "$WORK/lib-discard.build.log" >&2
  echo "ARM-FAIL: the discard-result arm's library did not BUILD" >&2
  failures=$((failures + 1))
else
  ARMLIB="$WORK/lib-discard.a"
  if ! build_c "$WORK/gdh3-discard" "$ARMLIB"; then
    tail -20 "$WORK/gdh3-discard.build.log" >&2
    echo "ARM-FAIL: the discard-result arm did not COMPILE" >&2
    failures=$((failures + 1))
  else
    mkdir -p "$WORK/discard"
    timeout "$TIMEOUT" "$WORK/gdh3-discard" refusal "$WORK/discard" \
      >"$WORK/discard.out" 2>&1
    drc=$?
    if [[ $drc -eq 124 ]]; then
      echo "ARM-FAIL: the discard-result arm HUNG (rc 124)" >&2
      failures=$((failures + 1))
    elif [[ $drc -eq 0 ]]; then
      echo "ARM-FAIL: the discard-result arm PASSED the refusal gate" >&2
      failures=$((failures + 1))
    elif ! grep -q "GDH3-FAIL\[gdh3_refused_registration_reaches_the_c_caller\]" \
           "$WORK/discard.out"; then
      echo "ARM-FAIL: the discard-result arm exited $drc but not with" \
           "GDH3-FAIL[gdh3_refused_registration_reaches_the_c_caller]:" >&2
      cat "$WORK/discard.out" >&2
      failures=$((failures + 1))
    else
      echo "   RED (rc $drc): $(grep -o 'GDH3-FAIL\[.*' "$WORK/discard.out" | head -1 | cut -c1-170)"
      redarms=$((redarms + 1))
    fi
  fi
fi
echo

echo "======================================================"
echo "gates green:   $green of 3"
echo "arms gone red: $redarms of 3"
echo "failures:      $failures"
echo "work dir:      $WORK"
if [[ $failures -ne 0 || $green -ne 3 || $redarms -ne 3 ]]; then
  exit 1
fi
echo "GDH-M3: the header declares what the fork needs, the C ABI mints a"
echo "        version, and all three falsifier arms went red in their own gates"
