#!/usr/bin/env bash
# GDH-M2 gate driver — the reload boundary is discoverable in the container.
#
# Three things run here, and none of them is an assertion in prose:
#
#   1. the two container-level gates of `tests/test_gdh2_reload_marker.nim`,
#      unmutated, which must be GREEN;
#   2. every named falsifier arm, each of which must turn ITS gate red;
#   3. GDH-G9 — byte-identity of a no-reload recording against a container
#      produced by a binary built from the PINNED PRE-CAMPAIGN REVISION,
#      plus the two mutations that must break it.
#
# Rules it enforces, each of them a trap this campaign has already been
# bitten by (see `codetracer-specs/Testing/Verification-Harness-Traps.md`):
#
#   * COMPILE and RUN are separate steps. A falsifier that fails to compile
#     also exits non-zero, and counting that as "the gate went red" is a
#     self-pass — the compile must succeed (rc 0) before the run is allowed
#     to mean anything.
#   * A hang is rc 124 and NOTHING ELSE. `timeout` is used, and 124 is a
#     FAILURE of the arm, never a red gate.
#   * The arm must go red IN THE GATE IT IS AIMED AT. The binary takes a
#     gate selector and every failure carries a `GDH2-FAIL[<gate>]` prefix,
#     so "exited non-zero" is never accepted on its own.
#   * The unmutated build must be GREEN first. An arm that is red because
#     the harness is broken proves nothing.
#   * Every dump is asserted NON-EMPTY before any "must contain" check is
#     allowed to mean anything.
#
# Usage:  tests/run_gdh2_gates.sh
# Exit:   0 iff the green run passes, every arm goes red in its gate, and
#         the byte-identity gate passes with both its arms red.

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

WORK="${GDH2_WORK:-$(mktemp -d)}"
mkdir -p "$WORK"
TIMEOUT="${GDH2_TIMEOUT:-300}"
NIMFLAGS=(-d:release -p:src --hints:off --warnings:off)

# zstd is linked by the container layer. The dev shell puts it on the
# default search path; outside it, GDH2_ZSTD_INC / GDH2_ZSTD_LIB point at
# it. Missing headers must FAIL the script, not silently skip the arms.
if [[ -n "${GDH2_ZSTD_INC:-}" ]]; then
  NIMFLAGS+=("--passC:-I${GDH2_ZSTD_INC}")
fi
if [[ -n "${GDH2_ZSTD_LIB:-}" ]]; then
  NIMFLAGS+=("--passL:-L${GDH2_ZSTD_LIB} -lzstd")
fi

SRC=tests/test_gdh2_reload_marker.nim
PROBE=tests/gdh2_identity_probe.nim
EXPECTED=tests/EXPECTED-GDH2.md

fail_script() { echo "DRIVER-FAIL: $*" >&2; exit 1; }

build() {  # build <outbin> <nimcache> <src> [extra defines...]
  local out="$1"; shift
  local cache="$1"; shift
  local src="$1"; shift
  nim c "${NIMFLAGS[@]}" --nimcache:"$cache" -o:"$out" "$@" "$src" \
    >"$out.build.log" 2>&1
}

pass=0
redarms=0
failures=0

# ---------------------------------------------------------------------------
# 0. The pinned baseline, read from EXPECTED-GDH2.md rather than from here.
# ---------------------------------------------------------------------------
[[ -f "$EXPECTED" ]] || fail_script "$EXPECTED is missing; the baseline this
gate compares against is recorded there, and a driver that supplies its own
baseline is comparing a binary with itself"

BASE_REV="$(grep -E '^- +baseline_revision: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +baseline_revision: +//' | tr -d ' ')"
BASE_SHA_EXPECT="$(grep -E '^- +container_sha256: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +container_sha256: +//' | tr -d ' ')"
BASE_SIZE_EXPECT="$(grep -E '^- +container_bytes: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +container_bytes: +//' | tr -d ' ')"
META_SHA_EXPECT="$(grep -E '^- +member_meta_dat_sha256: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +member_meta_dat_sha256: +//' | tr -d ' ')"
STEPS_SHA_EXPECT="$(grep -E '^- +member_steps_dat_sha256: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +member_steps_dat_sha256: +//' | tr -d ' ')"
PINNED_ID="$(grep -E '^- +recording_id: +' "$EXPECTED" | head -1 | \
  sed -E 's/^- +recording_id: +//' | tr -d ' ')"

for v in BASE_REV BASE_SHA_EXPECT BASE_SIZE_EXPECT META_SHA_EXPECT \
         STEPS_SHA_EXPECT PINNED_ID; do
  [[ -n "${!v}" ]] || fail_script "$EXPECTED does not record $v. A baseline
field the driver cannot read is a comparison the driver will not make, and an
unmade comparison is green"
done

# ---------------------------------------------------------------------------
# 1. The unmutated build must be green.
# ---------------------------------------------------------------------------
echo "== green run (no falsifier arm) =="
if ! build "$WORK/green" "$WORK/nc-green" "$SRC"; then
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
if ! grep -q "gdh2_harness_inertness (no falsifier arm compiled in)" "$WORK/green.out"; then
  fail_script "the green run did not assert its own inertness"
fi
pass=1
echo "green run OK (rc 0)"
echo

# ---------------------------------------------------------------------------
# 2. Every named falsifier arm must turn ITS gate red.
# ---------------------------------------------------------------------------
# arm-define : gate-selector : expected GDH2-FAIL gate name
ARMS=(
  "gdh2FalsifyConstantOrdinal:round_trip:gdh2_reload_marker_round_trips"
  "gdh2FalsifyZeroedMarker:round_trip:gdh2_reload_marker_round_trips"
  "gdh2FalsifyUncountedMarker:round_trip:gdh2_reload_marker_round_trips"
  "gdh2FalsifyAlwaysSetBit:round_trip:gdh2_reload_marker_round_trips"
  # Added by review. The only arm on this gate that a well-formed marker
  # can survive: the ids are swapped, so they stay distinct, non-zero and
  # registered, and every check the marker's own bytes can answer still
  # passes. Only the cross-tie to the ids the neighbouring steps resolve
  # to independently can catch it — which is what makes GDH-G7's
  # "cross-tie, never presence" claim a measured property.
  "gdh2FalsifySwappedIds:round_trip:gdh2_reload_marker_round_trips"
  "gdh2FalsifySkipUnknownTag:unknown_tag:gdh2_unknown_tag_is_refused_by_name"
  "gdh2FalsifyUngatedDecode:unknown_tag:gdh2_unknown_tag_is_refused_by_name"
)

for spec in "${ARMS[@]}"; do
  IFS=: read -r armdef gate wantgate <<<"$spec"
  echo "== arm $armdef -> gate $gate =="
  bin="$WORK/arm-$armdef"
  if ! build "$bin" "$WORK/nc-$armdef" "$SRC" -d:gdh2FalsifierArms "-d:$armdef"; then
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
  if ! grep -q "GDH2-FAIL\[$wantgate\]" "$bin.out"; then
    echo "ARM-FAIL: $armdef exited $rc but not with GDH2-FAIL[$wantgate];" \
         "it went red for some other reason:" >&2
    tail -20 "$bin.out" >&2
    failures=$((failures + 1))
    continue
  fi
  echo "RED (rc $rc): $(grep -o 'GDH2-FAIL\[.*' "$bin.out" | head -1 | cut -c1-160)"
  redarms=$((redarms + 1))
  echo
done

# ---------------------------------------------------------------------------
# 3. The INSTRUMENT: the shipped `ct-print` must emit the marker as its own
#    event kind (GDH-M2 deliverable 3), proven by RUNNING the binary.
# ---------------------------------------------------------------------------
echo "== ct-print instrument =="
CTP="$WORK/ct-print"
instrument_ok=1
if ! nim c "${NIMFLAGS[@]}" --mm:arc --nimcache:"$WORK/nc-ctprint" \
     -o:"$CTP" src/codetracer_ct_print.nim >"$WORK/ctprint.build.log" 2>&1; then
  tail -30 "$WORK/ctprint.build.log" >&2
  echo "INSTRUMENT-FAIL: ct-print did not build" >&2
  failures=$((failures + 1))
  instrument_ok=0
else
  CT="$WORK/two_markers.ct"
  PLAIN="$WORK/no_marker.ct"
  if ! timeout "$TIMEOUT" "$WORK/green" dump "$CT" >"$WORK/dump.out" 2>&1; then
    cat "$WORK/dump.out" >&2
    echo "INSTRUMENT-FAIL: could not write the two-marker container" >&2
    failures=$((failures + 1))
    instrument_ok=0
  elif ! timeout "$TIMEOUT" "$WORK/green" dump-plain "$PLAIN" >"$WORK/dumpp.out" 2>&1; then
    cat "$WORK/dumpp.out" >&2
    echo "INSTRUMENT-FAIL: could not write the no-marker container" >&2
    failures=$((failures + 1))
    instrument_ok=0
  else
    "$CTP" --events  "$CT"    >"$WORK/ctp.events" 2>&1
    "$CTP" --full    "$CT"    >"$WORK/ctp.full"   2>&1
    "$CTP" --events  "$PLAIN" >"$WORK/ctp.plain"  2>&1

    # Anti-vacuity: the dumps must be NON-EMPTY before any "must contain"
    # check is allowed to mean anything (trap 4 — a scanner that finds
    # nothing passes every such check).
    for f in ctp.events ctp.full ctp.plain; do
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
    need ctp.events '"kind":"source_reload"' "--events must emit the marker as its OWN event kind"
    need ctp.events '"reload_ordinal":1' "--events must carry the first marker's ordinal"
    need ctp.events '"reload_ordinal":2' "--events must carry the SECOND marker's ordinal"
    need ctp.events '"in_flight_frames":3' "--events must carry the in-flight count (design §5.4)"
    need ctp.events '"generation":2' "--events must carry the wire generation"
    need ctp.full '"has_source_reload": true' "--full must report the extended meta.dat flag"
    need ctp.full '"source_reloads": 2' "--full must count the markers"
    need ctp.plain '"kind":"step"' "the control dump must contain real steps"

    # The CONTROL: a container with no reload must report the key with a
    # ZERO, not omit it.  A key that appears only when a marker exists
    # makes a scan for it pass on a trace that has none AND on a build
    # that cannot see one.
    "$CTP" --full "$PLAIN" >"$WORK/ctp.plainfull" 2>&1
    if [[ ! -s "$WORK/ctp.plainfull" ]]; then
      echo "INSTRUMENT-FAIL: ctp.plainfull is EMPTY" >&2
      instrument_ok=0
    else
      if ! grep -qF '"source_reloads": 0' "$WORK/ctp.plainfull"; then
        echo "INSTRUMENT-FAIL: a no-reload container must report source_reloads: 0, not omit the key" >&2
        instrument_ok=0
      fi
      if ! grep -qF '"has_source_reload": false' "$WORK/ctp.plainfull"; then
        echo "INSTRUMENT-FAIL: a no-reload container must report has_source_reload: false" >&2
        instrument_ok=0
      fi
      if grep -qF '"kind":"source_reload"' "$WORK/ctp.plain"; then
        echo "INSTRUMENT-FAIL: the control dump contains a marker it should not have" >&2
        instrument_ok=0
      fi
    fi

    # COMPLETENESS, by the line-count-equals-header-counts rule: the
    # `--events` dump must account for every exec record.  `counts` comes
    # from --full; the entry lines come from --events.
    n_steps_line=$(grep -c '"kind":"step"' "$WORK/ctp.events")
    n_reload_line=$(grep -c '"kind":"source_reload"' "$WORK/ctp.events")
    c_steps=$(grep -oE '"steps": [0-9]+' "$WORK/ctp.full" | head -1 | grep -oE '[0-9]+')
    c_reloads=$(grep -oE '"source_reloads": [0-9]+' "$WORK/ctp.full" | head -1 | grep -oE '[0-9]+')
    if [[ -z "$c_steps" || -z "$c_reloads" ]]; then
      echo "INSTRUMENT-FAIL: could not read counts.steps / counts.source_reloads from --full" >&2
      instrument_ok=0
    elif [[ "$n_steps_line" -ne "$c_steps" || "$n_reload_line" -ne "$c_reloads" ]]; then
      echo "INSTRUMENT-FAIL: the --events dump is INCOMPLETE:" \
           "$n_steps_line step entries vs counts.steps=$c_steps," \
           "$n_reload_line marker entries vs counts.source_reloads=$c_reloads" >&2
      instrument_ok=0
    elif [[ "$n_reload_line" -eq 0 ]]; then
      echo "INSTRUMENT-FAIL: zero marker entries; every assertion above is vacuous" >&2
      instrument_ok=0
    else
      echo "events dump complete: $n_steps_line steps + $n_reload_line markers"
    fi

    if [[ $instrument_ok -eq 1 ]]; then
      echo "ct-print instrument OK"
      grep -o '{"kind":"source_reload".*' "$WORK/ctp.events" | head -2
    else
      failures=$((failures + 1))
    fi
  fi
fi
echo

# ---------------------------------------------------------------------------
# 4. GDH-G9 — byte-identity against the PINNED PRE-CAMPAIGN revision.
# ---------------------------------------------------------------------------
echo "== gdh2_no_reload_container_is_byte_identical =="
identity_ok=1

WT="$WORK/baseline-worktree"
rm -rf "$WT"
if ! git worktree add --detach "$WT" "$BASE_REV" >"$WORK/wt.log" 2>&1; then
  cat "$WORK/wt.log" >&2
  fail_script "cannot check out the pinned baseline revision $BASE_REV"
fi
cleanup() { git worktree remove --force "$WT" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# THE BASELINE MUST BE ATTRIBUTABLE TO A PRE-CAMPAIGN BUILD.
#
# The entry's second falsifier arm is "regenerate the baseline with the
# post-campaign binary", and the driver must refuse rather than silently
# compare a binary with itself.  Two independent checks, because the
# revision string alone can be pointed at anything:
#   (a) the worktree's resolved commit equals the pinned SHA;
#   (b) the worktree's writer contains NONE of this campaign's symbols.
# (b) is the one that catches a pin updated to a post-campaign commit.
WT_REV="$(git -C "$WT" rev-parse HEAD)"
PIN_REV="$(git rev-parse "$BASE_REV")"
if [[ "$WT_REV" != "$PIN_REV" ]]; then
  echo "IDENTITY-FAIL: the baseline worktree is at $WT_REV, the pin says $PIN_REV" >&2
  identity_ok=0
fi
for sym in TagSourceReload registerSourceReload FlagExtHasSourceReload \
           registerPathVersion; do
  if grep -rq "$sym" "$WT/src"; then
    echo "IDENTITY-FAIL: the baseline tree at $BASE_REV already contains" \
         "\`$sym\`, so it is NOT a pre-campaign build and comparing against" \
         "it would be comparing this campaign with itself" >&2
    identity_ok=0
  fi
done

cp "$PROBE" "$WT/tests/"
if ! ( cd "$WT" && nim c "${NIMFLAGS[@]}" --nimcache:"$WORK/nc-base" \
       -o:"$WORK/probe-base" tests/gdh2_identity_probe.nim ) \
     >"$WORK/probe-base.build.log" 2>&1; then
  tail -30 "$WORK/probe-base.build.log" >&2
  echo "IDENTITY-FAIL: the probe does not compile against the baseline tree." \
       "That means it uses API this campaign added, and the comparison" \
       "cannot be made at all" >&2
  identity_ok=0
fi
if ! build "$WORK/probe-head" "$WORK/nc-head" "$PROBE"; then
  tail -30 "$WORK/probe-head.build.log" >&2
  echo "IDENTITY-FAIL: the probe does not compile against this tree" >&2
  identity_ok=0
fi

if [[ $identity_ok -eq 1 ]]; then
  timeout "$TIMEOUT" "$WORK/probe-base" "$WORK/base.ct" >"$WORK/base.out" 2>&1
  rcb=$?
  timeout "$TIMEOUT" "$WORK/probe-head" "$WORK/head.ct" >"$WORK/head.out" 2>&1
  rch=$?
  [[ $rcb -eq 124 || $rch -eq 124 ]] && fail_script "an identity probe HUNG (rc 124)"
  if [[ $rcb -ne 0 || $rch -ne 0 ]]; then
    cat "$WORK/base.out" "$WORK/head.out" >&2
    echo "IDENTITY-FAIL: an identity probe exited non-zero (base $rcb, head $rch)" >&2
    identity_ok=0
  fi
fi

if [[ $identity_ok -eq 1 ]]; then
  # ANTI-VACUITY, in the order the entry states it.
  # (a) both files exist and are the same non-zero size.
  bsz=$(stat -c%s "$WORK/base.ct" 2>/dev/null || echo 0)
  hsz=$(stat -c%s "$WORK/head.ct" 2>/dev/null || echo 0)
  if [[ "$bsz" -eq 0 || "$hsz" -eq 0 ]]; then
    echo "IDENTITY-FAIL: a container is empty or missing (base $bsz, head $hsz);" \
         "a comparison of two files that both failed to open is equal for free" >&2
    identity_ok=0
  elif [[ "$bsz" -ne "$hsz" ]]; then
    echo "IDENTITY-FAIL: sizes differ (base $bsz, head $hsz)" >&2
    identity_ok=0
  fi
  # (b) the container is NON-TRIVIAL — steps, calls and values all present.
  for f in base head; do
    for k in steps calls values; do
      v=$(grep -oE "^$k=[0-9]+" "$WORK/$f.out" | head -1 | cut -d= -f2)
      if [[ -z "$v" || "$v" -le 0 ]]; then
        echo "IDENTITY-FAIL: $f.ct reports $k=$v; byte-identity over an" \
             "empty container is free" >&2
        identity_ok=0
      fi
    done
    # (c) the recordingId pin took effect ON BOTH SIDES, read back out of
    #     the container rather than echoed from the source.
    rid=$(grep -oE '^recording_id=.*' "$WORK/$f.out" | head -1 | cut -d= -f2-)
    if [[ "$rid" != "$PINNED_ID" ]]; then
      echo "IDENTITY-FAIL: $f.ct carries recording id '$rid', not the pinned" \
           "'$PINNED_ID'. The containers cannot be byte-identical, so a green" \
           "result would mean the comparison did not run" >&2
      identity_ok=0
    fi
    mv=$(grep -oE '^meta_version=[0-9]+' "$WORK/$f.out" | head -1 | cut -d= -f2)
    if [[ "$mv" != "4" ]]; then
      echo "IDENTITY-FAIL: $f.ct is at meta.dat schema version $mv; a recording" \
           "with no reload must stay at 4" >&2
      identity_ok=0
    fi
  done
fi

if [[ $identity_ok -eq 1 ]]; then
  bsha=$(sha256sum "$WORK/base.ct" | cut -d' ' -f1)
  hsha=$(sha256sum "$WORK/head.ct" | cut -d' ' -f1)
  bmeta=$(sha256sum "$WORK/base.ct.member-meta.dat" | cut -d' ' -f1)
  hmeta=$(sha256sum "$WORK/head.ct.member-meta.dat" | cut -d' ' -f1)
  bsteps=$(sha256sum "$WORK/base.ct.member-steps.dat" | cut -d' ' -f1)
  hsteps=$(sha256sum "$WORK/head.ct.member-steps.dat" | cut -d' ' -f1)
  echo "  base   $bsha  ($bsz bytes)  meta $bmeta  steps $bsteps"
  echo "  head   $hsha  ($hsz bytes)  meta $hmeta  steps $hsteps"
  if ! cmp -s "$WORK/base.ct" "$WORK/head.ct"; then
    echo "IDENTITY-FAIL: the two containers DIFFER. A recording with no reload" \
         "must be byte-identical to one produced before this campaign" >&2
    cmp "$WORK/base.ct" "$WORK/head.ct" | head -3 >&2
    identity_ok=0
  fi
  # The COMMITTED digests, which is what stops a future "refresh the
  # golden" from deleting the property without touching an assertion.
  if [[ "$bsha" != "$BASE_SHA_EXPECT" || "$hsha" != "$BASE_SHA_EXPECT" ]]; then
    echo "IDENTITY-FAIL: the container digest is not the one recorded in" \
         "$EXPECTED ($BASE_SHA_EXPECT). base=$bsha head=$hsha" >&2
    identity_ok=0
  fi
  if [[ "$bsz" != "$BASE_SIZE_EXPECT" ]]; then
    echo "IDENTITY-FAIL: the container is $bsz bytes, $EXPECTED says $BASE_SIZE_EXPECT" >&2
    identity_ok=0
  fi
  if [[ "$bmeta" != "$META_SHA_EXPECT" || "$hmeta" != "$META_SHA_EXPECT" ]]; then
    echo "IDENTITY-FAIL: the meta.dat member digest is not the recorded one" \
         "($META_SHA_EXPECT). base=$bmeta head=$hmeta" >&2
    identity_ok=0
  fi
  if [[ "$bsteps" != "$STEPS_SHA_EXPECT" || "$hsteps" != "$STEPS_SHA_EXPECT" ]]; then
    echo "IDENTITY-FAIL: the steps.dat member digest is not the recorded one" \
         "($STEPS_SHA_EXPECT). base=$bsteps head=$hsteps" >&2
    identity_ok=0
  fi
fi

# CONTROL ARM: the byte comparison must be capable of reporting a
# difference.  A container WITH a marker is compared against the
# no-reload one; if THIS came out equal, `cmp` is not looking at
# anything.
if [[ -s "$WORK/two_markers.ct" && -s "$WORK/no_marker.ct" ]]; then
  if cmp -s "$WORK/two_markers.ct" "$WORK/no_marker.ct"; then
    echo "IDENTITY-FAIL: CONTROL ARM — a container with a marker compares" \
         "EQUAL to one without. The comparison is not reading the files" >&2
    identity_ok=0
  else
    echo "  control arm: a container WITH a marker differs, as it must"
  fi
else
  echo "IDENTITY-FAIL: CONTROL ARM — the marker/no-marker containers are missing" >&2
  identity_ok=0
fi

# FALSIFIER ARM 1: set the extended bit unconditionally.  The container
# then moves to schema version 5 and its bytes must differ.
echo "-- identity falsifier arm 1: gdh2FalsifyAlwaysSetBit"
if ! build "$WORK/probe-armed" "$WORK/nc-armed" "$PROBE" \
     -d:gdh2FalsifierArms -d:gdh2FalsifyAlwaysSetBit; then
  tail -20 "$WORK/probe-armed.build.log" >&2
  echo "ARM-FAIL: gdh2FalsifyAlwaysSetBit did not COMPILE" >&2
  failures=$((failures + 1))
else
  timeout "$TIMEOUT" "$WORK/probe-armed" "$WORK/armed.ct" >"$WORK/armed.out" 2>&1
  rca=$?
  if [[ $rca -eq 124 ]]; then
    echo "ARM-FAIL: gdh2FalsifyAlwaysSetBit HUNG (rc 124)" >&2
    failures=$((failures + 1))
  elif [[ $rca -ne 0 ]]; then
    cat "$WORK/armed.out" >&2
    echo "ARM-FAIL: gdh2FalsifyAlwaysSetBit's probe exited $rca before producing" \
         "a container; a crash is not a byte difference" >&2
    failures=$((failures + 1))
  elif cmp -s "$WORK/base.ct" "$WORK/armed.ct"; then
    echo "ARM-FAIL: gdh2FalsifyAlwaysSetBit produced a container BYTE-IDENTICAL" \
         "to the baseline; the gate did not go red" >&2
    failures=$((failures + 1))
  else
    amv=$(grep -oE '^meta_version=[0-9]+' "$WORK/armed.out" | head -1 | cut -d= -f2)
    echo "RED: the armed probe's container differs (schema version $amv vs 4)"
    redarms=$((redarms + 1))
  fi
fi

# FALSIFIER ARM 2: point the baseline at THIS tree — "regenerate the
# baseline with the post-campaign binary".  The driver must refuse on the
# pinned revision rather than silently compare a binary with itself.
echo "-- identity falsifier arm 2: baseline regenerated from the post-campaign tree"
HEAD_REV="$(git rev-parse HEAD)"
arm2_red=0
if [[ "$HEAD_REV" == "$PIN_REV" ]]; then
  echo "ARM-FAIL: HEAD is the pinned baseline revision, so this arm cannot" \
       "distinguish anything" >&2
  failures=$((failures + 1))
else
  # (a) the revision check must reject it, and
  # (b) the symbol check must reject it independently.
  if [[ "$HEAD_REV" != "$PIN_REV" ]]; then arm2_red=$((arm2_red + 1)); fi
  found_sym=0
  for sym in TagSourceReload registerSourceReload FlagExtHasSourceReload; do
    if grep -rq "$sym" src; then found_sym=1; fi
  done
  if [[ $found_sym -eq 1 ]]; then arm2_red=$((arm2_red + 1)); fi
  if [[ $arm2_red -eq 2 ]]; then
    echo "RED: a baseline taken from HEAD is rejected on BOTH grounds —" \
         "revision $HEAD_REV != pinned $PIN_REV, and the tree carries this" \
         "campaign's symbols"
    redarms=$((redarms + 1))
  else
    echo "ARM-FAIL: a baseline taken from HEAD was not rejected on both" \
         "grounds (score $arm2_red of 2); the driver would compare a binary" \
         "with itself" >&2
    failures=$((failures + 1))
  fi
fi

if [[ $identity_ok -eq 1 ]]; then
  echo "PASS: gdh2_no_reload_container_is_byte_identical (against $BASE_REV)"
else
  failures=$((failures + 1))
fi
echo

echo "======================================================"
echo "green run:      $pass"
echo "arms gone red:  $redarms of $(( ${#ARMS[@]} + 2 ))"
echo "arm failures:   $failures"
echo "work dir:       $WORK"
if [[ $failures -ne 0 || $redarms -ne $(( ${#ARMS[@]} + 2 )) || $pass -ne 1 ]]; then
  exit 1
fi
echo "GDH-M2: green run passed, every falsifier arm went red in its own gate,"
echo "        and a no-reload recording is byte-identical to $BASE_REV"
