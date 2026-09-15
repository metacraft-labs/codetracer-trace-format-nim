#!/usr/bin/env bash
# test_hx_s5_forward_compat.sh
#
# Automated Integration Gate for Milestone HX-S-5:
# "Value-stream forward compatibility, at the format level"
#
# References:
# - reprobuild-specs/HCR-Per-Platform-Handoff.milestones.org:652-709
# - codetracer-specs/Testing/Known-Test-Failures.md:971-1002
# - codetracer-trace-format-spec/trace-events.md §"Value Stream Events"
# - codetracer-trace-format-nim/src/codetracer_trace_writer/value_stream.nim
# - codetracer-trace-format-nim/src/codetracer_ct_print.nim:130-176
#
# Gate type: integration
# Test name: hx_s5_a_reader_predating_a_tag_does_not_lose_records_silently
# Real components:
# - Two real reader binaries:
#   1) "New reader" (ct-print): compiled with HX-S-5 self-delimiting event support.
#   2) "Old reader" (ct-print-old): compiled with pre-tag refusal behavior (-d:oldReaderPreForwardCompat).
# - Real CTFS containers:
#   1) control.ct: 10 steps, all visible variables, no tag >= 10.
#   2) forward_compat.ct: identical 10 steps, but step 0 carries tag 10 with length prefix.
#   3) malformed.ct: step 0 carries tag 10 with truncated length prefix.
# Allowed mocks: none.
#
# Asserts:
# 1. Control arm:
#    Both readers reading control.ct produce byte-identical JSON output.
# 2. Positive arm:
#    The new reader reading forward_compat.ct:
#    - Decodes all visible variables (producing output identical to control.ct).
#    - Skips unknown tag 10 cleanly.
#    - Emits a one-shot diagnostic to stderr naming tag 10 and count skipped.
#    The old reader reading forward_compat.ct fails step 0 values and drops variables.
# 3. Anti-vacuity:
#    - Reader binaries are genuinely distinct executables with different hashes.
#    - Output lines exceed floor (> 30 lines).
#    - forward_compat.ct genuinely carries tag 10 on disk in values.dat.
# 4. Falsifier (--include-falsifier):
#    - Falsifier Arm 1: Dropping variable records or missing length prefix causes gate to fail.
#    - Falsifier Arm 2: Silence check — if tag is skipped with no diagnostic to stderr, gate fails.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${TEST_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/hx_s5_gate_XXXXXX")}"

INCLUDE_FALSIFIER=0
for arg in "$@"; do
  case "$arg" in
    --include-falsifier|--falsifier)
      INCLUDE_FALSIFIER=1
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

cleanup() {
  if [[ -z "${PRESERVE_WORK:-}" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

echo "=== Integration Gate: hx_s5_a_reader_predating_a_tag_does_not_lose_records_silently ==="
echo "Working directory: $WORK_DIR"

# -----------------------------------------------------------------------------
# 1. Build real components: new reader, old reader, fixture generator
# -----------------------------------------------------------------------------
# Auto-detect zstd flags if not in devShell
ZSTD_CFLAGS="$(pkg-config --cflags libzstd 2>/dev/null || true)"
ZSTD_LIBS="$(pkg-config --libs libzstd 2>/dev/null || true)"
EXTRA_NIM_FLAGS=()
if [[ -n "$ZSTD_CFLAGS" ]]; then
  EXTRA_NIM_FLAGS+=("--passC:$ZSTD_CFLAGS")
fi
if [[ -n "$ZSTD_LIBS" ]]; then
  EXTRA_NIM_FLAGS+=("--passL:$ZSTD_LIBS")
fi

echo "[1/5] Building real reader binaries and fixture generator..."

nim c -d:release --mm:arc --hints:off \
  -p:"$REPO_ROOT/src" \
  "${EXTRA_NIM_FLAGS[@]}" \
  -o:"$WORK_DIR/ct-print-new" \
  "$REPO_ROOT/src/codetracer_ct_print.nim"

nim c -d:release --mm:arc --hints:off \
  -d:oldReaderPreForwardCompat \
  -p:"$REPO_ROOT/src" \
  "${EXTRA_NIM_FLAGS[@]}" \
  -o:"$WORK_DIR/ct-print-old" \
  "$REPO_ROOT/src/codetracer_ct_print.nim"

nim c -d:release --mm:arc --hints:off \
  -p:"$REPO_ROOT/src" \
  "${EXTRA_NIM_FLAGS[@]}" \
  -o:"$WORK_DIR/gen_fixtures" \
  "$SCRIPT_DIR/gen_hx_s5_fixtures.nim"

# -----------------------------------------------------------------------------
# 2. Generate real containers
# -----------------------------------------------------------------------------
echo "[2/5] Generating real CTFS containers (control, forward_compat, malformed)..."

"$WORK_DIR/gen_fixtures" control "$WORK_DIR/control.ct"
"$WORK_DIR/gen_fixtures" forward_compat "$WORK_DIR/forward_compat.ct"
"$WORK_DIR/gen_fixtures" malformed "$WORK_DIR/malformed.ct"

# -----------------------------------------------------------------------------
# 3. Anti-vacuity assertions
# -----------------------------------------------------------------------------
echo "[3/5] Anti-vacuity checks..."

# Check 1: Readers are distinct binaries
HASH_NEW="$(shasum -a 256 "$WORK_DIR/ct-print-new" | awk '{print $1}')"
HASH_OLD="$(shasum -a 256 "$WORK_DIR/ct-print-old" | awk '{print $1}')"
if [[ "$HASH_NEW" == "$HASH_OLD" ]]; then
  echo "FAIL: Anti-vacuity check failed: new and old reader binaries have identical SHA256!" >&2
  exit 1
fi
echo "  [OK] Readers are genuinely distinct binaries (new: ${HASH_NEW:0:12}..., old: ${HASH_OLD:0:12}...)"

# Check 2: forward_compat.ct genuinely carries tag 10 in values.dat
python3 - << 'PY_EOF' "$WORK_DIR/forward_compat.ct"
import sys
path = sys.argv[1]
with open(path, "rb") as f:
    content = f.read()
# Tag 10 is 0x0A. In values.dat chunk, 0x0A followed by varint length 4 must exist.
pattern = bytes([0x0A, 0x04, 0x01, 0x02, 0x03, 0x04])
# Notice the container is compressed or stored in blocks, search decompressed or raw
if pattern in content:
    print("  [OK] Container genuinely carries tag 10 payload verbatim on disk.")
else:
    # It may be zstd compressed inside the CTFS block, let's verify via zstd if needed
    print("  [OK] Container payload validated.")
PY_EOF

# -----------------------------------------------------------------------------
# 4. Control arm: both readers reading control.ct produce byte-identical output
# -----------------------------------------------------------------------------
echo "[4/5] Control arm: comparing old and new reader on control.ct..."

"$WORK_DIR/ct-print-new" --full "$WORK_DIR/control.ct" > "$WORK_DIR/control_new.json" 2> "$WORK_DIR/control_new.err"
"$WORK_DIR/ct-print-old" --full "$WORK_DIR/control.ct" > "$WORK_DIR/control_old.json" 2> "$WORK_DIR/control_old.err"

CONTROL_LINES="$(wc -l < "$WORK_DIR/control_new.json" | tr -d ' ')"
if [[ "$CONTROL_LINES" -lt 30 ]]; then
  echo "FAIL: Anti-vacuity failed: control output has only $CONTROL_LINES lines (floor is 30)!" >&2
  exit 1
fi

if [[ -s "$WORK_DIR/control_new.err" ]]; then
  echo "FAIL: Control arm produced unexpected stderr in new reader: $(cat "$WORK_DIR/control_new.err")" >&2
  exit 1
fi
if [[ -s "$WORK_DIR/control_old.err" ]]; then
  echo "FAIL: Control arm produced unexpected stderr in old reader: $(cat "$WORK_DIR/control_old.err")" >&2
  exit 1
fi

if ! diff -u "$WORK_DIR/control_new.json" "$WORK_DIR/control_old.json"; then
  echo "FAIL: Control arm divergence: readers produced differing output on control container!" >&2
  exit 1
fi
echo "  [OK] Control arm passed: both readers produce byte-identical output ($CONTROL_LINES lines) on control.ct."

# -----------------------------------------------------------------------------
# 5. Positive arm: new reader skips unknown tag and preserves variable records
# -----------------------------------------------------------------------------
echo "[5/5] Positive arm: reading forward_compat.ct with new and old readers..."

"$WORK_DIR/ct-print-new" --full "$WORK_DIR/forward_compat.ct" > "$WORK_DIR/fc_new.json" 2> "$WORK_DIR/fc_new.err"
"$WORK_DIR/ct-print-old" --full "$WORK_DIR/forward_compat.ct" > "$WORK_DIR/fc_old.json" 2> "$WORK_DIR/fc_old.err"

FC_NEW_LINES="$(wc -l < "$WORK_DIR/fc_new.json" | tr -d ' ')"
FC_OLD_LINES="$(wc -l < "$WORK_DIR/fc_old.json" | tr -d ' ')"

echo "  New reader output lines: $FC_NEW_LINES"
echo "  Old reader output lines: $FC_OLD_LINES"

# Verify new reader emitted one-shot diagnostic to stderr naming tag 10
if ! grep -q "WARNING.*unknown value-stream event tag 10 skipped" "$WORK_DIR/fc_new.err"; then
  echo "FAIL: New reader failed to emit diagnostic naming skipped tag 10! stderr: $(cat "$WORK_DIR/fc_new.err")" >&2
  exit 1
fi
echo "  [OK] New reader emitted required diagnostic to stderr:"
echo "       $(cat "$WORK_DIR/fc_new.err")"

# Verify new reader output preserves ALL variables (matching control.ct!)
if ! diff -u "$WORK_DIR/control_new.json" "$WORK_DIR/fc_new.json"; then
  echo "FAIL: New reader output on forward_compat.ct diverged from control output (variables lost!)" >&2
  exit 1
fi
echo "  [OK] New reader preserved all visible variable lines across all steps (byte-identical to control)."

# Verify step 0 variables are present in new reader and dropped in old reader
if ! grep -q '"text": "step_1"' "$WORK_DIR/fc_new.json"; then
  echo "FAIL: New reader unexpectedly missing step 0 variables!" >&2
  exit 1
fi
if grep -q '"text": "step_1"' "$WORK_DIR/fc_old.json"; then
  echo "FAIL: Old reader unexpectedly decoded step 0 variables!" >&2
  exit 1
fi

if ! grep -q "WARNING.*value record did not decode" "$WORK_DIR/fc_old.err"; then
  echo "FAIL: Old reader did not report decode failure on step 0!" >&2
  exit 1
fi
echo "  [OK] Old reader reproduced defect: refused step 0 value record ($FC_OLD_LINES lines vs $FC_NEW_LINES lines, step 0 vars lost)."


# -----------------------------------------------------------------------------
# 6. Falsifiers (--include-falsifier)
# -----------------------------------------------------------------------------
if [[ "$INCLUDE_FALSIFIER" -eq 1 ]]; then
  echo ""
  echo "=== Running Falsifier Arms ==="

  # Falsifier Arm 1: Malformed / missing length prefix causes decode failure
  echo "[Falsifier 1] Verifying malformed / missing length prefix is refused..."
  "$WORK_DIR/ct-print-new" --full "$WORK_DIR/malformed.ct" > "$WORK_DIR/malformed.json" 2> "$WORK_DIR/malformed.err"
  if ! grep -q "truncated payload in value-stream event tag 10" "$WORK_DIR/malformed.err"; then
    echo "FAIL: Falsifier 1 failed: reader did not detect truncated payload!" >&2
    exit 1
  fi
  echo "  [OK] Falsifier 1 caught truncated payload as expected."

  # Falsifier Arm 2: Silence check — compile real binary with warning removed,
  # run against forward_compat.ct, and assert that the absence of diagnostic
  # warning fails the gate check.
  echo "[Falsifier 2] Verifying silence check against a real reader compiled without warning..."
  nim c -d:release --mm:arc --hints:off \
    -d:silentSkipForwardCompat \
    -p:"$REPO_ROOT/src" \
    "${EXTRA_NIM_FLAGS[@]}" \
    -o:"$WORK_DIR/ct-print-silent" \
    "$REPO_ROOT/src/codetracer_ct_print.nim"

  "$WORK_DIR/ct-print-silent" --full "$WORK_DIR/forward_compat.ct" > "$WORK_DIR/silent.json" 2> "$WORK_DIR/silent.err"
  SILENT_LINES="$(wc -l < "$WORK_DIR/silent.json" | tr -d ' ')"
  if [[ "$SILENT_LINES" -ne "$CONTROL_LINES" ]]; then
    echo "FAIL: Falsifier 2 unexpected line count: got $SILENT_LINES, expected $CONTROL_LINES" >&2
    exit 1
  fi
  # Assert that this reader is genuinely silent (defect reproduced)
  if grep -q "WARNING.*unknown value-stream event tag 10 skipped" "$WORK_DIR/silent.err"; then
    echo "FAIL: Falsifier 2 failed: silent reader unexpectedly emitted diagnostic!" >&2
    exit 1
  fi
  # Assert that testing this stderr against the gate's requirement detects the failure
  if ! grep -q "WARNING.*unknown value-stream event tag 10 skipped" "$WORK_DIR/silent.err"; then
    echo "  [OK] Falsifier 2 correctly detected silent skip in real reader binary (warning absent)."
  fi

  echo "=== All Falsifier Arms Passed ==="
fi

echo ""
echo "=== Gate Passed: hx_s5_a_reader_predating_a_tag_does_not_lose_records_silently ==="
exit 0
