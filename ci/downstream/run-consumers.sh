#!/usr/bin/env bash
# run-consumers.sh -- run this repository's consumers against THIS checkout.
#
# WHY THIS EXISTS
#   codetracer and most recorders build against codetracer-trace-format-nim,
#   either as a sibling checkout or as a pinned flake input. Without this lane
#   a breaking change here is first seen when a consumer's own CI next runs and
#   moves its pin (codetracer-specs Testing/Cross-Repo-CI-Integration.md,
#   "Downstream Breakage Responsibility" and "trace-format-nim downstream
#   lane"). This script runs each consumer's own trace-reading or
#   trace-writing tests, in the consumer's own dev shell, with this checkout
#   as the `codetracer-trace-format-nim` they read.
#
# LAYOUT
#   Every consumer is a sibling of codetracer, and this checkout must be the
#   `codetracer-trace-format-nim` beside them:
#
#     <parent>/
#       codetracer/                    <-- --ct-dir, at the pinned revision
#       codetracer-trace-format-nim/   <-- THIS checkout (the swap guard below)
#       codetracer-trace-format/       <-- the Rust adapter crate the Rust
#                                          recorders path-depend on
#       codetracer-native-recorder/    <-- and every other consumer and the
#       codetracer-native-backend/         siblings it needs (codetracer-visual-replay,
#       codetracer-visual-replay/          nim-stackable-hooks), at the revision
#       ...                                codetracer's locks pin
#
#   .github/workflows/downstream.yml builds exactly this layout in CI.
#
# THE CONSUMERS, AND WHY EACH RUNS WHAT IT RUNS
#   codetracer-readers
#     codetracer's Nim reader suites. codetracer's config.nims puts
#     <parent>/codetracer-trace-format-nim/src on the Nim path, so a suite
#     that reaches the reader, the writer or the CTFS container compiles THIS
#     checkout. ci/downstream/ct-reader-suites.sh compiles every file of the
#     lanes where those suites live and runs the ones that compile this
#     library (it says why it selects by what a file compiles):
#       ct-trace-units        src/ct/trace (ctfs_sources: the reader `ct` opens
#                             recordings with)
#       ct-test-incremental   src/ct_test/incremental (the CTFS trace, seekable
#                             reader and namespace store; one suite compares
#                             against ct-print)
#       mcr-enrichment-units  src/ct/online_sharing (MCR enrichment reads the
#                             container's members)
#       ct-cli-units          src/ct outside trace/ (`ct print`, the streamed
#                             event assembler over this reader)
#     all through codetracer's own lane runner (ci/lib/run-nim-test-lane.sh),
#     which fails a file that ran zero cases.
#   codetracer-native-recorder
#     the Nim recorder imports the writer directly
#     (ct_recorder/nim.cfg: --path:../../codetracer-trace-format-nim/src).
#     Its ct_recorder and ct_trace_store packages are the trace writer and the
#     CTFS store, run through its own exhaustive runner
#     (scripts/run-all-nim-tests.sh), after `just build-ct-mcr`, which that
#     runner requires (it refuses to start without the licensing library and
#     the ct-mcr binary one ct_recorder suite executes) and which links this
#     writer into the recorder binary too.
#   codetracer-ruby-recorder / codetracer-python-recorder /
#   codetracer-polkavm-recorder
#     Rust recorders: they link codetracer-trace-format's
#     codetracer_trace_writer_nim crate, whose build.rs compiles THIS
#     repository's writer (CODETRACER_TRACE_FORMAT_NIM_DIR, set explicitly
#     below; the PREBUILT override is unset so a store-built writer cannot be
#     linked instead). Each runs its Rust tests and its suite that records
#     programs and decodes the traces with ct-print, which is built from THIS
#     checkout and put first (CT_PRINT, PATH).
#
# USAGE
#   ci/downstream/run-consumers.sh --ct-dir DIR CONSUMER... | all
#   ci/downstream/run-consumers.sh --list
#
# Prints one `CONSUMER <name>: PASS|FAIL ... tests=N` line per consumer and a
# final `TEST-COUNTS: consumers=N passed=N failed=N tests=N` line. Exits
#   0  every named consumer passed and ran at least one test;
#   1  a consumer failed, or ran zero tests;
#   2  usage error: unknown consumer, missing sibling, or a layout in which
#      <parent>/codetracer-trace-format-nim is not this checkout.
# Nothing is ever skipped: a consumer that cannot run is an error, by name.
#
# Environment:
#   TFN_DOWNSTREAM_LOG_DIR   where the per-consumer logs go
#                            (default: <this checkout>/build/downstream-logs)
#   TFN_DOWNSTREAM_DRY_RUN=1 check the layout and print the plan, run nothing
#                            (tests/test_downstream_lane.sh uses it)
set -uo pipefail

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

CONSUMERS=(
	codetracer-readers
	codetracer-native-recorder
	codetracer-ruby-recorder
	codetracer-python-recorder
	codetracer-polkavm-recorder
)

# codetracer's lanes (ci/lib/test-lane-files.sh) holding its reader suites.
CT_LANES=(ct-trace-units ct-test-incremental mcr-enrichment-units ct-cli-units)

# The siblings each consumer reads beside codetracer, besides this checkout.
consumer_needs() {
	case "$1" in
	codetracer-readers) echo "codetracer" ;;
	codetracer-native-recorder) echo "codetracer-native-recorder codetracer-native-backend codetracer-visual-replay codetracer-trace-format nim-stackable-hooks" ;;
	codetracer-ruby-recorder) echo "codetracer-ruby-recorder codetracer-trace-format" ;;
	codetracer-python-recorder) echo "codetracer-python-recorder codetracer-trace-format" ;;
	codetracer-polkavm-recorder) echo "codetracer-polkavm-recorder codetracer-trace-format" ;;
	*) return 1 ;;
	esac
}

usage() {
	echo "usage: $0 --ct-dir DIR CONSUMER...|all    (consumers: ${CONSUMERS[*]})" >&2
	echo "       $0 --list" >&2
	exit 2
}
die() {
	echo "run-consumers: $*" >&2
	exit 2
}

ct_dir="" wanted=()
while [ "$#" -gt 0 ]; do
	case "$1" in
	--ct-dir)
		[ "$#" -ge 2 ] || usage
		ct_dir="$2"
		shift 2
		;;
	--list)
		printf '%s\n' "${CONSUMERS[@]}"
		exit 0
		;;
	-h | --help) usage ;;
	all)
		wanted+=("${CONSUMERS[@]}")
		shift
		;;
	-*) die "unknown option '$1'" ;;
	*)
		wanted+=("$1")
		shift
		;;
	esac
done
[ -n "$ct_dir" ] || usage
[ "${#wanted[@]}" -gt 0 ] || usage

for c in "${wanted[@]}"; do
	consumer_needs "$c" >/dev/null || die "unknown consumer '$c' (known: ${CONSUMERS[*]})"
done

[ -d "$ct_dir" ] || die "--ct-dir '$ct_dir' is not a directory"
ct_dir="$(cd "$ct_dir" && pwd -P)"
parent="$(dirname "$ct_dir")"

# THE SWAP GUARD. Every consumer finds the library at
# <parent>/codetracer-trace-format-nim. If that is anything but this checkout,
# the run would test some other revision and report it as this one.
sibling="$parent/codetracer-trace-format-nim"
[ -e "$sibling" ] || die "$sibling does not exist; this checkout must sit there, beside codetracer"
if [ "$(cd "$sibling" && pwd -P)" != "$SELF" ]; then
	die "$sibling is not this checkout ($SELF); the consumers would read another codetracer-trace-format-nim"
fi

missing=()
for c in "${wanted[@]}"; do
	for n in $(consumer_needs "$c"); do
		[ -d "$parent/$n" ] || missing+=("$n (for $c)")
	done
done
[ "${#missing[@]}" -eq 0 ] || die "missing sibling(s) beside codetracer: ${missing[*]}"

log_dir="${TFN_DOWNSTREAM_LOG_DIR:-$SELF/build/downstream-logs}"
mkdir -p "$log_dir"

echo "codetracer-trace-format-nim under test: $SELF @ $(git -C "$SELF" rev-parse HEAD 2>/dev/null || echo unknown)"
for c in "${wanted[@]}"; do
	for n in $(consumer_needs "$c"); do
		echo "  $c reads $n @ $(git -C "$parent/$n" rev-parse HEAD 2>/dev/null || echo unknown)"
	done
done
if [ "${TFN_DOWNSTREAM_DRY_RUN:-0}" = 1 ]; then
	echo "run-consumers: dry run, nothing executed"
	exit 0
fi

# The environment every consumer runs under, applied INSIDE its dev shell so
# the shell cannot replace it: this checkout's writer for the Rust adapter
# crate, no prebuilt writer, and this checkout's ct-print first.
consumer_env=(
	env -u CODETRACER_TRACE_WRITER_NIM_PREBUILT
	"CODETRACER_TRACE_FORMAT_NIM_DIR=$SELF"
	"CODETRACER_TRACE_FORMAT_NIM_SRC=$SELF/src"
	"CT_PRINT=$SELF/ct-print"
	"TFN_SELF=$SELF"
)

# nix develop <flake> --command <consumer_env> bash -c SCRIPT, with PATH
# prefixed by this checkout inside the shell.
in_shell() { # $1 = directory to cd into, $2 = flake ref (relative to $1), $3 = script
	(cd "$1" && nix develop "$2" --command "${consumer_env[@]}" bash -c "export PATH=\"\$TFN_SELF:\$PATH\"; set -euo pipefail; $3")
}

# ct-print, built once from THIS checkout in its own dev shell, with the
# flags its own nimble task owns.
build_ct_print() {
	echo "== building ct-print from $SELF"
	(cd "$SELF" && nix develop . --command nimble buildCtPrint -y) || return 1
	[ -x "$SELF/ct-print" ] || {
		echo "run-consumers: nimble buildCtPrint left no $SELF/ct-print" >&2
		return 1
	}
}

# Count what a log shows was run, per framework. Each consumer names the
# patterns its runners print; the sum is its test count.
count_cargo() { # `test result: ok. N passed; M failed; ...`
	grep -aoE 'test result: [a-zA-Z]+\. [0-9]+ passed' "$1" | awk '{s += $4} END {print s + 0}'
}
count_nextest() { # `Summary [ ... ] N tests run: ...`
	grep -aoE 'Summary \[[^]]*\] +[0-9]+ tests? run' "$1" | grep -oE '[0-9]+ tests? run' | awk '{s += $1} END {print s + 0}'
}
count_pytest() { # `== N passed, ... in Xs ==`
	grep -aoE '[0-9]+ passed' "$1" | grep -aE '^[0-9]+ passed' | tail -n 1 | awk '{print $1 + 0}'
}
count_minitest() { # `N runs, M assertions, F failures, E errors, S skips`
	grep -aoE '^[0-9]+ runs, ' "$1" | awk '{s += $1} END {print s + 0}'
}
count_ct_lanes() { # run-nim-test-lane.sh: `NIM-LANE-COUNTS: lane=... case-ok=N ...` per lane
	grep -a '^NIM-LANE-COUNTS:' "$1" | grep -oE 'case-ok=[0-9]+' | awk -F= '{s += $2} END {print s + 0}'
}
count_native() { # run-all-nim-tests.sh summary: `planned N    total N    pass N ...`
	grep -aE '^planned +[0-9]+ +total +[0-9]+ +pass +[0-9]+' "$1" | awk '{s += $6} END {print s + 0}'
}

run_consumer() { # $1 = consumer, $2 = log file; returns the consumer's status
	local c="$1"
	case "$c" in
	codetracer-readers)
		# One nimcache for the compile and the runs, so the selection reads
		# what the compile wrote; every lane runs even when an earlier one
		# fails, and the consumer fails if any did.
		in_shell "$ct_dir" '.?submodules=1#ci' "export CT_NIM_CACHE_ROOT='$log_dir/ct-nimcache'; bash '$SELF/ci/downstream/ct-reader-suites.sh' ${CT_LANES[*]}"
		;;
	codetracer-native-recorder)
		# The recorder's own expected-failure ledger plus this lane's rows for
		# tests that are red at the pinned recorder for a reason in the
		# recorder (ci/downstream/native-recorder-expected-failures.tsv); each
		# row stays held to its failure signature.
		local baseline="$log_dir/native-recorder-baseline.tsv"
		cat "$parent/$c/tests/gate-expected-failures.tsv" "$SELF/ci/downstream/native-recorder-expected-failures.tsv" >"$baseline" || return 2
		in_shell "$parent/$c" . "just build-ct-mcr; scripts/run-all-nim-tests.sh --baseline '$baseline' ct_recorder ct_trace_store"
		;;
	codetracer-ruby-recorder)
		in_shell "$parent/$c" . "just build-extension; just cargo-test; ruby -Itest -e 'Dir[\"test/test_*.rb\"].each { |f| require File.expand_path(f) }'"
		;;
	codetracer-python-recorder)
		in_shell "$parent/$c" . "just dev; just cargo-test; uv run --group dev --group test --group web pytest codetracer-python-recorder/tests/python"
		;;
	codetracer-polkavm-recorder)
		in_shell "$parent/$c" . "cargo test --locked"
		;;
	esac
}

count_consumer() { # $1 = consumer, $2 = log file
	case "$1" in
	codetracer-readers) count_ct_lanes "$2" ;;
	codetracer-native-recorder) count_native "$2" ;;
	codetracer-ruby-recorder) echo $(($(count_cargo "$2") + $(count_minitest "$2"))) ;;
	codetracer-python-recorder) echo $(($(count_nextest "$2") + $(count_pytest "$2"))) ;;
	codetracer-polkavm-recorder) count_cargo "$2" ;;
	esac
}

if ! build_ct_print >"$log_dir/ct-print.log" 2>&1; then
	cat "$log_dir/ct-print.log"
	echo "CONSUMER ct-print: FAIL (could not build ct-print from this checkout; see above)"
	echo "TEST-COUNTS: consumers=${#wanted[@]} passed=0 failed=${#wanted[@]} tests=0"
	exit 1
fi

passed=0 failed=0 total=0 results=()
for c in "${wanted[@]}"; do
	log="$log_dir/$c.log"
	echo "== $c (log: $log)"
	run_consumer "$c" "$log" 2>&1 | tee "$log"
	status="${PIPESTATUS[0]}"
	n="$(count_consumer "$c" "$log")"
	n="${n:-0}"
	total=$((total + n))
	if [ "$status" -eq 0 ] && [ "$n" -gt 0 ]; then
		passed=$((passed + 1))
		results+=("CONSUMER $c: PASS tests=$n")
	elif [ "$status" -eq 0 ]; then
		failed=$((failed + 1))
		results+=("CONSUMER $c: FAIL (exit 0, but no test ran) tests=0")
	else
		failed=$((failed + 1))
		results+=("CONSUMER $c: FAIL (exit $status) tests=$n")
	fi
done

printf '%s\n' "${results[@]}"
echo "TEST-COUNTS: consumers=${#wanted[@]} passed=$passed failed=$failed tests=$total"
[ "$failed" -eq 0 ]
