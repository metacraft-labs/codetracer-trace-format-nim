#!/usr/bin/env bash
# The workflow text this suite matches carries literal `${{ ... }}` and `${VAR}`
# expressions, which must NOT expand here.
# shellcheck disable=SC2016
#
# test_downstream_lane.sh -- this repository has a downstream lane that holds a
# change against its consumers before it reaches the mainline, and that lane
# tests THIS commit against pinned consumers.
#
# codetracer reads traces through this repository's reader and most recorders
# write them through its writer; each resolves it as a sibling checkout or a
# pinned flake input. Without .github/workflows/downstream.yml no workflow of
# this repository builds or tests any consumer, so a breaking change is first
# seen when a consumer's own CI next moves its pin (codetracer-pm
# issues/2026-11-26-trace-format-nim-has-no-downstream-lane-though-codetracer-
# and-the-recorders-build-it.md). This test fails on a tree with no such lane,
# and on a lane that:
#   * does not run on push and pull_request to the mainline branches its other
#     workflows run on (dev, stable);
#   * takes codetracer from anywhere but ci/downstream/repro.lock or a 40-hex
#     workflow_dispatch input -- in particular from a branch tip;
#   * does not run every consumer the driver defines;
#   * lets the sibling provisioning reset this checkout (no `exclude:`);
#   * whose lock does not pin codetracer at a 40-hex revision.
# It also RUNS the revision resolver (ci/downstream/codetracer-revision.sh)
# against the committed lock and fixture locks, and the driver
# (ci/downstream/run-consumers.sh) in its dry-run mode against fixture
# layouts, so their refusals are observed rather than read off the source.
#
# The inputs are this repository's own files, temporary copies of them, and
# directory layouts built here. The fixture layouts stand in for a real
# sibling workspace only to the extent the driver's LAYOUT checks read
# (directories and one symlink). The consumers themselves are run for real by
# the workflow.
#
# ONE STAND-IN, AND WHY: the last section puts a `nix` script first on PATH
# so the driver's VERDICT (a failed consumer, one that ran zero tests, one
# that passed) can be observed without building codetracer and the recorders,
# which takes hours and the network. The stand-in replaces only the dev-shell
# entry the driver calls; the driver's own code -- the swap guard, the
# per-consumer status, the test counting and the exit status -- runs
# unchanged. Whether the real consumers count and fail correctly is shown by
# running them (the workflow, and `just test-downstream`).
#
# Run: bash tests/test_downstream_lane.sh   (bash, grep, sed, awk; no network)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
WF="$ROOT/.github/workflows/downstream.yml"
OTHER="$ROOT/.github/workflows/ci-reprobuild.yml"
NIM="$ROOT/ci/downstream/repro.nim"
RESOLVER="$ROOT/ci/downstream/codetracer-revision.sh"
DRIVER="$ROOT/ci/downstream/run-consumers.sh"
# The number of assertions this suite makes when every section runs. A
# section that is deleted or short-circuited changes it, and the suite fails.
EXPECTED_ASSERTIONS=52

pass=0 failn=0
ok() {
	pass=$((pass + 1))
	echo "  ok   $1"
}
fail() {
	failn=$((failn + 1))
	echo "  FAIL $1"
	shift
	for l in "$@"; do echo "       $l"; done
}
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# The top-level `on:` block's `<event>:` section, as text.
on_event() { # $1 = event name, $2 = workflow file (default: the lane)
	awk -v ev="$1" '
		/^on:/ { inon = 1; next }
		inon && /^[^ #]/ { inon = 0 }
		inon && $0 ~ "^  " ev ":" { inev = 1; next }
		inon && inev && /^  [^ ]/ { inev = 0 }
		inon && inev { print }' "${2:-$WF}"
}
# The `branches:` list of an `on:` event, one name per line, sorted.
event_branches() { # $1 = event, $2 = workflow file (default: the lane)
	on_event "$1" "${2:-$WF}" | sed -n 's/^ *branches: *\[\(.*\)\].*/\1/p' | tr ',' '\n' | tr -d ' ' | sort
}
# Every sibling ci/downstream/run-consumers.sh's consumer_needs() names, one per
# line: the quoted words of its `echo "..."` lines.
driver_needs() {
	sed -n '/^consumer_needs()/,/^}/p' "$DRIVER" | sed -n 's/.*echo "\([^"]*\)".*/\1/p' | tr ' ' '\n' | sort -u
}
concurrency_of() {
	awk '/^concurrency:/ { c = 1; print; next } c && /^[^ ]/ { c = 0 } c { print }' "$1"
}

echo "== the downstream workflow exists"
if [ ! -f "$WF" ]; then
	fail "$WF exists" "this repository has no workflow that builds or tests a consumer against a change"
	echo
	echo "test_downstream_lane: $failn of $((pass + failn)) assertions FAILED (stopped: nothing else to check)"
	exit 1
fi
ok "$WF exists"
case "$(basename "$WF")" in
*downstream*) ok "its name says 'downstream' (what the CI test audit looks for)" ;;
*) fail "its name says 'downstream'" ;;
esac

echo "== it runs on push and pull_request to the mainline branches"
for ev in push pull_request; do
	got="$(event_branches "$ev" | paste -sd, -)"
	if [ "$got" = "dev,stable" ]; then
		ok "on.$ev branches are dev and stable"
	else
		fail "on.$ev branches are dev and stable" "got: '$got'"
	fi
	if [ "$got" = "$(event_branches "$ev" "$OTHER" | paste -sd, -)" ]; then
		ok "on.$ev branches match ci-reprobuild.yml's"
	else
		fail "on.$ev branches match ci-reprobuild.yml's"
	fi
done
if grep -q '^      codetracer_revision:' <<<"$(on_event workflow_dispatch)"; then
	ok "workflow_dispatch takes an optional codetracer_revision input"
else
	fail "workflow_dispatch takes a codetracer_revision input" "$(on_event workflow_dispatch)"
fi
if [ -n "$(concurrency_of "$OTHER")" ] && [ "$(concurrency_of "$WF")" = "$(concurrency_of "$OTHER")" ]; then
	ok "the concurrency block is ci-reprobuild.yml's"
else
	fail "the concurrency block is ci-reprobuild.yml's"
fi

echo "== codetracer comes only from the lock or a 40-hex dispatch input"
if grep -qF 'run: bash ci/downstream/codetracer-revision.sh "${INPUT_REV}" >>"${GITHUB_OUTPUT}"' "$WF" &&
	grep -qF 'INPUT_REV: ${{ github.event.inputs.codetracer_revision }}' "$WF"; then
	ok "the resolve job runs ci/downstream/codetracer-revision.sh on the dispatch input alone"
else
	fail "the resolve job runs ci/downstream/codetracer-revision.sh on the dispatch input"
fi
if grep -qF 'CT_REV: ${{ needs.resolve.outputs.rev }}' "$WF"; then
	ok "the consumers job takes the revision from the resolve job"
else
	fail "the consumers job takes the revision from the resolve job"
fi
if grep -qF 'fetch -q --depth 1 origin "${{ steps.plan.outputs.ct-rev }}"' "$WF"; then
	ok "codetracer is fetched at that revision"
else
	fail "codetracer is fetched at that revision"
fi
# No branch for codetracer or for any consumer: `ref: dev`, `codetracer@dev`,
# `<name>=dev` in a siblings list, `refs/heads/...`. Action refs (`uses:
# owner/repo/path@dev`) name the action's revision, not a tested repository.
if grep -nE '(^ *ref: *(main|master|dev|stable|agents)\b|codetracer[-a-z]*(=|@) *(main|master|dev|stable|agents)\b|refs/heads/)' "$WF" |
	grep -v 'uses:' >"$tmp/branches"; then
	fail "the workflow names no branch for a tested repository" "$(cat "$tmp/branches")"
else
	ok "the workflow names no branch for a tested repository"
fi

echo "== it runs every consumer the driver defines, against this checkout"
mapfile -t consumers < <(bash "$DRIVER" --list)
if [ "${#consumers[@]}" -ge 5 ]; then
	ok "the driver defines ${#consumers[@]} consumers (${consumers[*]})"
else
	fail "the driver defines the codetracer readers and four recorders" "got: ${consumers[*]}"
fi
for c in codetracer-readers codetracer-native-recorder codetracer-ruby-recorder codetracer-python-recorder codetracer-polkavm-recorder; do
	if printf '%s\n' "${consumers[@]}" | grep -qx "$c" &&
		grep -qF "run: bash ci/downstream/run-consumers.sh --ct-dir \"\${CT_DIR}\" $c" "$WF"; then
		ok "consumer $c is defined and the workflow runs it"
	else
		fail "consumer $c is defined and the workflow runs it"
	fi
done
steps_run="$(grep -c 'run: bash ci/downstream/run-consumers.sh --ct-dir' "$WF")"
if [ "$steps_run" = "${#consumers[@]}" ]; then
	ok "one workflow step per consumer, and no other"
else
	fail "one workflow step per consumer" "$steps_run steps for ${#consumers[@]} consumers"
fi
if [ "$(grep -c 'if: success() || failure()' "$WF")" -ge "${#consumers[@]}" ]; then
	ok "every consumer step runs whatever happened to the previous one"
else
	fail "every consumer step runs whatever happened to the previous one"
fi
if grep -qE '^ *exclude: *codetracer-trace-format-nim *$' "$WF"; then
	ok "provisioning excludes this repository, so its checkout is never reset"
else
	fail "provisioning passes exclude: codetracer-trace-format-nim"
fi
if grep -qF -- '--self codetracer-trace-format-nim' "$WF"; then
	ok "siblings are resolved from codetracer's locks with this repository as self"
else
	fail "siblings are resolved with --self codetracer-trace-format-nim"
fi
if grep -qF 'is at ${head}, not the commit under test ${SELF_SHA}' "$WF"; then
	ok "the job re-checks that the sibling is still the commit under test"
else
	fail "the job re-checks the sibling after provisioning"
fi
# Every sibling the driver needs is planned in the workflow, so CI clones it.
missing_plan=""
for n in $(driver_needs); do
	[ "$n" = codetracer ] && continue
	grep -qE "^ +echo \"$n\"$" "$WF" || missing_plan+=" $n"
done
if [ -z "$missing_plan" ]; then
	ok "every sibling the driver needs is in the workflow's plan"
else
	fail "every sibling the driver needs is in the workflow's plan" "missing:$missing_plan"
fi

echo "== ci/downstream pins codetracer"
if [ -f "$NIM" ] && grep -qE '^ *"codetracer"$' "$NIM"; then
	ok "ci/downstream/repro.nim uses \"codetracer\""
else
	fail "ci/downstream/repro.nim uses \"codetracer\""
fi
if out="$(bash "$RESOLVER" "" 2>"$tmp/err")" && [[ "$out" =~ ^rev=[0-9a-f]{40}$ ]]; then
	ok "ci/downstream/repro.lock pins codetracer at a 40-hex revision (${out#rev=})"
else
	fail "ci/downstream/repro.lock pins codetracer at a 40-hex revision" "$out" "$(cat "$tmp/err")"
fi

echo "== the resolver accepts only the lock or a 40-hex dispatch input"
sha=0123456789abcdef0123456789abcdef01234567
out="$(bash "$RESOLVER" "$sha" 2>/dev/null)"
if [ "$out" = "rev=$sha" ]; then ok "a 40-hex dispatch input wins over the lock"; else fail "a 40-hex dispatch input wins" "$out"; fi
for bad in dev main agents v1.0.0 0123456 "${sha^^}" "$sha " "-$sha"; do
	if bash "$RESOLVER" "$bad" >/dev/null 2>&1; then
		fail "dispatch input '$bad' is refused"
	else
		ok "dispatch input '$bad' is refused"
	fi
done
entry() { printf '{ name = "%s", coord_kind = "vcs", url = "https://github.com/metacraft-labs/%s", revision = "%s", integrity = "", version = "", visibility = "public", participation = "", depends = "", tags = "" }' "$1" "$1" "$2"; }
mklock() { printf 'schema = "reprobuild.solved-graph-lock.v3"\n\n[lock]\ndeps = [%s]\n' "$1" >"$tmp/lock"; }
resolve_fixture() { TFN_DOWNSTREAM_LOCK="$tmp/lock" bash "$RESOLVER" "" 2>/dev/null; }
mklock "$(entry codetracer-ruby-recorder "$sha"), $(entry codetracer "$sha")"
if [ "$(resolve_fixture)" = "rev=$sha" ]; then
	ok "the lock's exact 'codetracer' entry is read, not a codetracer-* one"
else
	fail "the lock's exact 'codetracer' entry is read"
fi
mklock "$(entry codetracer-ruby-recorder "$sha")"
if resolve_fixture >/dev/null; then fail "a lock with no codetracer entry is refused"; else ok "a lock with no codetracer entry is refused"; fi
mklock "$(entry codetracer dev)"
if resolve_fixture >/dev/null; then fail "a lock pinning codetracer at a branch is refused"; else ok "a lock pinning codetracer at a branch is refused"; fi
mklock "$(entry codetracer "$sha"), $(entry codetracer "${sha%7}8")"
if resolve_fixture >/dev/null; then fail "a lock with two codetracer entries is refused"; else ok "a lock with two codetracer entries is refused"; fi
rm -f "$tmp/lock"
if resolve_fixture >/dev/null; then fail "a missing lock is refused"; else ok "a missing lock is refused"; fi

echo "== the codetracer reader suites are selected by what they compile"
SUITES="$ROOT/ci/downstream/ct-reader-suites.sh"
if [ -f "$SUITES" ] && grep -qF 'ci/downstream/ct-reader-suites.sh' "$DRIVER" && bash -n "$SUITES"; then
	ok "the driver runs ci/downstream/ct-reader-suites.sh, which parses"
else
	fail "the driver runs ci/downstream/ct-reader-suites.sh, which parses"
fi
mkdir -p "$tmp/not-codetracer"
if (cd "$tmp/not-codetracer" && CT_NIM_CACHE_ROOT="$tmp/cache" bash "$SUITES" ct-trace-units >"$tmp/out" 2>&1); then
	fail "ct-reader-suites.sh refuses to run outside a codetracer checkout"
elif grep -q 'run me from a codetracer checkout' "$tmp/out"; then
	ok "ct-reader-suites.sh refuses to run outside a codetracer checkout"
else
	fail "ct-reader-suites.sh refuses to run outside a codetracer checkout, by name" "$(cat "$tmp/out")"
fi
if (cd "$tmp/not-codetracer" && env -u CT_NIM_CACHE_ROOT bash "$SUITES" ct-trace-units >"$tmp/out" 2>&1); then
	fail "ct-reader-suites.sh refuses to run without CT_NIM_CACHE_ROOT"
elif grep -q 'CT_NIM_CACHE_ROOT must be set' "$tmp/out"; then
	ok "ct-reader-suites.sh refuses to run without CT_NIM_CACHE_ROOT"
else
	fail "ct-reader-suites.sh refuses to run without CT_NIM_CACHE_ROOT, by name" "$(cat "$tmp/out")"
fi

echo "== the native recorder rows this lane adds to the recorder's ledger are well formed"
LEDGER="$ROOT/ci/downstream/native-recorder-expected-failures.tsv"
# Each row is `package <TAB> test <TAB> #task <TAB> signature <TAB> why`, for a
# package the lane runs, with a signature: scripts/run-all-nim-tests.sh scores a
# named row expected-fail only when its log carries that signature, so an empty
# one would absorb any failure of the test, including one this library caused.
bad_rows="$(grep -v '^#' "$LEDGER" | grep -v '^$' |
	awk -F'\t' 'NF != 5 || ($1 != "ct_recorder" && $1 != "ct_trace_store") || $3 !~ /^#/ || length($4) < 20 || $5 == "" { print NR ": " $0 }')"
if [ -f "$LEDGER" ] && [ -z "$bad_rows" ] && grep -qF "ci/downstream/native-recorder-expected-failures.tsv" "$DRIVER"; then
	ok "every row names a run package, a task, a real signature and a reason, and the driver passes them"
else
	fail "the native recorder rows are well formed and passed by the driver" "$bad_rows"
fi

echo "== the driver refuses a layout that would not test this checkout"
# A layout: <parent>/codetracer plus every sibling a consumer needs.
mklayout() { # $1 = parent directory
	mkdir -p "$1/codetracer"
	local n
	for n in $(driver_needs); do
		mkdir -p "$1/$n"
	done
}
dry() { TFN_DOWNSTREAM_DRY_RUN=1 TFN_DOWNSTREAM_LOG_DIR="$tmp/logs" bash "$DRIVER" "$@" >"$tmp/out" 2>&1; }

mklayout "$tmp/good"
ln -s "$ROOT" "$tmp/good/codetracer-trace-format-nim"
if dry --ct-dir "$tmp/good/codetracer" all; then
	ok "a layout whose codetracer-trace-format-nim IS this checkout is accepted"
else
	fail "a layout whose codetracer-trace-format-nim is this checkout is accepted" "$(cat "$tmp/out")"
fi

mklayout "$tmp/other"
mkdir -p "$tmp/other/codetracer-trace-format-nim"
if dry --ct-dir "$tmp/other/codetracer" all; then
	fail "a layout whose codetracer-trace-format-nim is ANOTHER checkout is refused"
elif [ $? -eq 2 ] && grep -q 'is not this checkout' "$tmp/out"; then
	ok "a layout whose codetracer-trace-format-nim is another checkout is refused (exit 2, by name)"
else
	fail "a layout whose codetracer-trace-format-nim is another checkout is refused by name" "$(cat "$tmp/out")"
fi

mklayout "$tmp/none"
if dry --ct-dir "$tmp/none/codetracer" all; then
	fail "a layout with no codetracer-trace-format-nim is refused"
elif grep -q 'does not exist' "$tmp/out"; then
	ok "a layout with no codetracer-trace-format-nim is refused"
else
	fail "a layout with no codetracer-trace-format-nim is refused by name" "$(cat "$tmp/out")"
fi

if dry --ct-dir "$tmp/good/codetracer" codetracer-nonexistent-recorder; then
	fail "an unknown consumer is refused"
elif [ $? -eq 2 ] && grep -q "unknown consumer 'codetracer-nonexistent-recorder'" "$tmp/out"; then
	ok "an unknown consumer is refused (exit 2, by name)"
else
	fail "an unknown consumer is refused by name" "$(cat "$tmp/out")"
fi

rm -rf "$tmp/good/codetracer-ruby-recorder"
if dry --ct-dir "$tmp/good/codetracer" codetracer-ruby-recorder; then
	fail "a consumer whose sibling is missing is refused, not skipped"
elif grep -q 'missing sibling(s) beside codetracer: codetracer-ruby-recorder' "$tmp/out"; then
	ok "a consumer whose sibling is missing is refused, not skipped"
else
	fail "a consumer whose sibling is missing is refused by name" "$(cat "$tmp/out")"
fi

echo "== the driver's verdict: a failed consumer, or one that ran no test, fails the lane"
# A dev-shell stand-in (see the header): `nimble buildCtPrint` succeeds, and
# every consumer command prints the given output and exits with the given
# status. ct-print must exist for the driver to go on, so a fixture layout
# links a COPY of this checkout that has one.
mkdir -p "$tmp/bin" "$tmp/verdict"
mkdir -p "$tmp/self/ci"
cp -R "$ROOT/ci/downstream" "$tmp/self/ci/"
: >"$tmp/self/ct-print"
chmod +x "$tmp/self/ct-print"
mkdir -p "$tmp/verdict/codetracer" "$tmp/verdict/codetracer-polkavm-recorder" "$tmp/verdict/codetracer-trace-format"
ln -s "$tmp/self" "$tmp/verdict/codetracer-trace-format-nim"
verdict() { # $1 = shell body of the stand-in for consumer commands
	printf '#!/bin/sh\ncase "$*" in *buildCtPrint*) exit 0 ;; esac\n%s\n' "$1" >"$tmp/bin/nix"
	chmod +x "$tmp/bin/nix"
	PATH="$tmp/bin:$PATH" TFN_DOWNSTREAM_LOG_DIR="$tmp/vlogs" \
		bash "$tmp/self/ci/downstream/run-consumers.sh" --ct-dir "$tmp/verdict/codetracer" codetracer-polkavm-recorder >"$tmp/out" 2>&1
}
if verdict 'echo "test result: FAILED. 3 passed; 1 failed"; exit 101'; then
	fail "a consumer that fails fails the lane" "$(cat "$tmp/out")"
elif grep -q 'CONSUMER codetracer-polkavm-recorder: FAIL (exit 101) tests=3' "$tmp/out"; then
	ok "a consumer that fails fails the lane (exit 1, by name)"
else
	fail "a consumer that fails fails the lane, by name" "$(cat "$tmp/out")"
fi
if verdict 'exit 0'; then
	fail "a consumer that ran no test fails the lane" "$(cat "$tmp/out")"
elif grep -q 'CONSUMER codetracer-polkavm-recorder: FAIL (exit 0, but no test ran) tests=0' "$tmp/out"; then
	ok "a consumer that exits 0 but ran no test fails the lane"
else
	fail "a consumer that ran no test fails the lane, by name" "$(cat "$tmp/out")"
fi
if verdict 'echo "test result: ok. 3 passed; 0 failed"; exit 0' &&
	grep -q 'TEST-COUNTS: consumers=1 passed=1 failed=0 tests=3' "$tmp/out"; then
	ok "a consumer that ran tests and passed passes, counted"
else
	fail "a consumer that ran tests and passed passes, counted" "$(cat "$tmp/out")"
fi

echo
total=$((pass + failn))
if [ "$total" -ne "$EXPECTED_ASSERTIONS" ]; then
	echo "test_downstream_lane: made $total assertions, expected $EXPECTED_ASSERTIONS -- a section was added, removed or short-circuited; update EXPECTED_ASSERTIONS only after reading why"
	exit 1
fi
if [ "$failn" -ne 0 ]; then
	echo "test_downstream_lane: $failn of $total assertions FAILED"
	exit 1
fi
echo "test_downstream_lane: all $total assertions passed"
