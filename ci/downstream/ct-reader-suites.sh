#!/usr/bin/env bash
# ct-reader-suites.sh -- run the files of codetracer's Nim test lanes that
# compile this repository's library, and only those.
#
# Run by ci/downstream/run-consumers.sh (consumer `codetracer-readers`) from
# the codetracer checkout, inside codetracer's `ci` dev shell. codetracer's
# config.nims puts <parent>/codetracer-trace-format-nim/src on the Nim path, so
# a lane file that imports the reader, the writer or the CTFS container --
# directly or through any codetracer module -- compiles THIS checkout's
# sources.
#
# WHY SELECT BY WHAT A FILE COMPILES
#   A lane is a directory's worth of suites, and most of them never touch the
#   trace format (ct-trace-units also holds the recorder-spawn and --version
#   probes; ct-cli-units the launcher and sourcemap suites). Running those
#   here would make this lane red for reasons this repository cannot cause or
#   fix. A hand-kept list of the files that DO read traces would go stale the
#   day codetracer adds a reader suite. So every file of each lane is compiled
#   (codetracer's own runner, `--compile-only`), and the files whose nimcache
#   holds a C file generated from one of this library's modules are the ones
#   run (the same runner, `--file`, with the lane's own flags, environment and
#   scoring).
#
# WHAT FAILS
#   * a lane file that does not compile (it may not compile BECAUSE of this
#     checkout, and a file that did not compile cannot be classified);
#   * a selected file that fails, crashes or runs no case (the runner's own
#     verdict, `ci/lib/test-lane-report.sh`);
#   * a lane in which no file compiles this library: the selection, or the
#     nimcache naming it relies on, has stopped seeing the reader suites, and
#     a lane that tests nothing must not read as green.
#
# Usage (from the codetracer checkout):
#   bash <this checkout>/ci/downstream/ct-reader-suites.sh LANE...
#
# Environment:
#   CT_NIM_CACHE_ROOT   the nimcache root both runner calls share (required,
#                       so the classification reads the caches the compile
#                       wrote).
set -uo pipefail

[ "$#" -gt 0 ] || {
	echo "usage: $0 LANE..." >&2
	exit 2
}
[ -n "${CT_NIM_CACHE_ROOT:-}" ] || {
	echo "ct-reader-suites: CT_NIM_CACHE_ROOT must be set" >&2
	exit 2
}
[ -f ci/lib/run-nim-test-lane.sh ] || {
	echo "ct-reader-suites: run me from a codetracer checkout (no ci/lib/run-nim-test-lane.sh in $PWD)" >&2
	exit 2
}

# The top-level modules and packages under this repository's src/. Nim names a
# module's generated C file after its path relative to the search path it was
# found on, with `/` written `@s` and an `@p`/`@m` prefix
# (`@pcodetracer_ctfs@sbase40.nim.c`), so a C file whose name starts with one of
# these comes from this library.
LIB_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../src" && pwd)"
lib_modules=()
for entry in "$LIB_SRC"/*; do
	base="$(basename "$entry")"
	case "$base" in
	*.nim) lib_modules+=("${base%.nim}") ;;
	*.*) ;;
	*) [ -d "$entry" ] && lib_modules+=("$base") ;;
	esac
done
[ "${#lib_modules[@]}" -gt 0 ] || {
	echo "ct-reader-suites: found no module under $LIB_SRC" >&2
	exit 2
}
lib_re="^@[pm]($(
	IFS='|'
	echo "${lib_modules[*]}"
))(@s|\.nim\.c$)"

# The number of C files in a test's nimcache generated from this library.
library_c_files() { # $1 = nimcache directory
	[ -d "$1" ] || {
		echo 0
		return
	}
	find "$1" -maxdepth 1 -name '*.nim.c' -printf '%f\n' | grep -cE "$lib_re"
}

rc=0
for lane in "$@"; do
	echo "=== $lane: compiling every file, to find the ones that compile this library"
	if ! bash ci/lib/run-nim-test-lane.sh "$lane" --compile-only; then
		echo "ct-reader-suites: $lane: a file did not compile (above); it may be this checkout's doing, so the consumer fails"
		rc=1
	fi
	mapfile -t files < <(NIM_TEST_LANE_SHARD=1/1 bash ci/lib/run-nim-test-lane.sh "$lane" --list-shard)
	selected=0
	for f in "${files[@]}"; do
		[ -n "$f" ] || continue
		n="$(library_c_files "$CT_NIM_CACHE_ROOT/$lane-$(basename "$f" .nim)")"
		if [ "$n" -gt 0 ]; then
			selected=$((selected + 1))
			echo "--- $lane: $f compiles $n C file(s) of this library: running it"
			bash ci/lib/run-nim-test-lane.sh "$lane" --file "$f" || rc=1
		else
			echo "--- $lane: $f compiles nothing of this library: not this lane's concern"
		fi
	done
	if [ "$selected" -eq 0 ]; then
		echo "ct-reader-suites: $lane: no file of ${#files[@]} compiles this library; refusing to report a lane that tested nothing"
		rc=1
	fi
	echo "READER-SUITES: lane=$lane files=${#files[@]} selected=$selected"
done
exit "$rc"
