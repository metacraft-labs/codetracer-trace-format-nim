#!/usr/bin/env bash
# codetracer-revision.sh -- the codetracer commit the downstream lane
# (.github/workflows/downstream.yml) tests this repository against.
#
# Usage: ci/downstream/codetracer-revision.sh [DISPATCH_REVISION]
#
# Prints `rev=<40-hex>` (a GITHUB_OUTPUT line) and exits 0, taking:
#   1. DISPATCH_REVISION, when it is non-empty -- the workflow_dispatch
#      `codetracer_revision` input. It must be exactly 40 lowercase hex
#      digits: a branch, a tag or a short sha is refused, not resolved;
#   2. otherwise the revision ci/downstream/repro.lock pins for `codetracer`.
# Anything else -- no lock, no codetracer entry, a revision that is not
# 40-hex -- is an error (exit 1). A branch tip is never a fallback
# (codetracer-specs Testing/Cross-Repo-CI-Integration.md: "Required cross-repo
# gates must use the workspace lock or an explicit workflow input override").
#
# The codetracer revision is the only one this lane pins itself. Every other
# consumer (the recorders) is taken at the revision codetracer's own locks
# pin for it at that commit; see ci/downstream/repro.nim.
#
# TFN_DOWNSTREAM_LOCK names another lock file (tests/test_downstream_lane.sh
# uses it for its fixture locks).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lock="${TFN_DOWNSTREAM_LOCK:-$root/ci/downstream/repro.lock}"
input="${1:-}"

err() {
	echo "::error::$*" >&2
	exit 1
}

if [ -n "$input" ]; then
	[[ "$input" =~ ^[0-9a-f]{40}$ ]] ||
		err "codetracer_revision must be a 40-hex codetracer commit (got '$input'); the downstream lane never tests a branch or a tag"
	echo "codetracer revision: $input (from the workflow_dispatch input)" >&2
	echo "rev=$input"
	exit 0
fi

[ -f "$lock" ] || err "$lock does not exist; it is what pins the codetracer revision this lane tests against (see ci/downstream/repro.nim)"
rev="" seen=0
# The lock's `deps` array holds one `{ ... }` entry per pinned repository;
# take the one whose name is exactly "codetracer" (not codetracer-<anything>).
while IFS= read -r entry; do
	if [[ "$entry" =~ (^|[^-a-z])name\ =\ \"codetracer\" ]]; then
		seen=$((seen + 1))
		if [[ "$entry" =~ revision\ =\ \"([^\"]*)\" ]]; then rev="${BASH_REMATCH[1]}"; fi
	fi
done < <(grep '^deps' "$lock" | tr '{' '\n')
[ "$seen" -le 1 ] || err "$lock has $seen codetracer entries; refresh it with 'repro lock refresh .' in ci/downstream"
[ -n "$rev" ] || err "$lock pins no codetracer revision; refresh it with 'repro lock refresh .' in ci/downstream"
[[ "$rev" =~ ^[0-9a-f]{40}$ ]] || err "$lock pins codetracer at '$rev', which is not a 40-hex revision"
echo "codetracer revision: $rev (from $lock)" >&2
echo "rev=$rev"
