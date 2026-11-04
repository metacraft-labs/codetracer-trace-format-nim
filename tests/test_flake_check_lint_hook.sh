#!/usr/bin/env bash
# `nix flake check`'s pre-commit check runs the `lint` hook to completion and
# passes it.
#
# The check runs the git-hooks.nix hook set inside the Nix build sandbox.
# Its `lint` hook used to be `entry = "just lint"` with `language = "system"`,
# and nothing put `just` -- or the `nim`, Nim dependencies and `nixfmt` that
# `just lint` needs -- into the sandbox, so the hook failed to start:
#
#   Lint......error: Failed to run hook `lint`
#     caused by: No such file or directory (os error 2)
#
# Asserted, from the build log of `checks.<system>.pre-commit-check`:
#   * the `Lint` hook's status line is present (the positive control: the log
#     really is the hook runner's output and the hook was run);
#   * it reads `Passed`, and the log carries no `Failed to run hook`.
# Only the Lint hook is judged: the check's other hooks (editorconfig,
# end-of-file) report on the tree's content, which is not what this test is
# about, so the derivation's own exit status is not used.
#
# No mocks: this builds the real check derivation with the real hook runner.
#
#   bash tests/test_flake_check_lint_hook.sh
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }

system="$(nix eval --raw --impure --expr builtins.currentSystem)"
drv="$(nix eval --raw "$REPO#checks.$system.pre-commit-check.drvPath")" \
  || fail "could not evaluate checks.$system.pre-commit-check"

# A failing hook fails the build; the log is what is judged, so the exit
# status is deliberately ignored here.
nix build --no-link "$drv^*" >/dev/null 2>&1 || true
log="$(nix log "$drv" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g; s/\x1b\[[0-9]*K//g')" \
  || fail "no build log for $drv"

lint_line="$(printf '%s\n' "$log" | grep -oE 'Lint\.{3,}.*' | tail -n1 || true)"
[ -n "$lint_line" ] || fail "the log of $drv has no Lint hook status line"
if printf '%s\n' "$log" | grep -q 'Failed to run hook'; then
  fail "a hook could not be started: $(printf '%s\n' "$log" | grep -A2 'Failed to run hook')"
fi
case "$lint_line" in
  *Passed) ;;
  *) fail "the Lint hook did not pass: $lint_line
$(printf '%s\n' "$log" | sed -n '/^Lint\./,$p' | head -n 40)" ;;
esac
echo "ok: $lint_line"
