#!/usr/bin/env bash
# Entering this repository's dev shell from ANOTHER git repository writes
# nothing into that repository.
#
# The shell's hook installs git hooks (`.pre-commit-config.yaml`, a
# pre-commit hook running `just lint`) and seeds a writable NIMBLE_DIR. Both
# used to land in whatever repository encloses the working directory: run
# from a sibling checkout, the hook planted this repo's lint hook there and
# blocked that repository's commits. flake.nix now anchors both to this
# repository.
#
# Asserted, from a scratch git repository (and from a subdirectory of it):
#   * no `.pre-commit-config.yaml`, no installed hook, no `.nimble/`, no
#     `core.hooksPath` change -- `git status --ignored` stays empty;
# and, as the positive control, entered from this repository the hook still
# installs: `.pre-commit-config.yaml` is the git-hooks.nix symlink.
#
#   bash tests/test_dev_shell_writes_nothing_elsewhere.sh
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

git -C "$SCRATCH" init -q
git -C "$SCRATCH" -c user.name=t -c user.email=t@t commit -q --allow-empty -m init
mkdir -p "$SCRATCH/sub"
hooks_before="$(ls "$SCRATCH/.git/hooks")"

for dir in "$SCRATCH" "$SCRATCH/sub"; do
  ( cd "$dir" && env -u NIMBLE_DIR nix develop "$REPO" --no-write-lock-file -c true ) \
    >/dev/null 2>&1 || fail "the dev shell did not start from $dir"
  [ ! -e "$SCRATCH/.pre-commit-config.yaml" ] || fail "entered from $dir: .pre-commit-config.yaml was written into the other repository"
  [ ! -e "$SCRATCH/.nimble" ] && [ ! -e "$SCRATCH/sub/.nimble" ] || fail "entered from $dir: a .nimble directory was seeded into the other repository"
  [ "$(ls "$SCRATCH/.git/hooks")" = "$hooks_before" ] || fail "entered from $dir: git hooks were installed into the other repository"
  [ -z "$(git -C "$SCRATCH" config --local --get core.hooksPath || true)" ] || fail "entered from $dir: core.hooksPath was changed"
  [ -z "$(git -C "$SCRATCH" status --porcelain --ignored)" ] || fail "entered from $dir: the other repository is dirty: $(git -C "$SCRATCH" status --porcelain --ignored)"
done

( cd "$REPO" && nix develop "$REPO" --no-write-lock-file -c true ) >/dev/null 2>&1 || fail "the dev shell did not start from this repository"
[ -L "$REPO/.pre-commit-config.yaml" ] || fail "control: entered from this repository, the hook did not install .pre-commit-config.yaml"

echo "PASS: entered from another repository the dev shell writes nothing there; from this one it installs its hooks"
