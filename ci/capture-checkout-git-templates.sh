#!/usr/bin/env bash
# PRIVATE proposed read-only actual checkout template authority capture.
# No mocks; real Git initializes an exclusively owned template probe.
# No actual hooks/config/source/index writes; receipts contain hashes only.
set -Eeuo pipefail
capture_stage=initial-shell-safety
capture_failure() {
  local code=$1 line=$2
  printf 'checkout-template-capture failed stage=%s line=%s exit=%s\n' "$capture_stage" "$line" "$code" >&2
  exit "$code"
}
trap 'capture_failure "$?" "$LINENO"' ERR
shopt -s dotglob nullglob
test -z "${GLOBIGNORE-}"
capture_stage=receipt-path
receipt=$1
test ! -e "$receipt"
capture_stage=receipt-creation
mkdir -m 700 "$receipt"
capture_stage=git-environment-selectors
test -z "${GIT_DIR-}${GIT_WORK_TREE-}${GIT_COMMON_DIR-}${GIT_TEMPLATE_DIR-}${GIT_CONFIG_PARAMETERS-}${GIT_CONFIG_COUNT-}"
capture_stage=git-image-resolution
git_exe=$(command -v git)
git_real=$(realpath "$git_exe")
test -f "$git_real" && test -x "$git_real"
capture_stage=git-prefix-authority
case "$git_real" in
  /nix/store/*/bin/git) git_root=${git_real%/bin/git} ;;
  /usr/bin/git)
    # Actual hosted checkout principal; diagnostic only, never hook admission.
    test "$(git --exec-path)" = /usr/lib/git-core
    git_root=/usr ;;
  *) echo 'unqualified checkout Git prefix' >&2; exit 1 ;;
esac
templates="$git_root/share/git-core/templates"
git_body_before=$(sha256sum "$git_real")
capture_stage=template-source-directories
test -d "$templates/hooks" && test ! -L "$templates" && test ! -L "$templates/hooks"
# An inherited template selector is outside this source-qualified authority.
capture_stage=inherited-template-selector
if git config --get init.templateDir >/dev/null; then
  echo 'inherited Git template selector refused' >&2; exit 1
else
  test "$?" = 1
fi
capture_stage=owning-git-topology
own_root=$(git rev-parse --show-toplevel)
test -d "$own_root/.git" && test ! -L "$own_root/.git"
common=$(realpath "$(git rev-parse --path-format=absolute --git-common-dir)")
test "$common" = "$(realpath "$own_root/.git")"
refuse_hooks_path() {
  if git config --get core.hooksPath >/dev/null; then
    echo 'configured hooksPath refused by ordinary checkout capture' >&2
    return 1
  else
    test "$?" = 1
  fi
}
capture_stage=inherited-hooks-path
refuse_hooks_path
record_source() {
  local entry fields name kind mode body
  git ls-files --stage -z > "$receipt/source-index-census-input" || return 1
  while IFS= read -r -d '' entry; do
    fields=${entry%%$'\t'*}; name=${entry#*$'\t'}
    test "${fields##* }" = 0 || return 1
    kind=${fields%% *}
    mode=$(stat -c %a -- "$name") || return 1
    case "$kind" in
      100644|100755) test -f "$name" && test ! -L "$name" || return 1; body=$(sha256sum -- "$name" | cut -d ' ' -f1) || return 1 ;;
      120000) test -L "$name" || return 1; body=$(readlink -z -- "$name" | sha256sum | cut -d ' ' -f1) || return 1 ;;
      *) echo 'unsupported tracked source kind refused' >&2; return 1 ;;
    esac
    printf '%s\0%s\0%s\0' "$entry" "$mode" "$body"
  done < "$receipt/source-index-census-input"
}
capture_stage=owning-hook-directory
hooks="$common/hooks"
test -d "$hooks" && test ! -L "$hooks"
capture_stage=owning-clean-source
test -z "$(git --no-optional-locks status --porcelain=v1 --untracked-files=no)"
capture_stage=source-index-before
source_before=$(record_source | sha256sum)
index_before=$(sha256sum "$common/index")
capture_stage=config-state-before
config_before=$(git config --null --show-origin --show-scope --list | base64 -w0)
record_inventory() {
  local root=$1 member name mode body
  local members=("$root"/*)
  test "${#members[@]}" -gt 0 || { echo 'empty hook inventory refused' >&2; return 1; }
  for member in "${members[@]}"; do
    test -f "$member" && test ! -L "$member" || return 1
    name=${member##*/}
    case "$name" in *.sample) ;; *) echo 'unexpected checkout hook entry refused' >&2; return 1 ;; esac
    mode=$(stat -c %a -- "$member") || return 1
    body=$(sha256sum -- "$member" | cut -d ' ' -f1) || return 1
    printf '%s\t%s\t%s\n' "$name" "$mode" "$body"
  done | LC_ALL=C sort
}
capture_stage=template-inventory-before
record_inventory "$templates/hooks" > "$receipt/template-source-before.tsv"
initialized_umask=$(umask)
capture_stage=actual-probe-initialization
git init --template="$templates" "$receipt/probe" > "$receipt/probe.stdout" 2> "$receipt/probe.stderr"
capture_stage=initialized-hook-inventory
record_inventory "$hooks" > "$receipt/initialized-before.tsv"
capture_stage=probe-hook-inventory
record_inventory "$receipt/probe/.git/hooks" > "$receipt/actual-template-initialized.tsv"
capture_stage=initialized-template-equality
test "$(sha256sum < "$receipt/initialized-before.tsv")" = "$(sha256sum < "$receipt/actual-template-initialized.tsv")"
capture_stage=template-inventory-after
record_inventory "$templates/hooks" > "$receipt/actual-template-source.tsv"
# Hosted source is mutable: require the entire template body/mode inventory
# and executable image unchanged across the read-only probe.
test "$(sha256sum < "$receipt/template-source-before.tsv")" = "$(sha256sum < "$receipt/actual-template-source.tsv")"
capture_stage=git-image-continuity
test "$git_body_before" = "$(sha256sum "$git_real")"
# Revalidate actual whole inventory after probe, before treating capture stable.
capture_stage=initialized-hook-continuity
record_inventory "$hooks" > "$receipt/initialized-after.tsv"
test "$(sha256sum < "$receipt/initialized-before.tsv")" = "$(sha256sum < "$receipt/initialized-after.tsv")"
capture_stage=source-index-config-continuity
test "$source_before" = "$(record_source | sha256sum)"
test "$index_before" = "$(sha256sum "$common/index")"
test "$config_before" = "$(git config --null --show-origin --show-scope --list | base64 -w0)"
unset config_before
test "$common" = "$(realpath "$(git rev-parse --path-format=absolute --git-common-dir)")"
test -d "$own_root/.git" && test ! -L "$own_root/.git"
refuse_hooks_path
capture_stage=receipt-finalization
printf '%s\n' "$initialized_umask" > "$receipt/initialized-umask.txt"
printf '%s\n' "$git_real" > "$receipt/git-image.txt"
sha256sum "$git_real" > "$receipt/git-image.sha256"
printf '%s\n' "$templates" > "$receipt/templates-source.txt"
printf '%s\n' "$(git rev-parse HEAD)" > "$receipt/source-head.txt"
printf '%s\n' 'read-only exact initialized inventory equals actual source constructor; whole state stable' > "$receipt/result.txt"
