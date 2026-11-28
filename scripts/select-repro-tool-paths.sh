#!/usr/bin/env bash
# Source with the current owning manifest as $1. A nested/re-entered shell
# replaces an older shell's selection, while preserving unrelated variants.
ct_select_repro_tool_paths() {
  local manifest="$1" item variants=""
  local -a assignments
  case "${REPRO_VARIANTS-}" in
    *$'\n'*|*$'\r'*) echo 'invalid multiline REPRO_VARIANTS' >&2; return 1 ;;
  esac
  case "$manifest" in
    *','*|*$'\n'*|*$'\r'*) echo 'tool-path manifest path cannot contain a comma or newline' >&2; return 1 ;;
  esac
  IFS=',' read -r -a assignments <<< "${REPRO_VARIANTS-}"
  for item in "${assignments[@]}"; do
    if [[ "$item" =~ ^[[:space:]]*toolPathsFile[[:space:]]*= ]]; then continue; fi
    if [[ -n "$item" ]]; then variants+="${variants:+,}$item"; fi
  done
  export REPRO_VARIANTS="${variants:+$variants,}toolPathsFile=$manifest"
}
if ct_select_repro_tool_paths "$1"; then
  unset -f ct_select_repro_tool_paths
else
  unset -f ct_select_repro_tool_paths
  return 1
fi
