#!/usr/bin/env bash
# Reject a CI reference that names a branch which does not exist, or one that
# names a legacy `main`/`master` a repository has moved off.
#
# --- VENDOR-BLOCK BEGIN ----------------------------------------------------
# SOURCED COPY -- local adaptations are recorded below
#
#   upstream:  metacraft-labs/codetracer-trace-format
#   path:      ci/check-ci-refs.sh
#   revision:  f09a2525b4818c0d23d960224838b4a2b175a1cc
#
# Local adaptations add process-only GitHub authentication (which replaces,
# rather than duplicates, a header persisted by actions/checkout), full
# branch-ref parsing, controls for multiline push filters, and a resolver that
# tells an unreachable network apart from a remote that answered with an error
# (with real-git self-tests against a local stub). They are not yet upstream.
# The upstream revision above names
# the source copied here, not a claim of byte identity. Everything repo-specific arrives through
# $WORKFLOW_DIR and $SELF_REPO_URL (the latter defaulting to this repo's
# `origin`). Keep it that way, so the two copies stay diffable by `cmp`.
#
# WHY A COPY, RATHER THAN A SHARED ACTION
#
# The obvious home is `metacraft-labs/metacraft-github-actions`, which both
# repos already consume. It was rejected for one reason: this gate exists to
# detect `uses:` references that have rotted, and a gate delivered BY a
# `uses:` reference can be disabled by the very defect it looks for. An
# absent action fails loudly, so that case is survivable -- but a shared
# action pinned at a frozen `main` is the quiet regime this script's own
# `check_pinned_ref` treats as fatal: it would resolve, run a stale gate,
# and report success. A checker must not depend on the thing it checks.
#
# The original advisory comparison against upstream dev was removed: this copy
# has explicit local adaptations, so byte identity is no longer its contract.
# Its behavior is checked here with hermetic cases and mutation controls.
# --- VENDOR-BLOCK END ------------------------------------------------------
# WHY THIS EXISTS
#
# The `main` -> `dev` migration across this repo family has now produced the
# same defect five separate times, in three different shapes, and none of them
# was caught by anything:
#
#   1. `uses: owner/repo/action@main` where the action repo has no `main`.
#      Every step using it fails to load. Eight of these were live on this
#      repo's `dev`.
#   2. `on: push: branches: [main]` where `main` was deleted. The workflow
#      simply never runs again, and a never-run workflow reports nothing at
#      all -- so the mainline looked clean because it was unchecked.
#   3. `with: ref: main` on a clone step. Five siblings had no `main` (hard
#      failure); one still had a frozen `main` a month behind its `dev`, so CI
#      quietly built a stale tree and reported it as current.
#
# THE TRAP THIS IS BUILT AROUND
#
# `git ls-remote --heads <url> main` exits 0 and prints NOTHING when the ref
# is absent. Any check written around its exit status therefore passes. This
# script tests the OUTPUT, never the status. That single distinction is what
# eight dead references survived.
#
# ASYMMETRY, DELIBERATELY ENCODED
#
# The three shapes do not fail alike, so they are not reported alike. A gate
# that cries wolf on the harmless case gets disabled by whoever hits it first.
#
#   * A `uses:` or `ref:` naming an absent branch is FATAL -- the job breaks.
#   * A `uses:` or `ref:` naming a `main`/`master` that resolves but is NOT
#     that repo's default is FATAL -- this is the quiet regime that builds a
#     frozen tree and reports it as current. It is the one a resolve-only
#     check misses.
#   * A push-trigger branch filter listing a branch that does not exist is
#     only a WARNING: a filter entry that matches nothing is inert.
#   * ...UNLESS no entry in the filter exists, in which case the workflow can
#     never trigger on push at all, and that is FATAL.
#
# Version tags (`@v4`, `@v1.2.3`) and 40-hex SHAs are not branch names and are
# left alone.
#
# USAGE
#   ci/check-ci-refs.sh              scan .github/workflows against the network
#   ci/check-ci-refs.sh --self-test  run the hermetic self-tests (no network)
#
# NETWORK
#   Unreachable network is a FAILURE when $CI is set and a SKIP otherwise, so
#   the gate cannot be quietly satisfied by an offline runner.
#   A remote that ANSWERS with an error (HTTP 400/401/403/404, "Repository
#   not found", ...) is a FAILURE with or without $CI: the network is fine,
#   so the offline SKIP does not apply. Every failure quotes git's own reason.

set -uo pipefail

# Authenticate private-ref queries in this process only. GitHub is the only
# credential destination; no runner-global URL rewrite or credential file.
#
# `http.<url>.extraheader` is MULTI-VALUED: every value git finds is sent.
# `actions/checkout` (default `persist-credentials: true`) writes its own
# `AUTHORIZATION: basic ...` under the same key into .git/config, so simply
# appending ours makes git send two Authorization headers, and GitHub answers
# `remote: Duplicate header: "Authorization"` with HTTP 400 -- for public and
# private repos alike. That is how this gate once failed every query on a
# healthy runner. An EMPTY value resets the list collected so far
# (https://git-scm.com/docs/git-config#Documentation/git-config.txt-httpextraHeader),
# and GIT_CONFIG_KEY_n/VALUE_n entries are read after every config file, so
# the reset-then-set pair below makes our header the only one, however this
# script is invoked. (A *more specific* key such as
# `http.https://github.com/owner/.extraheader` would still win over ours under
# git's URL matching; nothing in this family writes one.)
#
# CI_REFS_GITHUB_HTTP_PREFIX exists only so the self-test can point the same
# mechanism at a local HTTP stub; leave it unset otherwise.
install_git_auth() {
  [ -n "${CI_REFS_GITHUB_TOKEN:-}" ] || return 0
  local key idx
  key="http.${CI_REFS_GITHUB_HTTP_PREFIX:-https://github.com/}.extraheader"
  idx="${GIT_CONFIG_COUNT:-0}"
  export "GIT_CONFIG_KEY_${idx}=$key"
  export "GIT_CONFIG_VALUE_${idx}="
  idx=$((idx + 1))
  export "GIT_CONFIG_KEY_${idx}=$key"
  export "GIT_CONFIG_VALUE_${idx}=AUTHORIZATION: basic $(printf 'x-access-token:%s' "$CI_REFS_GITHUB_TOKEN" | base64 | tr -d '\n')"
  export GIT_CONFIG_COUNT=$((idx + 1))
}
install_git_auth

# Never let a query block on an interactive credential prompt; a missing
# credential must surface as a failure the gate can report.
export GIT_TERMINAL_PROMPT=0
# git's stderr is quoted in this gate's error messages, so a caller's
# GIT_CURL_VERBOSE/GIT_TRACE_CURL must never dump the Authorization header
# unredacted. git redacts by default; pin it so GIT_TRACE_REDACT=0 cannot
# turn that off (https://git-scm.com/docs/git#Documentation/git.txt-GITTRACEREDACT).
export GIT_TRACE_REDACT=1

WORKFLOW_DIR="${WORKFLOW_DIR:-.github/workflows}"
SELF_REPO_URL="${SELF_REPO_URL:-}"

errors=0
warnings=0
refs_checked=0

err()  { printf 'ERROR: %s\n'   "$*" >&2; errors=$((errors + 1)); }
warn() { printf 'WARNING: %s\n' "$*" >&2; warnings=$((warnings + 1)); }
info() { printf '%s\n' "$*"; }

# --------------------------------------------------------------------------
# Resolution
#
# Every resolver prints ONE line on stdout and always exits 0:
#     present | absent                    (resolve_ref)
#     ok<TAB><branch>                     (default_branch; branch may be empty)
#     unreachable<TAB><reason>            the network could not be reached
#     failed<TAB><reason>                 the remote ANSWERED, with an error
#
# The last two are deliberately different outcomes. "Could not reach GitHub"
# is the only condition the offline-developer SKIP is meant for; "GitHub
# answered 400/401/404" is a definite answer that this gate cannot verify the
# reference, and labelling it "network unreachable" once hid a duplicated
# Authorization header behind a message about connectivity.
#
# `CI_REFS_FAKE_RESOLVER` points at a table so the self-tests can exercise
# every branch of the logic hermetically. Lines are:
#     <url><TAB><ref><TAB>present|absent|unreachable|failed[<TAB><reason>]
#     <url><TAB>@default<TAB><branch>
# --------------------------------------------------------------------------

classify_git_failure() {
  # $1 = git's stderr. Prints "unreachable" only for transport failures that
  # mean no answer came back; anything else is a remote (or local) error.
  case "$1" in
    *"Could not resolve host"*|*"Could not resolve proxy"*|\
    *"Temporary failure in name resolution"*|*"Failed to connect"*|\
    *"Couldn't connect to server"*|*"Connection refused"*|\
    *"Connection timed out"*|*"Operation timed out"*|\
    *"Network is unreachable"*|*"No route to host"*|\
    *"Connection reset"*|*"Recv failure"*|*"Send failure"*)
      echo unreachable ;;
    *) echo failed ;;
  esac
}

git_query() {
  # $@ = arguments to `git ls-remote`. On success prints git's stdout and
  # returns 0; on failure prints "<unreachable|failed><TAB><reason>" and
  # returns 1. The reason is git's own stderr, flattened to one line, with
  # any `user:secret@` in a URL masked.
  local errf out rc reason
  errf=$(mktemp) || { printf 'failed\tmktemp failed\n'; return 1; }
  out=$(git ls-remote "$@" 2>"$errf"); rc=$?
  reason=$(tr '\n' ' ' < "$errf" | sed -e 's#://[^/@ ]*@#://***@#g' -e 's/  */ /g' -e 's/ $//')
  rm -f "$errf"
  if [ $rc -ne 0 ]; then
    printf '%s\t%s\n' "$(classify_git_failure "$reason")" \
      "${reason:-git ls-remote exited $rc and printed nothing}"
    return 1
  fi
  printf '%s\n' "$out"
}

resolve_ref() {
  # $1 = repo url, $2 = ref. See the output contract above.
  local url="$1" ref="$2" out
  if [ -n "${CI_REFS_FAKE_RESOLVER:-}" ]; then
    local hit
    hit=$(awk -F'\t' -v u="$url" -v r="$ref" \
          '$1==u && $2==r {print $3 "\t" ($4 == "" ? "fake resolver: " $3 : $4)}' \
          "$CI_REFS_FAKE_RESOLVER")
    case "${hit%%$'\t'*}" in
      present) echo present ;;
      unreachable|failed) printf '%s\n' "$hit" ;;
      *) echo absent ;;
    esac
    return 0
  fi

  # Query heads AND tags: a `uses:` ref is legitimately either.
  if ! out=$(git_query "$url" "refs/heads/$ref" "refs/tags/$ref"); then
    printf '%s\n' "$out"; return 0
  fi
  # THE POINT: judge the OUTPUT, not the status. ls-remote exits 0 for a
  # missing ref.
  if [ -n "$out" ]; then echo present; else echo absent; fi
}

default_branch() {
  # $1 = repo url. See the output contract above.
  local url="$1" out
  if [ -n "${CI_REFS_FAKE_RESOLVER:-}" ]; then
    printf 'ok\t%s\n' "$(awk -F'\t' -v u="$url" '$1==u && $2=="@default" {print $3}' \
      "$CI_REFS_FAKE_RESOLVER")"
    return 0
  fi
  if ! out=$(git_query --symref "$url" HEAD); then
    printf '%s\n' "$out"; return 0
  fi
  printf 'ok\t%s\n' "$(printf '%s\n' "$out" \
    | awk '/^ref:/{sub("refs/heads/","",$2); print $2; exit}')"
}

network_unreachable() {
  # $1 = url, $2 = reason
  if [ -n "${CI:-}" ]; then
    err "cannot reach the network to resolve refs in $1, and \$CI is set. A gate that cannot resolve is not a gate that passes. git said: $2"
  else
    info "SKIP: network unreachable and \$CI is unset; not resolving refs in $1 ($2)."
  fi
}

report_unresolved() {
  # $1 = resolver output line, $2 = url. Returns 0 after reporting when the
  # line is a failure; returns 1 (reporting nothing) when it is an answer.
  local kind="${1%%$'\t'*}" reason="${1#*$'\t'}"
  case "$kind" in
    unreachable) network_unreachable "$2" "$reason"; return 0 ;;
    failed)
      # Fatal with or without $CI: the network is fine, the remote refused.
      err "the remote answered with an error while resolving refs in $2, so this gate cannot tell whether the reference exists. git said: $reason"
      return 0 ;;
  esac
  return 1
}

# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------

check_pinned_ref() {
  # $1 = repo url, $2 = ref, $3 = human location
  local url="$1" ref="$2" where="$3" state deflt
  case "$ref" in
    v[0-9]*|[0-9]*.[0-9]*) return 0 ;;                 # version tag
    *[!0-9a-f]*) : ;;                                  # not a bare sha
    *) [ ${#ref} -eq 40 ] && return 0 ;;               # pinned sha
  esac

  refs_checked=$((refs_checked + 1))
  state=$(resolve_ref "$url" "$ref")
  report_unresolved "$state" "$url" && return 1

  if [ "$state" = "absent" ]; then
    err "$where: '$ref' does not exist in $url — this reference is broken, not merely stale."
    return 0
  fi

  case "$ref" in
    main|master)
      deflt=$(default_branch "$url")
      report_unresolved "$deflt" "$url" && return 1
      deflt=${deflt#ok$'\t'}
      if [ -z "$deflt" ]; then
        network_unreachable "$url" "the remote reported no default branch (HEAD symref)"
        return 1
      fi
      if [ "$deflt" != "$ref" ]; then
        err "$where: '$ref' resolves in $url but that repo's default is '$deflt'. A frozen legacy branch resolves cleanly and silently supplies a stale tree; pin '$deflt' or a SHA."
      fi
      ;;
  esac
}

scan_uses_and_pins() {
  local f base line url ref repo
  for f in "$WORKFLOW_DIR"/*.y*ml; do
    [ -e "$f" ] || continue
    base=$(basename "$f")

    # ---- axis 1: `uses: owner/repo[/path]@ref`
    while IFS= read -r line; do
      ref=${line##*@}
      repo=${line%@*}
      repo=${repo#*uses:}
      repo=$(printf '%s' "$repo" | tr -d " '\"")
      # owner/repo are the first two path components
      url="https://github.com/$(printf '%s' "$repo" | cut -d/ -f1-2)"
      check_pinned_ref "$url" "$ref" "$base: uses: $repo@$ref"
    done < <(tr -d '\r' < "$f" | grep -oE "uses: *[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[A-Za-z0-9_./-]+")

    # ---- axis 3: a `repo:`/`ref:` pair in a clone step
    while IFS=$'\t' read -r repo ref; do
      [ -z "$repo" ] && continue
      url="https://github.com/$repo"
      check_pinned_ref "$url" "$ref" "$base: clones $repo at ref: $ref"
    done < <(tr -d '\r' < "$f" | awk '
      /^[[:space:]]*repo:[[:space:]]*[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+[[:space:]]*$/ {
        r=$2; have=1; next
      }
      have && /^[[:space:]]*ref:[[:space:]]*[A-Za-z0-9_.\/-]+[[:space:]]*$/ {
        print r "\t" $2; have=0; next
      }
      /^[[:space:]]*-[[:space:]]*(name|uses):/ { have=0 }
    ')
  done
}

scan_push_filters() {
  # axis 2: `on: push: branches: [...]` — inert per-entry, fatal if ALL dead.
  local f base list b state live total self
  self="$SELF_REPO_URL"
  if [ -z "$self" ]; then
    self=$(git config --get remote.origin.url 2>/dev/null)
  fi
  [ -z "$self" ] && { warn "no origin remote; skipping push-filter checks"; return 0; }

  for f in "$WORKFLOW_DIR"/*.y*ml; do
    [ -e "$f" ] || continue
    base=$(basename "$f")
    list=$(tr -d '\r' < "$f" | awk '
      /^on:/ { inon=1; inpush=0; inbranches=0; next }
      /^[^[:space:]]/ { inon=0; inpush=0; inbranches=0 }
      inon && /^  push:/ { inpush=1; next }
      inon && /^  [a-z_]+:/ { inpush=0; inbranches=0 }
      inpush && /^    branches:/ {
        inbranches=1; line=$0; sub(/.*branches:[[:space:]]*/,"",line);
        gsub(/[][",]/," ",line); gsub(/\047/,"",line); print line; next
      }
      inbranches && /^      - / {
        line=$0; sub(/^[[:space:]]*-[[:space:]]*/,"",line);
        gsub(/"|\047/,"",line); print line; next
      }
      inbranches && /^    [^ ]/ { inbranches=0 }
    ')
    [ -z "$list" ] && continue

    live=0; total=0
    for b in $list; do
      [ -z "$b" ] && continue
      total=$((total + 1))
      state=$(resolve_ref "$self" "$b")
      report_unresolved "$state" "$self" && return 1
      refs_checked=$((refs_checked + 1))
      if [ "$state" = "present" ]; then
        live=$((live + 1))
      else
        warn "$base: push filter names '$b', which does not exist here. Inert rather than harmful — a filter entry that matches nothing simply never fires — but it is misleading and should go."
      fi
    done

    if [ "$total" -gt 0 ] && [ "$live" -eq 0 ]; then
      err "$base: NOT ONE of its push-trigger branches ($list) exists. This workflow can never run on a push, so it reports nothing and its silence looks like success."
    fi
  done
}

# --------------------------------------------------------------------------
# Real-git self-tests
#
# The table-driven cases above stub the resolver, so they cannot see a defect
# in how git itself is driven -- which is exactly where the duplicated
# Authorization header lived. These cases run the REAL resolver (real git,
# real HTTP) against a local stub server instead of GitHub.
#
# Why a stub server is justified here: the behaviour under test is what git
# puts on the wire and how the script classifies git's failures. The only
# things standing in for GitHub are (a) serving a ref list over git's "dumb"
# HTTP protocol and (b) answering a request carrying two Authorization
# headers with HTTP 400 and `remote: Duplicate header`, the response GitHub
# gives (observed 2026-11-21 with git 2.54 against github.com). Hitting the
# real GitHub would make the self-test depend on the network and a token,
# which is what the hermetic step is defined not to need.
# --------------------------------------------------------------------------

self_test_real_git() {
  if ! command -v python3 >/dev/null 2>&1; then
    if [ -n "${CI:-}" ]; then
      printf '  FAIL real-git cases need python3 for the stub server, and CI is set\n'
      fail=$((fail + 1))
    else
      printf '  skip real-git cases: python3 not found\n'
    fi
    return 0
  fi

  cat > "$tmp/stub.py" <<'PYSTUB'
import http.server, sys
SHA = "1" * 40
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype="text/plain"):
        b = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
    def do_GET(self):
        auth = self.headers.get_all("Authorization") or []
        if len(auth) > 1:
            # A text/plain error body: git prints it as `remote: ...`.
            msg = 'Duplicate header: "Authorization"\n'
            return self.send(400, msg)
        if "/denied/" in self.path:
            return self.send(403, "forbidden\n")
        if self.path.split("?")[0].endswith("/info/refs"):
            # Plain text/plain: git falls back to the dumb protocol.
            return self.send(200, SHA + "\trefs/heads/dev\n")
        self.send(404, "not found\n")
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
open(sys.argv[1], "w").write(str(srv.server_address[1]))
srv.serve_forever()
PYSTUB
  python3 "$tmp/stub.py" "$tmp/port" & local stub_pid=$!
  local port=""
  for _ in $(seq 50); do
    [ -s "$tmp/port" ] && { port=$(cat "$tmp/port"); break; }
    sleep 0.1
  done
  if [ -z "$port" ]; then
    kill "$stub_pid" 2>/dev/null
    printf '  FAIL the local HTTP stub did not start\n'; fail=$((fail + 1)); return 0
  fi
  local base="http://127.0.0.1:$port/" me
  # The cases below cd into a scratch checkout, so $0 must be absolute.
  me="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

  # A checkout like the one actions/checkout leaves: a persisted extraheader
  # under the exact key the script uses.
  git init -q "$tmp/checkout"
  git -C "$tmp/checkout" config http."$base".extraheader \
    "AUTHORIZATION: basic $(printf 'x-access-token:persisted' | base64)"
  mkdir -p "$tmp/realwf"
  cat > "$tmp/realwf/w.yml" <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - run: true
Y

  real_case() { # $1 desc, $2 expect(pass|fail), $3 want-regex, $4 self url, $5.. env
    local desc="$1" expect="$2" want="$3" self="$4"; shift 4
    # Drop any real token the caller exported: only the cases' own fake
    # token may reach the stub (and the refused case must send none at all).
    out=$(cd "$tmp/checkout" && env -u CI_REFS_FAKE_RESOLVER -u CI_REFS_GITHUB_TOKEN CI=1 \
            CI_REFS_GITHUB_HTTP_PREFIX="$base" SELF_REPO_URL="$self" \
            WORKFLOW_DIR="$tmp/realwf" "$@" "$me" 2>&1); rc=$?
    if { { [ "$expect" = pass ] && [ $rc -eq 0 ]; } || \
         { [ "$expect" = fail ] && [ $rc -ne 0 ]; }; } && \
       printf '%s' "$out" | grep -qE "$want"; then
      printf '  ok   %s\n' "$desc"; pass=$((pass + 1))
    else
      printf '  FAIL %s (rc=%d, expected %s)\n%s\n' "$desc" "$rc" "$expect" "$out"
      fail=$((fail + 1))
    fi
  }

  # Control: the stub really rejects a doubled header, so the next case is
  # not passing vacuously. Appending without the reset reproduces the bug.
  out=$(cd "$tmp/checkout" && GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=1 \
        GIT_CONFIG_KEY_0="http.$base.extraheader" \
        GIT_CONFIG_VALUE_0="AUTHORIZATION: basic eDp5" \
        git ls-remote "${base}acme/self" refs/heads/dev 2>&1)
  if printf '%s' "$out" | grep -q 'Duplicate header'; then
    printf '  ok   control: a doubled Authorization header is rejected by the stub\n'
    pass=$((pass + 1))
  else
    printf '  FAIL control: the stub accepted a doubled Authorization header\n%s\n' "$out"
    fail=$((fail + 1))
  fi

  real_case "real git: token + persisted checkout header sends ONE header" pass \
    'no broken references' "${base}acme/self" CI_REFS_GITHUB_TOKEN=selftest-token
  real_case "real git: an HTTP 403 is reported as a remote error with git's reason" fail \
    'remote answered with an error.*403' "${base}denied/self" CI_REFS_GITHUB_TOKEN=selftest-token

  kill "$stub_pid" 2>/dev/null; wait "$stub_pid" 2>/dev/null

  # The stub is gone, so the port now refuses connections: a transport
  # failure, which must keep the "network" label and fail closed under CI.
  real_case "real git: a refused connection is reported as an unreachable network" fail \
    'cannot reach the network.*(refused|connect)' "${base}acme/self"
}

# --------------------------------------------------------------------------
# Self-tests — the mutation controls
# --------------------------------------------------------------------------

self_test() {
  local tmp rc out pass=0 fail=0
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' RETURN

  cat > "$tmp/resolver" <<'TBL'
https://github.com/acme/actions	main	absent
https://github.com/acme/actions	dev	present
https://github.com/acme/actions	feature/kept	present
https://github.com/acme/actions	@default	dev
https://github.com/acme/frozen	main	present
https://github.com/acme/frozen	dev	present
https://github.com/acme/frozen	@default	dev
https://github.com/acme/self	dev	present
https://github.com/acme/self	main	absent
https://github.com/acme/self	stable	present
https://github.com/acme/offline	main	unreachable
https://github.com/acme/denied	main	failed	remote: Duplicate header: "Authorization" fatal: unable to access 'https://github.com/acme/denied/': The requested URL returned error: 400
https://github.com/acme/unknown	main	present
TBL

  run_case() { # $1 desc, $2 dir, $3 expect(pass|fail), $4.. env
    local desc="$1" dir="$2" expect="$3"; shift 3
    out=$(env CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
              SELF_REPO_URL="https://github.com/acme/self" \
              WORKFLOW_DIR="$dir" "$@" "$0" 2>&1); rc=$?
    if { [ "$expect" = pass ] && [ $rc -eq 0 ]; } || \
        { [ "$expect" = fail ] && [ $rc -ne 0 ]; }; then
      printf '  ok   %s\n' "$desc"; pass=$((pass + 1))
    else
      printf '  FAIL %s (rc=%d, expected %s)\n%s\n' "$desc" "$rc" "$expect" "$out"
      fail=$((fail + 1))
    fi
  }

  mk() { mkdir -p "$tmp/$1"; cat > "$tmp/$1/w.yml"; }

  # A: uses: an absent branch -> fatal. Proves the scanner SEES the needle.
  mk a <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@main
Y
  run_case "uses: @main that does not exist is fatal" "$tmp/a" fail

  # B: the same file corrected -> clean. Proves A failed for its stated
  # reason and not because the fixture is malformed.
  mk b <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "uses: @dev resolves and passes" "$tmp/b" pass

  # C: the quiet regime -- resolves, but is not the default.
  mk c <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/frozen/act@main
Y
  run_case "uses: a resolving but non-default @main is fatal" "$tmp/c" fail

  # D: every push-filter branch dead -> fatal (workflow can never run).
  mk d <<'Y'
on:
  push:
    branches: [main, master]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "push filter with no live branch is fatal" "$tmp/d" fail

  # E: one dead + one live -> warn only. The harmless case must NOT fail,
  # or the gate gets switched off by the first person it annoys.
  mk e <<'Y'
on:
  push:
    branches: [main, dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "push filter with a dead entry but a live one only warns" "$tmp/e" pass

  # F: clone pin at an absent ref -> fatal.
  mk f <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/clone@dev
        with:
          repo: acme/actions
          ref: main
Y
  run_case "clone-step ref: main that does not exist is fatal" "$tmp/f" fail

  # G: version tags and SHAs are not branches and must be ignored.
  mk g <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: actions/checkout@v4
      - uses: acme/actions/setup@dev
Y
  run_case "version tags are left alone" "$tmp/g" pass

  # H: unreachable network fails under CI...
  mk h <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/offline/act@main
Y
  out=$(env CI=1 CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/h" "$0" 2>&1); rc=$?
  if [ $rc -ne 0 ]; then printf '  ok   unreachable network fails with CI set\n'; pass=$((pass+1))
  else printf '  FAIL unreachable network should fail with CI set\n'; fail=$((fail+1)); fi

  # ...and skips without it.
  out=$(env -u CI CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/h" "$0" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then printf '  ok   unreachable network skips with CI unset\n'; pass=$((pass+1))
  else printf '  FAIL unreachable network should skip with CI unset\n%s\n' "$out"; fail=$((fail+1)); fi

  # I: NON-VACUITY. A scanner that silently matched nothing would pass every
  # case above. Assert it actually examined refs in a known-populated tree.
  out=$(env CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/b" CI_REFS_REPORT_COUNT=1 "$0" 2>&1)
  if printf '%s' "$out" | grep -q 'refs checked: [1-9]'; then
    printf '  ok   the scanner reports a non-zero ref count (not vacuous)\n'; pass=$((pass+1))
  else
    printf '  FAIL scanner examined zero refs; every case above would pass vacuously\n%s\n' "$out"
    fail=$((fail+1))
  fi

  mk slashgood <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@feature/kept
Y
  run_case "full slashed ref resolves without truncation" "$tmp/slashgood" pass
  sed 's@feature/kept@feature/missing@' "$tmp/slashgood/w.yml" > "$tmp/slashbad.yml"
  mkdir -p "$tmp/slashbad"
  mv "$tmp/slashbad.yml" "$tmp/slashbad/w.yml"
  run_case "missing full slashed ref is fatal" "$tmp/slashbad" fail

  mk multilinebad <<'Y'
on:
  push:
    branches:
      - main
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "multiline push filter with no live branch is fatal" "$tmp/multilinebad" fail
  mkdir -p "$tmp/multilinegood"
  sed '/      - main/a\      - dev' "$tmp/multilinebad/w.yml" > "$tmp/multilinegood/w.yml"
  run_case "multiline push filter with a live branch only warns" "$tmp/multilinegood" pass
  mk quoted <<'Y'
on:
  push:
    branches: ['dev']
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "quoted inline branch filter resolves" "$tmp/quoted" pass

  mk unknowndefault <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/unknown/setup@main
Y
  run_case "unknown default branch fails closed in CI" "$tmp/unknowndefault" fail CI=1

  # J: a remote that ANSWERS with an error is not an unreachable network.
  # It must fail even without $CI (the offline SKIP is not for it), and the
  # message must carry git's reason instead of blaming connectivity.
  mk denied <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/denied/act@main
Y
  expect_output() { # $1 desc, $2 want-regex, $3 reject-regex ('' = none), $4.. env
    local desc="$1" want="$2" reject="$3"; shift 3
    out=$(env "$@" CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
              SELF_REPO_URL="https://github.com/acme/self" "$0" 2>&1); rc=$?
    if [ $rc -ne 0 ] && printf '%s' "$out" | grep -qE "$want" && \
       { [ -z "$reject" ] || ! printf '%s' "$out" | grep -qE "$reject"; }; then
      printf '  ok   %s\n' "$desc"; pass=$((pass + 1))
    else
      printf '  FAIL %s (rc=%d)\n%s\n' "$desc" "$rc" "$out"; fail=$((fail + 1))
    fi
  }
  expect_output "a remote HTTP error fails under CI, labelled as a remote error" \
    'remote answered with an error.*Duplicate header' 'cannot reach the network' \
    CI=1 WORKFLOW_DIR="$tmp/denied"
  expect_output "a remote HTTP error fails without CI too (no offline skip)" \
    'remote answered with an error.*returned error: 400' 'SKIP' \
    -u CI WORKFLOW_DIR="$tmp/denied"
  expect_output "an unreachable network is labelled as such, not as a remote error" \
    'cannot reach the network' 'remote answered' CI=1 WORKFLOW_DIR="$tmp/h"

  self_test_real_git

  printf 'self-test: %d passed, %d failed\n' "$pass" "$fail"
  [ "$fail" -eq 0 ]
}

# --------------------------------------------------------------------------

if [ "${1:-}" = "--self-test" ]; then
  self_test; exit $?
fi

scan_uses_and_pins
scan_push_filters

if [ -n "${CI_REFS_REPORT_COUNT:-}" ] || [ -z "${CI_REFS_FAKE_RESOLVER:-}" ]; then
  info "refs checked: $refs_checked"
fi

if [ "$errors" -gt 0 ]; then
  printf '\n%d error(s), %d warning(s).\n' "$errors" "$warnings" >&2
  exit 1
fi
printf '%d warning(s); no broken references.\n' "$warnings"
exit 0
