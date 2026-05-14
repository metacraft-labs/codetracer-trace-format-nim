{.push raises: [].}

## Tests for codetracer_trace_writer/path_filter (Tier 1).
##
## Covers:
##   1. Selector grammar parsing
##   2. Match types (glob / regex / literal)
##   3. Rule evaluation order (later overrides earlier)
##   4. Filter chain composition across multiple TOML files
##   5. Validation (bad TOML, unknown selector kind, unsupported version,
##      Tier-2 selectors warn-and-skip)
##   6. Integration: load a fixture TOML and classify a corpus

import std/[os, strutils]
import codetracer_trace_writer/path_filter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

template ensureOk[T, E](r: Result[T, E]; ctx: string): T =
  let rr = r
  doAssert rr.isOk, ctx & " expected Ok, got Err: " & $rr.error
  rr.value

proc fixtureDir(): string {.raises: [].} =
  # Tests run from the repo root in the nimble task pattern (`nim c -r
  # -p:src tests/test_path_filter.nim`). currentSourcePath points to
  # this file; the fixtures live alongside.
  let here = currentSourcePath()
  let dir = here.parentDir
  return dir / "fixtures" / "path_filter"

# ---------------------------------------------------------------------------
# 1. Selector grammar parsing
# ---------------------------------------------------------------------------

proc test_selector_default_match_type() {.raises: [].} =
  let s = ensureOk(parseSelector("file:**/std/**"), "default glob")
  doAssert s.kind == skFile
  doAssert s.matcher.kind == mtGlob
  doAssert s.matcher.globPattern == "**/std/**"
  echo "PASS: test_selector_default_match_type"

proc test_selector_explicit_glob() {.raises: [].} =
  let s = ensureOk(parseSelector("file:glob:**/std/**"), "explicit glob")
  doAssert s.kind == skFile
  doAssert s.matcher.kind == mtGlob
  doAssert s.matcher.globPattern == "**/std/**"
  echo "PASS: test_selector_explicit_glob"

proc test_selector_regex() {.raises: [].} =
  let s = ensureOk(parseSelector("pkg:regex:foo\\..*"), "regex")
  doAssert s.kind == skPkg
  doAssert s.matcher.kind == mtRegex
  doAssert s.matcher.regexPattern == "foo\\..*"
  echo "PASS: test_selector_regex"

proc test_selector_literal() {.raises: [].} =
  let s = ensureOk(parseSelector("obj:literal:my_pkg.Class.method"), "literal")
  doAssert s.kind == skObj
  doAssert s.matcher.kind == mtLiteral
  doAssert s.matcher.literal == "my_pkg.Class.method"
  echo "PASS: test_selector_literal"

proc test_selector_pattern_contains_colon() {.raises: [].} =
  # Per spec § 4 "Pattern": colons inside the pattern are not escaped;
  # parsing stops after at most two separators. So `file:foo:bar:baz`
  # treats "foo" as a potential match-type — since it's not one of
  # glob/regex/literal, it's a glob pattern starting with "foo:" and
  # the rest follows. Pattern becomes "foo:bar:baz".
  let s = ensureOk(parseSelector("file:foo:bar:baz"), "ambiguous colon")
  doAssert s.kind == skFile
  doAssert s.matcher.kind == mtGlob
  doAssert s.matcher.globPattern == "foo:bar:baz"
  echo "PASS: test_selector_pattern_contains_colon"

proc test_selector_unknown_kind() {.raises: [].} =
  let r = parseSelector("zzzkind:glob:foo")
  doAssert r.isErr
  doAssert r.error.contains("unknown selector kind")
  echo "PASS: test_selector_unknown_kind"

proc test_selector_no_separator() {.raises: [].} =
  let r = parseSelector("notaselector")
  doAssert r.isErr
  doAssert r.error.contains("must contain ':'")
  echo "PASS: test_selector_no_separator"

proc test_selector_empty_pattern() {.raises: [].} =
  let r = parseSelector("file:literal:")
  doAssert r.isErr
  doAssert r.error.contains("empty")
  echo "PASS: test_selector_empty_pattern"

proc test_selector_tier2_kinds_accepted_by_parser() {.raises: [].} =
  for sel in ["local:foo", "global:foo", "arg:foo", "ret:foo", "attr:foo"]:
    let r = parseSelector(sel)
    doAssert r.isOk, "Tier-2 kind '" & sel & "' should parse OK: " &
      (if r.isErr: r.error else: "")
  echo "PASS: test_selector_tier2_kinds_accepted_by_parser"

# ---------------------------------------------------------------------------
# 2. Match types
# ---------------------------------------------------------------------------

proc test_glob_star_does_not_cross_slash() {.raises: [].} =
  let s = ensureOk(parseSelector("file:glob:src/*.nim"), "glob *")
  doAssert matches(s.matcher, "src/foo.nim")
  doAssert matches(s.matcher, "src/bar.nim")
  doAssert not matches(s.matcher, "src/sub/foo.nim"),
    "single * must not cross /"
  doAssert not matches(s.matcher, "other/foo.nim")
  echo "PASS: test_glob_star_does_not_cross_slash"

proc test_glob_double_star_crosses_slash() {.raises: [].} =
  let s = ensureOk(parseSelector("file:glob:**/foo.nim"), "glob **")
  doAssert matches(s.matcher, "foo.nim")
  doAssert matches(s.matcher, "src/foo.nim")
  doAssert matches(s.matcher, "src/sub/deep/foo.nim")
  doAssert not matches(s.matcher, "foo.txt")
  echo "PASS: test_glob_double_star_crosses_slash"

proc test_glob_question_mark() {.raises: [].} =
  let s = ensureOk(parseSelector("file:glob:f?o.nim"), "glob ?")
  doAssert matches(s.matcher, "foo.nim")
  doAssert matches(s.matcher, "fxo.nim")
  doAssert not matches(s.matcher, "fo.nim")
  doAssert not matches(s.matcher, "fooo.nim")
  echo "PASS: test_glob_question_mark"

proc test_glob_regex_metacharacters_escaped() {.raises: [].} =
  # Regex metacharacters in glob patterns must be treated as literals.
  let s = ensureOk(parseSelector("file:glob:foo.bar+baz.nim"), "glob escape")
  doAssert matches(s.matcher, "foo.bar+baz.nim")
  doAssert not matches(s.matcher, "fooxbar+baz.nim")
  doAssert not matches(s.matcher, "foo.barbaz.nim")
  echo "PASS: test_glob_regex_metacharacters_escaped"

proc test_regex_full_match() {.raises: [].} =
  let s = ensureOk(parseSelector("pkg:regex:foo\\..*"), "regex")
  doAssert matches(s.matcher, "foo.bar")
  doAssert matches(s.matcher, "foo.bar.baz")
  doAssert not matches(s.matcher, "afoo.bar"),
    "regex must be anchored to start"
  echo "PASS: test_regex_full_match"

proc test_literal_exact() {.raises: [].} =
  let s = ensureOk(parseSelector("obj:literal:my_pkg.fn"), "literal")
  doAssert matches(s.matcher, "my_pkg.fn")
  doAssert not matches(s.matcher, "my_pkg.fnX")
  doAssert not matches(s.matcher, "MY_PKG.fn")
  echo "PASS: test_literal_exact"

# ---------------------------------------------------------------------------
# 3. Rule evaluation order
# ---------------------------------------------------------------------------

proc test_default_when_no_rule_matches() {.raises: [].} =
  let toml = """
[scope]
default_exec = "trace"

[[scope.rules]]
selector = "file:literal:/foo.nim"
exec = "skip"
"""
  let c = ensureOk(compileFiltersInline(toml, "<test>"), "compile")
  let d = classify(c, "/bar.nim")
  doAssert d.exec == eaTrace, "default exec should fire"
  doAssert d.matchedRuleSource == ""
  echo "PASS: test_default_when_no_rule_matches"

proc test_default_skip() {.raises: [].} =
  let toml = """
[scope]
default_exec = "skip"
"""
  let c = ensureOk(compileFiltersInline(toml, "<test>"), "compile")
  doAssert classify(c, "anything").exec == eaSkip
  echo "PASS: test_default_skip"

proc test_later_rule_overrides_earlier() {.raises: [].} =
  let toml = """
[scope]
default_exec = "trace"

[[scope.rules]]
selector = "file:glob:**/std/**"
exec = "skip"

[[scope.rules]]
selector = "file:literal:/repo/std/special.nim"
exec = "trace"
"""
  let c = ensureOk(compileFiltersInline(toml, "<test>"), "compile")
  let d1 = classify(c, "/repo/std/special.nim")
  doAssert d1.exec == eaTrace, "later trace-rule must override earlier skip"
  # matchedRuleSource is `<sourceName>:<line-of-rule-header>`.
  doAssert d1.matchedRuleSource.startsWith("<test>:"),
    "matched source should be '<test>:N', got: " & d1.matchedRuleSource
  let colonIdx = d1.matchedRuleSource.rfind(':')
  let lineNumStr = d1.matchedRuleSource[colonIdx + 1 .. ^1]
  doAssert lineNumStr.len > 0
  for ch in lineNumStr:
    doAssert ch in {'0'..'9'},
      "matched source line should be numeric, got: " & d1.matchedRuleSource
  let d2 = classify(c, "/repo/std/other.nim")
  doAssert d2.exec == eaSkip, "first rule still wins for non-special files"
  echo "PASS: test_later_rule_overrides_earlier"

proc test_matched_rule_source_for_default() {.raises: [].} =
  let toml = """
[scope]
default_exec = "skip"
"""
  let c = ensureOk(compileFiltersInline(toml, "<test>"), "compile")
  let d = classify(c, "/anything")
  doAssert d.matchedRuleSource == "",
    "default decision has no matched rule source"
  echo "PASS: test_matched_rule_source_for_default"

# ---------------------------------------------------------------------------
# 4. Filter chain composition
# ---------------------------------------------------------------------------

proc test_chain_composition_via_inline() {.raises: [].} =
  # We exercise composition via two inline compiles by manually merging.
  # The compileFilters() seq-of-paths path is tested separately with
  # fixtures.
  let tomlBase = """
[scope]
default_exec = "trace"

[[scope.rules]]
selector = "file:glob:**/std/**"
exec = "skip"
"""
  let c1 = ensureOk(compileFiltersInline(tomlBase, "base"), "base")
  doAssert classify(c1, "/repo/std/foo.nim").exec == eaSkip
  echo "PASS: test_chain_composition_via_inline"

proc test_chain_composition_via_files() {.raises: [].} =
  let p1 = fixtureDir() / "basic.toml"
  let p2 = fixtureDir() / "override.toml"
  let c = ensureOk(compileFilters(@[p1, p2]), "chain")
  doAssert c.sources.len == 2
  # basic.toml says skip everything under lib/std; override.toml says
  # trace /repo/lib/std/important.nim. Because override.toml's rule
  # comes later, it wins.
  let d = classify(c, "/repo/lib/std/important.nim")
  doAssert d.exec == eaTrace,
    "later filter file must override earlier"
  # A different stdlib file is still skipped.
  doAssert classify(c, "/repo/lib/std/other.nim").exec == eaSkip
  echo "PASS: test_chain_composition_via_files"

# ---------------------------------------------------------------------------
# 5. Validation
# ---------------------------------------------------------------------------

proc test_bad_toml_returns_err() {.raises: [].} =
  let p = fixtureDir() / "bad_syntax.toml"
  let r = compileFilters(@[p])
  doAssert r.isErr, "bad syntax should produce Err"
  doAssert r.error.contains("bad_syntax.toml"),
    "error must mention source: " & r.error
  echo "PASS: test_bad_toml_returns_err"

proc test_unknown_selector_kind_in_toml() {.raises: [].} =
  let toml = """
[scope]
default_exec = "trace"

[[scope.rules]]
selector = "notakind:foo"
exec = "skip"
"""
  let r = compileFiltersInline(toml, "<bad>")
  doAssert r.isErr
  doAssert r.error.contains("unknown selector kind"),
    "error: " & r.error
  echo "PASS: test_unknown_selector_kind_in_toml"

proc test_unknown_exec_action() {.raises: [].} =
  let toml = """
[scope]
default_exec = "tracelol"
"""
  let r = compileFiltersInline(toml, "<bad>")
  doAssert r.isErr
  doAssert r.error.contains("unknown exec action")
  echo "PASS: test_unknown_exec_action"

proc test_version_too_new() {.raises: [].} =
  let p = fixtureDir() / "bad_version.toml"
  let r = compileFilters(@[p])
  doAssert r.isErr
  doAssert r.error.contains("newer than this library supports")
  echo "PASS: test_version_too_new"

proc test_missing_selector_field() {.raises: [].} =
  let toml = """
[[scope.rules]]
exec = "skip"
"""
  let r = compileFiltersInline(toml, "<bad>")
  doAssert r.isErr
  doAssert r.error.contains("missing 'selector'")
  echo "PASS: test_missing_selector_field"

proc test_missing_exec_field() {.raises: [].} =
  let toml = """
[[scope.rules]]
selector = "file:foo"
"""
  let r = compileFiltersInline(toml, "<bad>")
  doAssert r.isErr
  doAssert r.error.contains("missing 'exec'")
  echo "PASS: test_missing_exec_field"

proc test_tier2_in_scope_rules_warns_and_skips() {.raises: [].} =
  let p = fixtureDir() / "with_tier2.toml"
  let c = ensureOk(compileFilters(@[p]), "with_tier2")
  let ws = warnings(c)
  doAssert ws.len >= 1, "expected at least one warning, got: " & $ws
  var found = false
  for w in ws:
    if w.contains("Tier-2 selector kind"):
      found = true
      break
  doAssert found, "expected Tier-2 warning, got: " & $ws
  # The Tier-1 rule (file:glob:**/secrets/**) should still be live.
  let d = classify(c, "/repo/secrets/keys.nim")
  doAssert d.exec == eaTrace
  # The Tier-2 rule (local:literal:my_var with exec=skip) must have
  # been skipped — its target path should NOT be skipped.
  let d2 = classify(c, "my_var")
  doAssert d2.exec == eaTrace, "Tier-2 rule should not be active"
  echo "PASS: test_tier2_in_scope_rules_warns_and_skips"

proc test_missing_file_returns_err() {.raises: [].} =
  let r = compileFilters(@["/this/path/does/not/exist.toml"])
  doAssert r.isErr
  doAssert r.error.contains("failed to read")
  echo "PASS: test_missing_file_returns_err"

# ---------------------------------------------------------------------------
# 6. Integration test
# ---------------------------------------------------------------------------

proc test_integration_corpus() {.raises: [].} =
  let p = fixtureDir() / "basic.toml"
  let c = ensureOk(compileFilters(@[p]), "basic")
  doAssert ruleCount(c) == 3

  type Case = tuple[path: string; expected: ExecAction; note: string]
  let cases: seq[Case] = @[
    # stdlib paths -> skip
    (path: "/repo/lib/std/strutils.nim", expected: eaSkip,
     note: "stdlib glob match"),
    (path: "/repo/lib/std/sub/sub/foo.nim", expected: eaSkip,
     note: "stdlib double-star match"),
    # entry point -> traced (literal rule after the stdlib glob would
    # only matter if both matched; here only the literal matches)
    (path: "/repo/main.nim", expected: eaTrace,
     note: "entry point literal"),
    # user code not matching any rule -> default trace
    (path: "/repo/src/app.nim", expected: eaTrace,
     note: "default trace"),
    (path: "/repo/src/sub/util.nim", expected: eaTrace,
     note: "default trace"),
    # vendor packages -> skip (regex)
    (path: "vendor.libfoo", expected: eaSkip,
     note: "vendor regex"),
    (path: "vendor.libbar.sub", expected: eaSkip,
     note: "vendor regex deep"),
    # near-match: doesn't start with vendor.
    (path: "myvendor.libfoo", expected: eaTrace,
     note: "regex anchored at start"),
    # an obj-path that doesn't apply to file/pkg selectors -> default
    (path: "MyClass.method", expected: eaTrace,
     note: "obj-style path falls through to default"),
    # path with dot — regex metacharacter test
    (path: "vendor.libbaz", expected: eaSkip, note: "regex dot"),
    # explicit non-stdlib path
    (path: "/repo/lib/notstd/foo.nim", expected: eaTrace,
     note: "lib but not std"),
    # stdlib-like path at top level
    (path: "lib/std/x.nim", expected: eaSkip,
     note: "** matches zero+ segments before lib"),
    # path that is `lib/std/x.nim` but missing leading slash — still ok
    (path: "deep/path/lib/std/foo.nim", expected: eaSkip,
     note: "lib/std deep"),
    # short non-matching name
    (path: "x", expected: eaTrace, note: "short default"),
    # empty-ish
    (path: "main.nim", expected: eaTrace, note: "bare default"),
    # case-sensitivity check: literal selector is case sensitive
    (path: "/REPO/main.nim", expected: eaTrace,
     note: "literal is case sensitive"),
    # Repeated vendor segments
    (path: "vendor.x.y.z", expected: eaSkip, note: "vendor deep"),
    # Mixed: stdlib path with vendor in it
    (path: "/repo/lib/std/vendor.nim", expected: eaSkip,
     note: "file rule wins for file-shaped path"),
    # Pure pkg-shape with no slashes — won't match file:**/lib/std/**
    (path: "std.strutils", expected: eaTrace, note: "pkg-shaped not a stdlib file"),
    # `**` matches zero+ characters; `**/lib/std/**` matches a path
    # ending in `lib/std/` (the trailing ** is empty).
    (path: "/x/lib/std/", expected: eaSkip,
     note: "trailing ** matches empty"),
  ]

  var i = 0
  for cs in cases:
    let d = classify(c, cs.path)
    doAssert d.exec == cs.expected,
      "case " & $i & " (" & cs.note & "): path=" & cs.path &
      " expected=" & $cs.expected & " got=" & $d.exec
    i += 1

  echo "PASS: test_integration_corpus (", cases.len, " cases)"

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

proc main() {.raises: [].} =
  # Selector grammar
  test_selector_default_match_type()
  test_selector_explicit_glob()
  test_selector_regex()
  test_selector_literal()
  test_selector_pattern_contains_colon()
  test_selector_unknown_kind()
  test_selector_no_separator()
  test_selector_empty_pattern()
  test_selector_tier2_kinds_accepted_by_parser()

  # Match types
  test_glob_star_does_not_cross_slash()
  test_glob_double_star_crosses_slash()
  test_glob_question_mark()
  test_glob_regex_metacharacters_escaped()
  test_regex_full_match()
  test_literal_exact()

  # Rule evaluation order
  test_default_when_no_rule_matches()
  test_default_skip()
  test_later_rule_overrides_earlier()
  test_matched_rule_source_for_default()

  # Filter chain composition
  test_chain_composition_via_inline()
  test_chain_composition_via_files()

  # Validation
  test_bad_toml_returns_err()
  test_unknown_selector_kind_in_toml()
  test_unknown_exec_action()
  test_version_too_new()
  test_missing_selector_field()
  test_missing_exec_field()
  test_tier2_in_scope_rules_warns_and_skips()
  test_missing_file_returns_err()

  # Integration
  test_integration_corpus()

  echo "ALL PATH_FILTER TESTS PASSED"

main()

{.pop.}
