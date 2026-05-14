{.push raises: [].}

## Path filter classifier — Tier 1 (scope-level) trace filtering.
##
## This module implements the cross-language trace-filter contract
## defined in
## `codetracer-trace-format-spec/Trace-Filters.md` (spec SHA 6a65524).
## Only Tier 1 (scope-level: `pkg`, `file`, `obj` selectors with
## `trace` / `skip` actions) is implemented here. Tier 2 (value-level
## redaction) is deferred to TF-M3b.
##
## Public API (see spec § 10):
##
## ```nim
## proc compileFilters*(paths: seq[string]): Result[Classifier, string]
## proc compileFiltersInline*(toml: string;
##                            sourceName: string): Result[Classifier, string]
## proc classify*(c: Classifier; path: string): Decision
## ```
##
## Hot-path note (spec § 6): this module performs filter classification at
## scope-registration time. Recorders MUST cache the resulting `Decision`
## in their host runtime's native per-scope metadata slot to avoid
## per-event hash lookups.
##
## TOML parser: a small custom parser implementing just the subset of TOML
## the trace-filter schema needs (header tables, array-of-tables, string /
## integer / boolean / array-of-string scalars, comments). Nim's
## `std/parsecfg` is INI-style and does not understand the
## `[[scope.rules]]` array-of-tables syntax used by the schema, and
## pulling in a full TOML library would add a heavy dep for ~150 lines of
## parsing. The subset we parse is exactly the schema declared in
## `Trace-Filters.md` § 4.
##
## Regex flavor: Nim's `std/re` is PCRE-equivalent. Patterns are anchored
## (full-match) per spec § 4.

import std/[re, strutils, options]
when defined(nimPreviewSlimSystem):
  import std/syncio
import results

export results

type
  SelectorKind* = enum
    ## Selector kinds. Tier-1 kinds are `pkg` / `file` / `obj`.
    ## Tier-2 kinds are accepted by the parser for cross-recorder
    ## portability but ignored by the Tier-1 classifier.
    skPkg = "pkg"
    skFile = "file"
    skObj = "obj"
    skLocal = "local"
    skGlobal = "global"
    skArg = "arg"
    skRet = "ret"
    skAttr = "attr"

  MatchType* = enum
    mtGlob = "glob"
    mtRegex = "regex"
    mtLiteral = "literal"

  ExecAction* = enum
    eaTrace = "trace"
    eaSkip = "skip"

  ValueAction* = enum
    ## Tier-2 value action. Defined here so the parser can accept
    ## value-pattern blocks in shared filter files; the Tier-1
    ## classifier does not consult these.
    vaAllow = "allow"
    vaRedact = "redact"
    vaDrop = "drop"

  CompiledMatcher* = object
    case kind*: MatchType
    of mtGlob:
      glob*: Regex      ## glob translated to anchored regex
      globPattern*: string
    of mtRegex:
      regex*: Regex
      regexPattern*: string
    of mtLiteral:
      literal*: string

  Selector* = object
    kind*: SelectorKind
    matcher*: CompiledMatcher

  ScopeRule* = object
    selector*: Selector
    exec*: ExecAction
    reason*: string
    # Tier-2 fields parsed but unused by the Tier-1 classifier.
    valueDefault*: Option[ValueAction]
    # For matchedRuleSource diagnostic:
    sourceFile*: string
    sourceLine*: int

  Decision* = object
    exec*: ExecAction
    matchedRuleSource*: string

  Classifier* = object
    defaultExec*: ExecAction
    rules*: seq[ScopeRule]
    warnings*: seq[string]
    sources*: seq[string]
      ## File paths (or sentinel sourceName values) that contributed
      ## rules, in composition order.

# ---------------------------------------------------------------------------
# Glob -> regex translation
# ---------------------------------------------------------------------------

proc globToRegex(pattern: string): string {.raises: [].} =
  ## Translate a shell-style glob into an anchored regex string.
  ##
  ## Semantics (matches spec § 4 "Match types" row for `glob`):
  ##   `*`   matches any character except `/`
  ##   `**`  matches any character including `/`
  ##   `?`   matches a single character except `/`
  ##   `[..]` character class (passed through to regex)
  ##   other regex metacharacters are escaped.
  var s = "^"
  var i = 0
  while i < pattern.len:
    let c = pattern[i]
    case c
    of '*':
      if i + 1 < pattern.len and pattern[i + 1] == '*':
        # `**/` matches zero or more path segments (including the empty
        # prefix, so `**/foo.nim` matches both `foo.nim` and
        # `a/b/foo.nim`). A bare `**` (no following slash) matches any
        # characters including `/`.
        if i + 2 < pattern.len and pattern[i + 2] == '/':
          s.add("(?:.*/)?")
          i += 3
        else:
          s.add(".*")
          i += 2
      else:
        s.add("[^/]*")
        i += 1
    of '?':
      s.add("[^/]")
      i += 1
    of '[':
      # Pass through character class verbatim; find the matching `]`.
      var j = i + 1
      if j < pattern.len and (pattern[j] == '!' or pattern[j] == '^'):
        j += 1
      if j < pattern.len and pattern[j] == ']':
        j += 1
      while j < pattern.len and pattern[j] != ']':
        j += 1
      if j < pattern.len:
        # Translate POSIX-ish `[!...]` -> `[^...]`
        if pattern[i + 1] == '!':
          s.add('[')
          s.add('^')
          for k in i + 2 ..< j:
            s.add(pattern[k])
          s.add(']')
        else:
          for k in i .. j:
            s.add(pattern[k])
        i = j + 1
      else:
        # No closing bracket — treat as literal.
        s.add("\\[")
        i += 1
    of '.', '+', '(', ')', '{', '}', '|', '^', '$', '\\':
      s.add('\\')
      s.add(c)
      i += 1
    else:
      s.add(c)
      i += 1
  s.add('$')
  return s

proc compileGlob(pattern: string): Result[CompiledMatcher, string] {.raises: [].} =
  let rxSrc = globToRegex(pattern)
  var rx: Regex
  try:
    rx = re(rxSrc)
  except RegexError as e:
    return err("invalid glob pattern '" & pattern & "': " & e.msg)
  except Exception as e:
    return err("invalid glob pattern '" & pattern & "': " & e.msg)
  ok(CompiledMatcher(kind: mtGlob, glob: rx, globPattern: pattern))

proc compileRegexMatcher(pattern: string): Result[CompiledMatcher, string] {.raises: [].} =
  var rx: Regex
  try:
    rx = re(pattern)
  except RegexError as e:
    return err("invalid regex pattern '" & pattern & "': " & e.msg)
  except Exception as e:
    return err("invalid regex pattern '" & pattern & "': " & e.msg)
  ok(CompiledMatcher(kind: mtRegex, regex: rx, regexPattern: pattern))

# ---------------------------------------------------------------------------
# Selector parsing
# ---------------------------------------------------------------------------

proc parseSelectorKind(s: string): Result[SelectorKind, string] {.raises: [].} =
  case s
  of "pkg": ok(skPkg)
  of "file": ok(skFile)
  of "obj": ok(skObj)
  of "local": ok(skLocal)
  of "global": ok(skGlobal)
  of "arg": ok(skArg)
  of "ret": ok(skRet)
  of "attr": ok(skAttr)
  else: err("unknown selector kind '" & s & "'")

proc parseMatchType(s: string): Result[MatchType, string] {.raises: [].} =
  case s
  of "glob": ok(mtGlob)
  of "regex": ok(mtRegex)
  of "literal": ok(mtLiteral)
  else: err("unknown match type '" & s & "'")

proc parseSelector*(raw: string): Result[Selector, string] {.raises: [].} =
  ## Parse a selector string of the form
  ## `<kind>:[<match_type>:]<pattern>`. Default match type is `glob`.
  ##
  ## Parsing stops after at most two colon separators; the rest is the
  ## pattern (colons inside the pattern are not escaped, per spec § 4
  ## "Pattern").
  let firstColon = raw.find(':')
  if firstColon < 0:
    return err("selector must contain ':' separator: '" & raw & "'")
  let kindStr = raw[0 ..< firstColon]
  let kind = ? parseSelectorKind(kindStr)

  let rest = raw[firstColon + 1 .. ^1]
  let secondColon = rest.find(':')
  var matchType = mtGlob
  var pattern = rest
  if secondColon >= 0:
    let possibleMt = rest[0 ..< secondColon]
    case possibleMt
    of "glob", "regex", "literal":
      matchType = ? parseMatchType(possibleMt)
      pattern = rest[secondColon + 1 .. ^1]
    else:
      # Not a recognized match type — treat the whole remainder as the
      # glob pattern. This is conservative: a selector like
      # `file:foo:bar.txt` is interpreted as `file:glob:foo:bar.txt`
      # (the pattern keeps the colon). Per spec § 4 we stop after at
      # most two separators, but a colon that is not one of the three
      # named match types is part of the pattern.
      discard

  if pattern.len == 0:
    return err("selector pattern is empty: '" & raw & "'")

  var matcher: CompiledMatcher
  case matchType
  of mtGlob:
    matcher = ? compileGlob(pattern)
  of mtRegex:
    matcher = ? compileRegexMatcher(pattern)
  of mtLiteral:
    matcher = CompiledMatcher(kind: mtLiteral, literal: pattern)

  ok(Selector(kind: kind, matcher: matcher))

# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

proc matches*(m: CompiledMatcher; path: string): bool {.raises: [].} =
  case m.kind
  of mtLiteral:
    return path == m.literal
  of mtGlob:
    try:
      return match(path, m.glob)
    except Exception:
      return false
  of mtRegex:
    try:
      return match(path, m.regex)
    except Exception:
      return false

# ---------------------------------------------------------------------------
# Minimal TOML subset parser
# ---------------------------------------------------------------------------
#
# Supports the subset of TOML needed for the trace-filter schema:
#   - Comments (`# ...` to end of line)
#   - Table headers:           [meta]   [scope]
#   - Array-of-tables headers: [[scope.rules]]  [[scope.rules.value_patterns]]
#   - Key = value pairs where value is one of:
#       * basic string ("..." with \n \r \t \" \\ \uXXXX escapes)
#       * literal string ('...')
#       * integer
#       * boolean (true / false)
#       * inline array of strings: ["a", "b"]
#
# This is enough to load any conformant filter file per spec § 4. Numeric
# floats, datetimes, nested inline tables, multi-line strings, and
# heterogeneous arrays are not supported and produce a clear error.

type
  TomlValueKind = enum
    tvString
    tvInt
    tvBool
    tvStringArray

  TomlValue = object
    line: int
    case kind: TomlValueKind
    of tvString:
      strVal: string
    of tvInt:
      intVal: int64
    of tvBool:
      boolVal: bool
    of tvStringArray:
      arrVal: seq[string]

  TomlTable = object
    name: string             # dotted name e.g. "scope" or "scope.rules"
    isArrayElem: bool        # true if produced by [[...]]
    line: int                # source line where the header appeared
    keys: seq[(string, TomlValue)]

  TomlDoc = object
    tables: seq[TomlTable]
      ## Tables in source order. A `[[name]]` header produces a new entry
      ## with `isArrayElem = true`. A `[name]` header produces a new entry
      ## with `isArrayElem = false`.

  TomlError = object
    line: int
    msg: string

proc isWhitespace(c: char): bool {.raises: [].} =
  c == ' ' or c == '\t'

proc skipWsInline(s: string; pos: var int) {.raises: [].} =
  while pos < s.len and isWhitespace(s[pos]):
    pos += 1

proc parseBasicString(s: string; pos: var int; line: int;
                      err: var TomlError): Option[string] {.raises: [].} =
  # Expects pos to be at the opening '"'.
  if pos >= s.len or s[pos] != '"':
    err = TomlError(line: line, msg: "expected '\"' to start string")
    return none(string)
  pos += 1
  var buf = ""
  while pos < s.len:
    let c = s[pos]
    if c == '"':
      pos += 1
      return some(buf)
    elif c == '\\':
      pos += 1
      if pos >= s.len:
        err = TomlError(line: line, msg: "unterminated escape in string")
        return none(string)
      let e = s[pos]
      case e
      of '"': buf.add('"')
      of '\\': buf.add('\\')
      of '/': buf.add('/')
      of 'b': buf.add('\x08')
      of 'f': buf.add('\x0C')
      of 'n': buf.add('\n')
      of 'r': buf.add('\r')
      of 't': buf.add('\t')
      of 'u':
        if pos + 4 >= s.len:
          err = TomlError(line: line, msg: "truncated \\u escape")
          return none(string)
        var code: int = 0
        for k in 1 .. 4:
          let h = s[pos + k]
          var v: int
          case h
          of '0' .. '9': v = ord(h) - ord('0')
          of 'a' .. 'f': v = ord(h) - ord('a') + 10
          of 'A' .. 'F': v = ord(h) - ord('A') + 10
          else:
            err = TomlError(line: line, msg: "invalid hex in \\u escape")
            return none(string)
          code = code * 16 + v
        # Emit as UTF-8.
        if code < 0x80:
          buf.add(chr(code))
        elif code < 0x800:
          buf.add(chr(0xC0 or (code shr 6)))
          buf.add(chr(0x80 or (code and 0x3F)))
        else:
          buf.add(chr(0xE0 or (code shr 12)))
          buf.add(chr(0x80 or ((code shr 6) and 0x3F)))
          buf.add(chr(0x80 or (code and 0x3F)))
        pos += 4
      else:
        err = TomlError(line: line, msg: "unsupported escape \\" & $e)
        return none(string)
      pos += 1
    elif c == '\n' or c == '\r':
      err = TomlError(line: line, msg: "unterminated string (newline)")
      return none(string)
    else:
      buf.add(c)
      pos += 1
  err = TomlError(line: line, msg: "unterminated string")
  return none(string)

proc parseLiteralString(s: string; pos: var int; line: int;
                       err: var TomlError): Option[string] {.raises: [].} =
  # Expects pos at the opening single quote.
  if pos >= s.len or s[pos] != '\'':
    err = TomlError(line: line, msg: "expected ''' to start literal string")
    return none(string)
  pos += 1
  let start = pos
  while pos < s.len and s[pos] != '\'' and s[pos] != '\n' and s[pos] != '\r':
    pos += 1
  if pos >= s.len or s[pos] != '\'':
    err = TomlError(line: line, msg: "unterminated literal string")
    return none(string)
  let res = s[start ..< pos]
  pos += 1
  return some(res)

proc parseStringArray(s: string; pos: var int; line: int;
                      err: var TomlError): Option[seq[string]] {.raises: [].} =
  # Expects pos at '['. Single-line only.
  if pos >= s.len or s[pos] != '[':
    err = TomlError(line: line, msg: "expected '['")
    return none(seq[string])
  pos += 1
  var buf: seq[string] = @[]
  skipWsInline(s, pos)
  if pos < s.len and s[pos] == ']':
    pos += 1
    return some(buf)
  while pos < s.len:
    skipWsInline(s, pos)
    if pos >= s.len:
      err = TomlError(line: line, msg: "unterminated array")
      return none(seq[string])
    let c = s[pos]
    var elem: Option[string]
    if c == '"':
      elem = parseBasicString(s, pos, line, err)
    elif c == '\'':
      elem = parseLiteralString(s, pos, line, err)
    else:
      err = TomlError(line: line, msg: "array element must be a string")
      return none(seq[string])
    if elem.isNone:
      return none(seq[string])
    buf.add(elem.get)
    skipWsInline(s, pos)
    if pos < s.len and s[pos] == ',':
      pos += 1
      skipWsInline(s, pos)
      # allow trailing comma
      if pos < s.len and s[pos] == ']':
        pos += 1
        return some(buf)
      continue
    if pos < s.len and s[pos] == ']':
      pos += 1
      return some(buf)
    err = TomlError(line: line, msg: "expected ',' or ']' in array")
    return none(seq[string])
  err = TomlError(line: line, msg: "unterminated array")
  return none(seq[string])

proc parseValue(s: string; pos: var int; line: int;
                err: var TomlError): Option[TomlValue] {.raises: [].} =
  skipWsInline(s, pos)
  if pos >= s.len:
    err = TomlError(line: line, msg: "expected value")
    return none(TomlValue)
  let c = s[pos]
  if c == '"':
    let opt = parseBasicString(s, pos, line, err)
    if opt.isNone: return none(TomlValue)
    return some(TomlValue(kind: tvString, strVal: opt.get, line: line))
  elif c == '\'':
    let opt = parseLiteralString(s, pos, line, err)
    if opt.isNone: return none(TomlValue)
    return some(TomlValue(kind: tvString, strVal: opt.get, line: line))
  elif c == '[':
    let opt = parseStringArray(s, pos, line, err)
    if opt.isNone: return none(TomlValue)
    return some(TomlValue(kind: tvStringArray, arrVal: opt.get, line: line))
  elif c == 't' or c == 'f':
    if pos + 4 <= s.len and s[pos ..< pos + 4] == "true":
      pos += 4
      return some(TomlValue(kind: tvBool, boolVal: true, line: line))
    if pos + 5 <= s.len and s[pos ..< pos + 5] == "false":
      pos += 5
      return some(TomlValue(kind: tvBool, boolVal: false, line: line))
    err = TomlError(line: line, msg: "expected 'true' or 'false'")
    return none(TomlValue)
  elif c == '-' or c == '+' or (c >= '0' and c <= '9'):
    let start = pos
    if c == '-' or c == '+':
      pos += 1
    var hasDigit = false
    while pos < s.len and s[pos] >= '0' and s[pos] <= '9':
      pos += 1
      hasDigit = true
    if not hasDigit:
      err = TomlError(line: line, msg: "expected digit in number")
      return none(TomlValue)
    if pos < s.len and (s[pos] == '.' or s[pos] == 'e' or s[pos] == 'E'):
      err = TomlError(line: line, msg: "floating-point values are not supported")
      return none(TomlValue)
    let numStr = s[start ..< pos]
    var iv: int64
    try:
      iv = parseBiggestInt(numStr)
    except ValueError:
      err = TomlError(line: line, msg: "invalid integer '" & numStr & "'")
      return none(TomlValue)
    return some(TomlValue(kind: tvInt, intVal: iv, line: line))
  else:
    err = TomlError(line: line, msg: "unrecognized value start: '" & $c & "'")
    return none(TomlValue)

proc parseBareKey(s: string; pos: var int): string {.raises: [].} =
  var key = ""
  while pos < s.len:
    let c = s[pos]
    if (c >= 'A' and c <= 'Z') or (c >= 'a' and c <= 'z') or
       (c >= '0' and c <= '9') or c == '_' or c == '-':
      key.add(c)
      pos += 1
    else:
      break
  return key

proc parseDottedKey(s: string; pos: var int): string {.raises: [].} =
  # Parse dotted bare key (no quoted segments needed for the filter
  # schema's table headers).
  var key = ""
  while pos < s.len:
    skipWsInline(s, pos)
    let seg = parseBareKey(s, pos)
    if seg.len == 0:
      break
    if key.len > 0:
      key.add('.')
    key.add(seg)
    skipWsInline(s, pos)
    if pos < s.len and s[pos] == '.':
      pos += 1
      continue
    else:
      break
  return key

proc parseToml(input: string; sourceName: string;
               doc: var TomlDoc; err: var TomlError): bool {.raises: [].} =
  ## Parse the input. Returns true on success; on failure populates `err`.
  doc = TomlDoc()
  # Implicit root table for [meta] / [scope] top-level keys before any
  # header. We model "before any header" as a synthetic root table with
  # name "" — but the schema doesn't put pairs at the very top, so we
  # simply allow it and treat it as table "".
  var currentTableIdx = -1
  var lineNum = 0
  var pos = 0
  while pos < input.len:
    lineNum += 1
    # Slice out the current line (without the newline).
    let lineStart = pos
    while pos < input.len and input[pos] != '\n':
      pos += 1
    let lineEnd = pos
    if pos < input.len and input[pos] == '\n':
      pos += 1

    var lp = lineStart
    skipWsInline(input, lp)
    if lp >= lineEnd:
      continue
    if input[lp] == '#':
      continue
    # Strip trailing CR
    var endIdx = lineEnd
    if endIdx > lineStart and input[endIdx - 1] == '\r':
      endIdx -= 1

    # Use a substring view for clarity.
    let line = input[lineStart ..< endIdx]
    var lineP = lp - lineStart

    # Header [..] or [[..]]
    if line[lineP] == '[':
      var isArrayHeader = false
      lineP += 1
      if lineP < line.len and line[lineP] == '[':
        isArrayHeader = true
        lineP += 1
      skipWsInline(line, lineP)
      let name = parseDottedKey(line, lineP)
      if name.len == 0:
        err = TomlError(line: lineNum, msg: "empty table header")
        return false
      skipWsInline(line, lineP)
      if isArrayHeader:
        if lineP + 1 < line.len and line[lineP] == ']' and line[lineP + 1] == ']':
          lineP += 2
        else:
          err = TomlError(line: lineNum, msg: "expected ']]' to close array-of-tables header")
          return false
      else:
        if lineP < line.len and line[lineP] == ']':
          lineP += 1
        else:
          err = TomlError(line: lineNum, msg: "expected ']' to close table header")
          return false
      skipWsInline(line, lineP)
      if lineP < line.len and line[lineP] != '#':
        err = TomlError(line: lineNum, msg: "unexpected text after table header")
        return false
      doc.tables.add(TomlTable(name: name, isArrayElem: isArrayHeader, line: lineNum))
      currentTableIdx = doc.tables.len - 1
      continue

    # Key = value
    let key = parseBareKey(line, lineP)
    if key.len == 0:
      err = TomlError(line: lineNum, msg: "expected key or table header")
      return false
    skipWsInline(line, lineP)
    if lineP >= line.len or line[lineP] != '=':
      err = TomlError(line: lineNum, msg: "expected '=' after key '" & key & "'")
      return false
    lineP += 1
    skipWsInline(line, lineP)
    var localErr: TomlError
    let v = parseValue(line, lineP, lineNum, localErr)
    if v.isNone:
      err = localErr
      return false
    skipWsInline(line, lineP)
    if lineP < line.len and line[lineP] != '#':
      err = TomlError(line: lineNum, msg: "unexpected text after value")
      return false

    if currentTableIdx < 0:
      doc.tables.add(TomlTable(name: "", isArrayElem: false, line: lineNum))
      currentTableIdx = doc.tables.len - 1
    doc.tables[currentTableIdx].keys.add((key, v.get))

  discard sourceName  # currently unused; reserved for future diagnostics
  return true

# ---------------------------------------------------------------------------
# Schema interpretation
# ---------------------------------------------------------------------------

proc lookupKey(t: TomlTable; key: string): Option[TomlValue] {.raises: [].} =
  for (k, v) in t.keys:
    if k == key:
      return some(v)
  return none(TomlValue)

proc parseExec(v: TomlValue): Result[ExecAction, string] {.raises: [].} =
  if v.kind != tvString:
    return err("exec/default_exec must be a string")
  case v.strVal
  of "trace": ok(eaTrace)
  of "skip": ok(eaSkip)
  else: err("unknown exec action '" & v.strVal & "'")

proc parseValueActionFromStr(s: string): Result[ValueAction, string] {.raises: [].} =
  case s
  of "allow": ok(vaAllow)
  of "redact": ok(vaRedact)
  of "drop": ok(vaDrop)
  else: err("unknown value action '" & s & "'")

proc compileScopeRule(t: TomlTable; sourceName: string):
    Result[ScopeRule, string] {.raises: [].} =
  let selOpt = lookupKey(t, "selector")
  if selOpt.isNone:
    return err(sourceName & ":" & $t.line &
               ": [[scope.rules]] entry missing 'selector' field")
  if selOpt.get.kind != tvString:
    return err(sourceName & ":" & $t.line & ": 'selector' must be a string")

  let selector = ? parseSelector(selOpt.get.strVal)

  let execOpt = lookupKey(t, "exec")
  if execOpt.isNone:
    return err(sourceName & ":" & $t.line &
               ": [[scope.rules]] entry missing 'exec' field")
  let exec = ? parseExec(execOpt.get)

  var reason = ""
  let reasonOpt = lookupKey(t, "reason")
  if reasonOpt.isSome:
    if reasonOpt.get.kind != tvString:
      return err(sourceName & ":" & $t.line & ": 'reason' must be a string")
    reason = reasonOpt.get.strVal

  var valueDefault: Option[ValueAction] = none(ValueAction)
  let vdOpt = lookupKey(t, "value_default")
  if vdOpt.isSome:
    if vdOpt.get.kind != tvString:
      return err(sourceName & ":" & $t.line & ": 'value_default' must be a string")
    let va = ? parseValueActionFromStr(vdOpt.get.strVal)
    valueDefault = some(va)

  ok(ScopeRule(
    selector: selector,
    exec: exec,
    reason: reason,
    valueDefault: valueDefault,
    sourceFile: sourceName,
    sourceLine: t.line,
  ))

proc isTier2Kind(k: SelectorKind): bool {.raises: [].} =
  case k
  of skLocal, skGlobal, skArg, skRet, skAttr: true
  else: false

proc loadDoc(c: var Classifier; doc: TomlDoc; sourceName: string):
    Result[void, string] {.raises: [].} =
  ## Apply a parsed TOML doc to the classifier in source order.
  # Tables we recognize:
  #   [meta]                       (only `version` is enforced)
  #   [scope]                      default_exec / default_value_action
  #   [[scope.rules]]              a scope rule
  #   [[scope.rules.value_patterns]]  ignored (Tier 2)
  for t in doc.tables:
    case t.name
    of "meta":
      let vOpt = lookupKey(t, "version")
      if vOpt.isSome:
        if vOpt.get.kind != tvInt:
          return err(sourceName & ":" & $vOpt.get.line &
                     ": [meta] version must be an integer")
        if vOpt.get.intVal > 1:
          return err(sourceName & ":" & $vOpt.get.line &
                     ": filter schema version " & $vOpt.get.intVal &
                     " is newer than this library supports (1)")
        if vOpt.get.intVal < 1:
          return err(sourceName & ":" & $vOpt.get.line &
                     ": [meta] version must be >= 1")
    of "scope":
      let deOpt = lookupKey(t, "default_exec")
      if deOpt.isSome:
        c.defaultExec = ? parseExec(deOpt.get)
      let dvaOpt = lookupKey(t, "default_value_action")
      if dvaOpt.isSome:
        if dvaOpt.get.kind != tvString:
          return err(sourceName & ":" & $dvaOpt.get.line &
                     ": 'default_value_action' must be a string")
        discard ? parseValueActionFromStr(dvaOpt.get.strVal)
        # Value-default itself is Tier-2 state. Tier-1 ignores it but we
        # validate the value here so a malformed default is caught early.
    of "scope.rules":
      if not t.isArrayElem:
        return err(sourceName & ":" & $t.line &
                   ": expected [[scope.rules]] (array-of-tables)")
      # Peek at the selector to decide whether to keep or warn-skip.
      let selOpt = lookupKey(t, "selector")
      if selOpt.isSome and selOpt.get.kind == tvString:
        let rawSel = selOpt.get.strVal
        let firstColon = rawSel.find(':')
        if firstColon > 0:
          let kindRes = parseSelectorKind(rawSel[0 ..< firstColon])
          if kindRes.isOk and isTier2Kind(kindRes.get):
            c.warnings.add(sourceName & ":" & $t.line &
                           ": Tier-2 selector kind '" &
                           rawSel[0 ..< firstColon] &
                           "' in [[scope.rules]] is ignored by Tier-1 classifier")
            continue
      let rule = ? compileScopeRule(t, sourceName)
      c.rules.add(rule)
    of "scope.rules.value_patterns":
      # Tier-2 value patterns. Accepted for cross-recorder file
      # portability, but the Tier-1 classifier doesn't visit them.
      if not t.isArrayElem:
        return err(sourceName & ":" & $t.line &
                   ": expected [[scope.rules.value_patterns]] (array-of-tables)")
      # No validation of value-pattern selectors here — recorders that
      # consume Tier-2 patterns own that. Validating now would force
      # callers without value-pattern intent to keep them syntactically
      # consistent, which is fine but not the responsibility of Tier 1.
      discard
    of "":
      # Top-level pairs (none expected by the schema). Ignore silently
      # rather than fail — keeps the parser permissive for now.
      discard
    else:
      # Unknown top-level table. Warn-and-skip rather than fail: a future
      # Tier-N filter file may carry new sections we don't recognize.
      c.warnings.add(sourceName & ":" & $t.line &
                     ": unknown table '[" & t.name & "]' ignored")
  ok()

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc compileFiltersInline*(toml: string; sourceName: string):
    Result[Classifier, string] {.raises: [].} =
  ## Compile a filter from an inline TOML string. `sourceName` is used
  ## in diagnostic messages and recorded as the contributing source.
  var c = Classifier(defaultExec: eaTrace)
  c.sources.add(sourceName)
  var doc: TomlDoc
  var perr: TomlError
  if not parseToml(toml, sourceName, doc, perr):
    return err(sourceName & ":" & $perr.line & ": " & perr.msg)
  let res = loadDoc(c, doc, sourceName)
  if res.isErr:
    return err(res.error)
  ok(c)

proc compileFilters*(paths: seq[string]): Result[Classifier, string] {.raises: [].} =
  ## Compile a list of TOML filter files in composition order. Later
  ## files override earlier ones per spec § 5.
  var c = Classifier(defaultExec: eaTrace)
  for path in paths:
    c.sources.add(path)
    var content = ""
    try:
      content = readFile(path)
    except IOError as e:
      return err("failed to read filter file '" & path & "': " & e.msg)
    except OSError as e:
      return err("failed to read filter file '" & path & "': " & e.msg)
    except Exception as e:
      return err("failed to read filter file '" & path & "': " & e.msg)
    var doc: TomlDoc
    var perr: TomlError
    if not parseToml(content, path, doc, perr):
      return err(path & ":" & $perr.line & ": " & perr.msg)
    let res = loadDoc(c, doc, path)
    if res.isErr:
      return err(res.error)
  ok(c)

proc matchesScope(sel: Selector; path: string): bool {.raises: [].} =
  ## Tier-1 selector match. The library does not interpret `pkg` vs
  ## `file` vs `obj` differently — the recorder is responsible for
  ## passing the correct scope identifier for the selector kind it
  ## wants to match. (See spec § 2 non-goal "Cross-recorder selector
  ## portability".)
  if isTier2Kind(sel.kind):
    return false
  matches(sel.matcher, path)

proc classify*(c: Classifier; path: string): Decision {.raises: [].} =
  ## Classify a scope path. Walks rules in source order; later matches
  ## override earlier; the final decision is the last-matched rule's
  ## action, or `defaultExec` if nothing matched.
  var exec = c.defaultExec
  var matchedSrc = ""
  for r in c.rules:
    if matchesScope(r.selector, path):
      exec = r.exec
      matchedSrc = r.sourceFile & ":" & $r.sourceLine
  Decision(exec: exec, matchedRuleSource: matchedSrc)

# ---------------------------------------------------------------------------
# Convenience accessors
# ---------------------------------------------------------------------------

proc warnings*(c: Classifier): seq[string] {.raises: [].} =
  ## Warnings accumulated during compilation (e.g. Tier-2 rules in
  ## `[[scope.rules]]` that were skipped). The order matches the order
  ## of rules they pertain to.
  c.warnings

proc sources*(c: Classifier): seq[string] {.raises: [].} =
  ## Sources (file paths or sourceName sentinels) that contributed to
  ## this classifier, in composition order.
  c.sources

proc ruleCount*(c: Classifier): int {.raises: [].} =
  c.rules.len

{.pop.}
