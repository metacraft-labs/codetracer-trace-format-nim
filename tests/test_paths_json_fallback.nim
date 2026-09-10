{.push raises: [].}

## The `paths.json` string-array decoder, and the retirement of the sidecar
## it used to serve.
##
## `paths.json` was the only JSON document `new_trace_reader` parsed. It used to
## be parsed with `std/json`, which reaches `parseFloat` and so libc's
## `strtod` — a symbol a freestanding target has no definition of, which stopped
## the reader linking for `wasm32-unknown-unknown` over a float parser no `.ct`
## container ever needs. `decodeJsonStringArray` replaced it.
##
## The sidecar is now retired: no reader path consults it, and a container
## carrying one is read entirely from the binary interning tables. The decoder
## survives as the only string-array parser here a freestanding target can
## link, so its grammar stays covered — escaped backslashes on a Windows path,
## `\/`, non-ASCII via `\uXXXX`, characters outside the BMP via a surrogate
## pair, and the malformed documents whose contract is "no result, not a
## crash".
##
## The last test is the retirement itself: a container whose only source of
## paths is `paths.json` now reports none, whatever that document says.

import std/options
import results
import codetracer_ctfs/container
import codetracer_trace_writer/new_trace_reader

proc containerWithPathsJson(text: string): seq[byte] =
  ## A container carrying `paths.json` and no binary paths interning table,
  ## which is exactly the shape that reaches the fallback.
  var ctfs = createCtfs()
  var f = ctfs.addFile("paths.json").get()
  doAssert ctfs.writeToFile(f, cast[seq[byte]](text)).isOk
  result = ctfs.toBytes()
  ctfs.closeCtfs()

proc pathsSeenBy(text: string): seq[string] =
  let r = openNewTraceFromBytes(containerWithPathsJson(text))
  doAssert r.isOk, "openNewTraceFromBytes failed for: " & text
  let reader = r.get()
  result = @[]
  for i in 0 ..< int(reader.pathCount()):
    let p = reader.path(uint64(i))
    doAssert p.isOk, "path(" & $i & ") failed for: " & text
    result.add(p.get())

# ---------------------------------------------------------------------------
# The decoder
# ---------------------------------------------------------------------------

proc test_decodes_a_plain_array() =
  let r = decodeJsonStringArray("[\"/src/main.py\", \"/src/helper.py\"]")
  doAssert r.isSome
  doAssert r.get() == @["/src/main.py", "/src/helper.py"]
  echo "PASS: test_decodes_a_plain_array"

proc test_decodes_an_empty_array() =
  let r = decodeJsonStringArray("[]")
  doAssert r.isSome
  doAssert r.get().len == 0
  echo "PASS: test_decodes_an_empty_array"

proc test_tolerates_whitespace_everywhere() =
  let r = decodeJsonStringArray("  [\n  \"a\" ,\t \"b\"\r\n ]  ")
  doAssert r.isSome
  doAssert r.get() == @["a", "b"]
  echo "PASS: test_tolerates_whitespace_everywhere"

proc test_decodes_the_escapes_a_path_can_contain() =
  # A Windows path arrives with its separators escaped, and a JSON writer is
  # free to escape a forward slash it did not have to.
  let r = decodeJsonStringArray("[\"C:\\\\src\\\\main.py\", \"\\/tmp\\/x\"]")
  doAssert r.isSome
  doAssert r.get() == @["C:\\src\\main.py", "/tmp/x"]
  echo "PASS: test_decodes_the_escapes_a_path_can_contain"

proc test_decodes_the_remaining_two_character_escapes() =
  let r = decodeJsonStringArray("[\"a\\\"b\", \"t\\tn\\nr\\rb\\bf\\f\"]")
  doAssert r.isSome
  doAssert r.get() == @["a\"b", "t\tn\nr\rb\bf\f"]
  echo "PASS: test_decodes_the_remaining_two_character_escapes"

proc test_decodes_a_unicode_escape_as_utf8() =
  # `é` is U+00E9, two bytes in UTF-8; `€` is U+20AC, three.
  let r = decodeJsonStringArray("[\"caf\\u00e9\", \"\\u20ac\"]")
  doAssert r.isSome
  doAssert r.get() == @["caf\xC3\xA9", "\xE2\x82\xAC"]
  echo "PASS: test_decodes_a_unicode_escape_as_utf8"

proc test_decodes_a_surrogate_pair() =
  # U+1F600, which JSON can only express as a surrogate pair.
  let r = decodeJsonStringArray("[\"\\ud83d\\ude00\"]")
  doAssert r.isSome
  doAssert r.get() == @["\xF0\x9F\x98\x80"]
  echo "PASS: test_decodes_a_surrogate_pair"

proc test_refuses_a_lone_surrogate() =
  doAssert decodeJsonStringArray("[\"\\ud83d\"]").isNone
  doAssert decodeJsonStringArray("[\"\\ude00\"]").isNone
  doAssert decodeJsonStringArray("[\"\\ud83dx\"]").isNone
  echo "PASS: test_refuses_a_lone_surrogate"

proc test_refuses_a_document_that_is_not_an_array_of_strings() =
  for text in ["", "  ", "null", "{\"a\": 1}", "[1, 2]", "[\"a\", 3]",
               "[\"a\", null]", "[[\"a\"]]", "[\"a\"",
               "[\"unterminated]", "[\"a\",]", "[,\"a\"]",
               "[\"a\"] trailing", "[\"a\"]]", "[\"\\q\"]",
               "[\"\\u00zz\"]", "[\"\\u00\"]"]:
    doAssert decodeJsonStringArray(text).isNone,
      "expected none for: " & text
  echo "PASS: test_refuses_a_document_that_is_not_an_array_of_strings"

proc test_refuses_an_unescaped_control_character() =
  doAssert decodeJsonStringArray("[\"a\nb\"]").isNone
  echo "PASS: test_refuses_an_unescaped_control_character"

# ---------------------------------------------------------------------------
# The reader no longer uses it
# ---------------------------------------------------------------------------

proc test_the_reader_no_longer_reads_paths_json() =
  # The retirement. A well-formed `paths.json` naming two real paths is the
  # document that used to produce two paths; it now produces none, because
  # nothing reads it. Opening still succeeds — a retired sidecar is ignored,
  # not rejected.
  doAssert pathsSeenBy("[\"/src/main.py\", \"/src/helper.py\"]").len == 0
  echo "PASS: test_the_reader_no_longer_reads_paths_json"

proc test_any_paths_json_leaves_no_paths() =
  # Well-formed, malformed and absent now have the same answer, which is the
  # point of retiring the sidecar: there is no document-shaped behaviour left
  # to get wrong. The container is otherwise well-formed, so it must open.
  for text in ["[\"/src/main.py\"]", "[]", "[1, 2]", "{\"paths\": []}",
               "[\"unterminated", "not json"]:
    let r = openNewTraceFromBytes(containerWithPathsJson(text))
    doAssert r.isOk, "open failed for: " & text
    doAssert r.get().pathCount() == 0'u64, "expected no paths for: " & text
  echo "PASS: test_any_paths_json_leaves_no_paths"

when isMainModule:
  test_decodes_a_plain_array()
  test_decodes_an_empty_array()
  test_tolerates_whitespace_everywhere()
  test_decodes_the_escapes_a_path_can_contain()
  test_decodes_the_remaining_two_character_escapes()
  test_decodes_a_unicode_escape_as_utf8()
  test_decodes_a_surrogate_pair()
  test_refuses_a_lone_surrogate()
  test_refuses_a_document_that_is_not_an_array_of_strings()
  test_refuses_an_unescaped_control_character()
  test_the_reader_no_longer_reads_paths_json()
  test_any_paths_json_leaves_no_paths()
  echo "All paths.json decoder tests passed!"

{.pop.}
