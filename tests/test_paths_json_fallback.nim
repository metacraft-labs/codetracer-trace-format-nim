{.push raises: [].}

## The reader's `paths.json` fallback, and the decoder underneath it.
##
## `paths.json` is the only JSON document `new_trace_reader` parses. It used to
## be parsed with `std/json`, which reaches `parseFloat` and so libc's
## `strtod` — a symbol a freestanding target has no definition of, which stopped
## the reader linking for `wasm32-unknown-unknown` over a float parser no `.ct`
## container ever needs. `decodeJsonStringArray` replaced it.
##
## These tests exist because that swap must not have changed what the reader
## READS. They cover the grammar a path can legitimately contain — escaped
## backslashes on a Windows path, `\/`, non-ASCII via `\uXXXX`, and characters
## outside the BMP via a surrogate pair — and the malformed documents whose
## contract is "no fallback, not a crash".

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
# The reader's use of it
# ---------------------------------------------------------------------------

proc test_the_reader_falls_back_to_paths_json() =
  let paths = pathsSeenBy("[\"/src/main.py\", \"/src/helper.py\"]")
  doAssert paths == @["/src/main.py", "/src/helper.py"], $paths
  echo "PASS: test_the_reader_falls_back_to_paths_json"

proc test_the_fallback_decodes_escapes() =
  let paths = pathsSeenBy("[\"C:\\\\Users\\\\caf\\u00e9\\\\main.py\"]")
  doAssert paths == @["C:\\Users\\caf\xC3\xA9\\main.py"], $paths
  echo "PASS: test_the_fallback_decodes_escapes"

proc test_a_malformed_paths_json_leaves_no_paths() =
  # The contract is a quiet empty fallback: the container is otherwise
  # well-formed, so opening it must still succeed.
  for text in ["[1, 2]", "{\"paths\": []}", "[\"unterminated", "not json"]:
    let r = openNewTraceFromBytes(containerWithPathsJson(text))
    doAssert r.isOk, "open failed for: " & text
    doAssert r.get().pathCount() == 0'u64, "expected no paths for: " & text
  echo "PASS: test_a_malformed_paths_json_leaves_no_paths"

proc test_an_empty_paths_json_leaves_no_paths() =
  let r = openNewTraceFromBytes(containerWithPathsJson("[]"))
  doAssert r.isOk
  doAssert r.get().pathCount() == 0'u64
  echo "PASS: test_an_empty_paths_json_leaves_no_paths"

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
  test_the_reader_falls_back_to_paths_json()
  test_the_fallback_decodes_escapes()
  test_a_malformed_paths_json_leaves_no_paths()
  test_an_empty_paths_json_leaves_no_paths()
  echo "All paths.json fallback tests passed!"

{.pop.}
