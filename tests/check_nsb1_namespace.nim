{.push raises: [].}

## Cross-read checker for an externally produced `NSB1` namespace image.
##
## This is the mirror image of `gen_cow_btree_crossread_fixture.nim`: that
## program writes an image here for a *foreign* reader to check, this one reads
## an image a *foreign writer* produced and checks it with the production Nim
## reader — `loadCowBTree` + `lookup` from `codetracer_ctfs/cow_btree`, with no
## test-only traversal anywhere in the path.
##
## Its first caller is `codetracer-wasm-recorder`'s Go page-CAS
## (`internal/wasmsnapshot`), whose `wcppages.ns` stream is a Leaf Type B
## namespace written straight from `CTFS-Binary-Format.md` §10. Its Go test
## `TestTheProductionNimReaderLooksUpEveryPage` shells out to this binary and
## fails if it exits non-zero, so "the Go writer emits a real namespace" is a
## claim the canonical Nim implementation adjudicates rather than a claim the
## Go side makes about itself.
##
## It is a **helper binary, not a test**: it is deliberately absent from the
## nimble `test` task and from `repro.nim`, because it needs an input file that
## only the foreign producer can make. It only has to compile and run.
##
## Usage:
##   `check_nsb1_namespace <image> <manifest>`
##
## The manifest is a line-oriented text file:
##   `<key-decimal> <descriptor-hex>`  — the key MUST resolve to exactly these
##                                       descriptor bytes
##   `!<key-decimal>`                  — the key MUST NOT be present (the
##                                       negative control, without which a
##                                       reader that found everything would
##                                       pass vacuously)
## Blank lines are ignored. Exits 0 only when every line holds.

import std/[os, strutils]
import results
import codetracer_ctfs/cow_btree

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("check_nsb1_namespace: " & msg)
  except IOError, ValueError:
    discard
  quit(1)

proc readBinary(path: string): seq[byte] {.raises: [].} =
  var data = ""
  try:
    data = readFile(path)
  except IOError, OSError:
    fail("cannot read " & path)
  result = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr result[0], addr data[0], data.len)

proc parseKey(text: string): uint64 {.raises: [].} =
  try:
    result = uint64(parseBiggestUInt(text))
  except ValueError:
    fail("not a decimal key: " & text)

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 2:
    fail("usage: check_nsb1_namespace <image> <manifest>")

  let image = readBinary(args[0])
  var manifest = ""
  try:
    manifest = readFile(args[1])
  except IOError, OSError:
    fail("cannot read manifest " & args[1])

  # The production reader, unmodified: header magic + page alignment, then the
  # highest-valid-commit-id root, then the immutable page graph.
  let loaded = loadCowBTree(image, cltTypeB)
  if loaded.isErr:
    fail("loadCowBTree(" & args[0] & "): " & loaded.error)
  let tree = loaded.value

  var resolved = 0
  var refused = 0
  for rawLine in manifest.splitLines():
    let line = rawLine.strip()
    if line.len == 0:
      continue

    if line.startsWith("!"):
      let key = parseKey(line[1 .. ^1])
      let found = tree.lookup(key)
      if found.isOk:
        fail("key " & $key & " must be absent but the namespace resolved it")
      refused += 1
      continue

    let parts = line.splitWhitespace()
    if parts.len != 2:
      fail("bad manifest line: " & line)
    let key = parseKey(parts[0])
    let want = parts[1].toLowerAscii()
    let found = tree.lookup(key)
    if found.isErr:
      fail("key " & $key & " was not found: " & found.error)
    var got = ""
    for b in found.value:
      got.add(toHex(int(b), 2))
    got = got.toLowerAscii()
    if got != want:
      fail("key " & $key & " resolved to descriptor " & got & ", expected " & want)
    resolved += 1

  if resolved == 0:
    fail("the manifest asked for no lookups; a vacuous pass is not a proof")
  echo "check_nsb1_namespace: OK — ", args[0], " (", image.len, " bytes, ",
    image.len div PageSize, " pages): ", resolved,
    " key(s) resolved through the production loadCowBTree/lookup, ",
    refused, " absent key(s) correctly refused"

main()
