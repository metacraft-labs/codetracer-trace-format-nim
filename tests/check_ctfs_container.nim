{.push raises: [].}

## Cross-read checker for an externally produced (or externally *modified*)
## CTFS container.
##
## Same shape as `check_nsb1_namespace.nim`: a foreign producer writes a
## container, this program reads it with the production Nim reader —
## `container.nim`'s `readInternalFile` / `hasInternalFile`, no test-only
## traversal anywhere in the path — and exits non-zero if anything disagrees.
##
## Its first caller is `codetracer-wasm-recorder`'s
## `internal/ctfs/ffi_crossread_test.go`, which builds a container through
## `ct_container_create` / `ct_container_append_files` and then has this
## program adjudicate it. That matters most for internal files past ~511 data
## blocks: the multi-level mapping of `CTFS-Binary-Format.md` §4 is where a
## second container writer has silently gone wrong before, and a container
## that only its own writer can read is exactly the failure the FFI entry
## point exists to prevent.
##
## `readInternalFile` is a genuinely separate transcription of §4's walk — it
## does not call `lookupDataBlock` — so even a Nim-written container being
## read here is two implementations meeting, not one round trip.
##
## It is a **helper binary, not a test**: it is deliberately absent from the
## nimble `test` task and from `repro.nim`, because it needs an input file
## that only the foreign producer can make. It only has to compile and run.
##
## Usage:
##   `check_ctfs_container <container.ct> <manifest>`
##
## The manifest is a line-oriented text file:
##   `<internal-name> <path>`  — the internal file MUST exist and its bytes
##                               MUST equal the contents of `<path>`
##   `!<internal-name>`        — the internal file MUST NOT be present (the
##                               negative control, without which a reader that
##                               found everything would pass vacuously)
## Blank lines and `#` comments are ignored. Exits 0 only when every line
## holds, printing `check_ctfs_container: OK`.
##
## ## What it accepts, and why that is not laxity
##
## Per `CTFS-Binary-Format.md` §5d a reader must accept a container whose
## length is not a whole number of blocks (what a crash inside an append's tail
## write leaves) and ignore the partial region. Since this program is the
## *adjudicating* reader for other implementations' containers, refusing that
## state would make it disagree with every conforming reader that defers to it.
## It therefore accepts and says so on stderr; genuine corruption is caught
## where it can actually be told apart — `readInternalFile` refuses a stream
## whose mapping root, mapping block or **data block** falls outside the
## container's whole blocks, so a truncated container loses exactly the streams
## that were truncated and keeps the rest byte-exact.

import std/[os, strutils]
import results
import codetracer_ctfs

proc fail(msg: string) {.raises: [].} =
  try:
    stderr.writeLine("check_ctfs_container: " & msg)
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

proc firstDifference(a, b: openArray[byte]): int {.raises: [].} =
  let n = min(a.len, b.len)
  for i in 0 ..< n:
    if a[i] != b[i]:
      return i
  if a.len != b.len: n else: -1

proc main() {.raises: [].} =
  let args = commandLineParams()
  if args.len < 2:
    fail("usage: check_ctfs_container <container.ct> <manifest>")

  let raw = readBinary(args[0])
  if not hasCtfsMagic(raw):
    fail(args[0] & " does not carry the CTFS magic")
  if not hasValidVersion(raw):
    fail(args[0] & " has an unrecognised CTFS version byte " & $raw[5])

  # Read the geometry out of the header rather than assuming the defaults, so
  # the checker adjudicates the container the producer actually wrote.
  let blockSize = readU32LE(raw, 8)
  var maxEntries = readU32LE(raw, 12)
  if blockSize == 0'u32:
    fail(args[0] & " declares a zero block size")
  if maxEntries == 0'u32:
    maxEntries = uint32(
      (int(blockSize) - HeaderSize - ExtHeaderSize) div FileEntrySize)
  # `CTFS-Binary-Format.md` §5d: a **reader** must accept a container whose
  # length is not a whole number of blocks and ignore the partial tail. This
  # program adjudicates other implementations' containers, so it has to accept
  # exactly what the spec says is acceptable — until M58 it refused here,
  # before reading anything, which meant the wasm recorder's Go reader (which
  # M57 made accept) deferred to a checker that would have rejected the same
  # file. An adjudicator that disagrees with the thing it adjudicates is worse
  # than either answer.
  #
  # The refusal was never a corruption detector, either. On a genuinely
  # *truncated* container the surviving streams still read back byte-exact and
  # the lost one is refused by name — by `readInternalFile`'s bound against the
  # container's whole blocks, which is where the check belongs, because it is
  # the only place that knows whether the missing bytes are actually
  # referenced. A partial tail is unreferenced by construction (block 0 is the
  # previous one), so it must not cost the reader the whole file.
  #
  # What is still refused is a container that cannot hold its own block 0.
  if raw.len < int(blockSize):
    fail(args[0] & " is " & $raw.len & " bytes, which is less than its own " &
      "declared block size of " & $blockSize & "; block 0 is not present")
  let partialTail = raw.len mod int(blockSize)
  if partialTail != 0:
    # Say it out loud rather than tolerating it silently: a checker that
    # accepted this state without a word would let a producer regress into
    # writing torn containers unnoticed.
    try:
      stderr.writeLine("check_ctfs_container: note: " & args[0] & " is " &
        $raw.len & " bytes — " & $(raw.len div int(blockSize)) & " whole " &
        $blockSize & "-byte blocks plus a " & $partialTail &
        "-byte partial tail. Accepted per CTFS-Binary-Format.md §5d; the " &
        "partial region is unaddressable and every stream below is resolved " &
        "against the whole blocks only.")
    except IOError, ValueError:
      discard

  var manifest = ""
  try:
    manifest = readFile(args[1])
  except IOError, OSError:
    fail("cannot read manifest " & args[1])

  var checkedPresent = 0
  var checkedAbsent = 0
  for rawLine in manifest.splitLines():
    let line = rawLine.strip()
    if line.len == 0 or line.startsWith("#"):
      continue

    if line.startsWith("!"):
      let name = line[1 .. ^1].strip()
      if hasInternalFile(raw, name, maxEntries):
        fail("internal file " & name & " is present but the manifest says it " &
          "must not be; a reader that finds everything proves nothing")
      inc checkedAbsent
      continue

    let parts = line.split(' ', 1)
    if parts.len != 2:
      fail("malformed manifest line: " & line)
    let name = parts[0]
    let expected = readBinary(parts[1].strip())

    if not hasInternalFile(raw, name, maxEntries):
      fail("internal file " & name & " is not present in " & args[0])
    let got = readInternalFile(raw, name, blockSize, maxEntries)
    if got.isErr:
      fail("cannot read internal file " & name & ": " & got.error)
    let bytes = got.get()
    if bytes.len != expected.len:
      fail(name & " is " & $bytes.len & " bytes, expected " & $expected.len)
    let d = firstDifference(bytes, expected)
    if d >= 0:
      fail(name & " differs from the expected content at byte " & $d &
        " (data block " & $(d div int(blockSize)) & " of the internal file): " &
        "got " & $bytes[d] & ", want " & $expected[d])
    inc checkedPresent

  if checkedPresent == 0:
    fail("the manifest asked for no internal file by name; that would pass " &
      "vacuously")
  if checkedAbsent == 0:
    fail("the manifest carries no `!name` negative control; that would let a " &
      "reader which finds everything pass")

  try:
    echo "check_ctfs_container: OK (" & $checkedPresent & " present, " &
      $checkedAbsent & " absent, " & $partialTail & "-byte partial tail)"
  except IOError, ValueError:
    discard

when isMainModule:
  main()
