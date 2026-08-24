{.push raises: [].}

## `addFile` rejects a duplicate root name (M61b hardening, ported from the
## native-recorder fork onto the canonical `codetracer_ctfs` library).
##
## Every reader resolves a name to the FIRST matching root entry, so a second
## entry with the same name does not update the file — it SHADOWS it, and the
## shadowed member becomes unreachable while still occupying the container.
## The failure is invisible (the write "succeeds", the read returns stale
## bytes), so the writer refuses the duplicate up front. NO MOCKS.

import std/strutils
import results
import codetracer_ctfs

const BS = 4096

proc test_add_file_rejects_a_duplicate_name() {.raises: [].} =
  var c = createCtfs(uint32(BS))

  let a = c.addFile("spans.dat")
  doAssert a.isOk, a.error
  var af = a.get()
  var payload: array[64, byte]
  for i in 0 ..< payload.len: payload[i] = byte(i)
  doAssert c.writeToFile(af, payload).isOk

  # A second entry with the same name is refused, even after it has content.
  let dup = c.addFile("spans.dat")
  doAssert dup.isErr, "addFile accepted a duplicate name"
  doAssert "duplicate" in dup.error, "unhelpful refusal: " & dup.error

  # A distinct name still succeeds.
  let other = c.addFile("meta.dat")
  doAssert other.isOk, "a distinct name was refused: " & other.error

  # And the duplicate is refused immediately after allocation too, before any
  # content is written (the entry's mapping-root pointer is already non-zero).
  let fresh = c.addFile("calls.dat")
  doAssert fresh.isOk, fresh.error
  let dup2 = c.addFile("calls.dat")
  doAssert dup2.isErr, "addFile accepted a duplicate of a just-created file"
  doAssert "duplicate" in dup2.error, "unhelpful refusal: " & dup2.error
  echo "PASS: test_add_file_rejects_a_duplicate_name"

when isMainModule:
  test_add_file_rejects_a_duplicate_name()
  echo "All ctfs duplicate-name tests passed!"
