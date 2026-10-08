## A full container's block size is 1024, 2048 or 4096
## (`ctfs-container.md` §1). A writer refuses any other, and a reader refuses
## a container declaring any other, naming the value.
##
## No mocks: the real C entry point creates real files, read back with this
## repository's reader.

include codetracer_trace_writer_ffi

{.pop.}

import std/[os, strutils]
import codetracer_trace_writer/new_trace_reader as ntr

proc lastErr(): string =
  let e = trace_writer_last_error()
  if e.isNil: "" else: $e

let dir = getTempDir() / "ctfs_block_sizes"
createDir(dir)

for bs in [1024'u32, 2048, 4096]:
  let p = dir / ("ok" & $bs & ".ct")
  doAssert ct_container_create(cstring(p), bs) == 0,
    "block size " & $bs & " is accepted: " & lastErr()
  let bytes = cast[seq[byte]](readFile(p))
  doAssert ntr.openNewTraceFromBytes(bytes).isOk, "a " & $bs & "-byte container opens"

for bs in [512'u32, 8192, 16384, 4104]:
  trace_writer_clear_last_error()
  let p = dir / ("bad" & $bs & ".ct")
  doAssert ct_container_create(cstring(p), bs) != 0,
    "block size " & $bs & " must be refused by the writer"
  doAssert ($bs) in lastErr(), "the refusal names " & $bs & ": " & lastErr()

# A container declaring 8192: a valid 4096-byte one with its BlockSize field
# rewritten, padded to one 8192-byte block.
let src = dir / "ok4096.ct"
var image = cast[seq[byte]](readFile(src))
image.setLen(8192)
image[8] = 0; image[9] = 0x20; image[10] = 0; image[11] = 0
let opened = ntr.openNewTraceFromBytes(image)
doAssert opened.isErr, "a container declaring an 8192-byte block is refused"
doAssert "8192" in opened.error, "the refusal names the value: " & opened.error
let p8 = dir / "declares8192.ct"
writeFile(p8, cast[string](image))
let openedPath = ntr.openNewTrace(p8)
doAssert openedPath.isErr and "8192" in openedPath.error,
  "and from a path: " & (if openedPath.isErr: openedPath.error else: "opened")
# The read-side entry points that take a path refuse it too: a container
# that cannot be read is a failure, not a container lacking the member.
var buf: ptr uint8
var n: csize_t
for (name, call) in [
    ("ct_linehits_json", ct_linehits_json(cstring(p8), addr buf, addr n)),
    ("ct_correlation_index_json", ct_correlation_index_json(cstring(p8), addr buf, addr n)),
    ("ct_marker_labels_json", ct_marker_labels_json(cstring(p8), addr buf, addr n))]:
  doAssert call == -1, name & " refuses a container declaring 8192, not reports it unindexed"
let empty = dir / "empty.ct"
writeFile(empty, "")
trace_writer_clear_last_error()
doAssert ct_linehits_json(cstring(empty), addr buf, addr n) == -1,
  "an empty file is not a container lacking linehits.tc"
doAssert lastErr().len > 0
removeDir(dir)
echo "test_block_size_is_one_of_three: OK"
