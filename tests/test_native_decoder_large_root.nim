## A container that declares a root directory larger than 4096 entries is read
## by the native decoder and by ct-print, and one that declares a root no
## image of its size could hold is still refused as nonsensical.
##
## `ctfs-container.md` §1 bounds `MaxRootEntries` by nothing but the image: a
## root larger than block 0 continues into the blocks after it. A checkpointed
## MCR recording declares 10,922 entries up front (64 blocks of 4096 bytes;
## codetracer-native-recorder `RecorderCheckpointedMaxRootEntries`), and
## `growRootDirectory` doubles a root that fills. Both readers refused any
## count over 4096 ("nonsensical CTFS root header (blockSize=4096
## maxRoot=10922)"), so no recording a checkpointing recorder made could be
## opened with them, while the Rust readers opened it.

import std/strutils
import results
import codetracer_ctfs
import codetracer_ctfs/types
import codetracer_ctfs/container
import native_decoder

const
  BS = 4096'u32
  Declared = 10922  # (64 * 4096 - 16) div 24

var failures: seq[string]

proc check(cond: bool, what: string) =
  if not cond:
    failures.add(what)
    echo "FAIL: ", what
  else:
    echo "ok:   ", what

proc threadFileName(tid: int): string =
  result = "t"
  for _ in 0 ..< (11 - ($tid).len): result.add('0')
  result.add($tid)

proc bundle(declared: int): seq[byte] =
  ## meta.dat and one thread stream in a container declaring `declared` root
  ## entries.
  var c = createCtfs(blockSize = BS, maxRootEntries = uint32(declared))
  for name in ["meta.dat", threadFileName(0)]:
    var f = c.addFile(name).get()
    doAssert c.writeToFile(f, @[1'u8, 2, 3, 4]).isOk
  c.toBytes()

let large = bundle(Declared)
doAssert readU32LE(large, 12) == uint32(Declared)

check(rootDirectoryFits(large),
  "rootDirectoryFits: a declared 10,922-entry root that the image holds")

let info = detectNativeBundle(large)
check(info.isOk or "nonsensical" notin info.unsafeError,
  "detectNativeBundle does not call a 10,922-entry root nonsensical" &
  (if info.isErr: " (got: " & info.unsafeError & ")" else: ""))
if info.isOk:
  check(info.get().maxRoot == uint32(Declared),
    "detectNativeBundle reads the header's 10,922 entries")
  check(info.get().threadStreams.len == 1,
    "detectNativeBundle finds the thread stream")

# The control: a header declaring more entries than the image holds is still
# nonsensical. Only header bytes 12..15 change.
var tooLarge = large
writeU32LE(tooLarge, 12, uint32(Declared * 4))
check(not rootDirectoryFits(tooLarge),
  "rootDirectoryFits: a root larger than the image")
let refused = detectNativeBundle(tooLarge)
check(refused.isErr and "nonsensical" in refused.unsafeError,
  "detectNativeBundle refuses a root larger than the image" &
  (if refused.isOk: " (it opened)" else: " (got: " & refused.unsafeError & ")"))

if failures.len > 0:
  echo failures.len, " check(s) failed"
  quit(1)
echo "all checks passed"
