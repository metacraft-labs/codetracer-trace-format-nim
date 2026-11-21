## The one reader protocol gains the attached mode (`ctfs-container.md` §6,
## "Reader Protocol"): the from-path image that `openNewTrace` and
## `NewTraceReader.refresh` follow attaches to the container's live
## coordination page and takes the root entries from its mirror, which may
## run ahead of the file between seals ("the page may run ahead of the file
## ... never behind it"); after the page is closed it reads the file.
##
## Null prediction: on the base the image read the root from the file only,
## so a member whose growth the page published ahead of the file read at its
## file size. Here the attached image sees the page's size and the member's
## bytes, which the writer put in the file first; and a refresh after close
## reads the file.

import std/[os]
import results
import codetracer_ctfs/[types, container, streaming, block_mapping, live_page,
                        member_view]

let path = getTempDir() / ("ckf4n_follow_" & $getCurrentProcessId() & ".ct")
discard tryRemoveFile(path)
var c = createCtfsStreaming(path).get()
var f = c.addFile("a.dat").get()
var payload = newSeq[byte](9000)
for i in 0 ..< payload.len: payload[i] = byte(i and 0xff)
doAssert c.writeToFile(f, payload[0 ..< 1000]).isOk
c.publish()
# The growth: bytes in the file, the page ahead, the file's entry not yet.
doAssert c.writeToFile(f, payload[1000 ..< 9000]).isOk
c.flushDirtyData()
let e = c.fileEntryOffset(f.entryIndex)
c.page.publishRoot(f.entryIndex, readU64LE(c.data, e), readU64LE(c.data, e + 8),
  readU64LE(c.data, e + 16))

var img = openFileImage(path).get()
doAssert img.readsFromFile
let v = viewMember(img, "a.dat").get()
doAssert v.len == 9000, "the attached image read a.dat at " & $v.len & " bytes, not the page's 9000"
doAssert v.contents().get() == payload, "the attached image's a.dat bytes differ"

doAssert c.closeCtfs().isOk
doAssert img.refresh().isOk
let v2 = viewMember(img, "a.dat").get()
doAssert v2.len == 9000 and v2.contents().get() == payload
discard tryRemoveFile(path)
echo "PASS test_follower_attaches"
