{.push raises: [].}

## One inflated chunk of a stream whose chunks are zstd frames of
## varint-length-prefixed records (`values.dat`, `calls.dat`, `events.dat`),
## held for repeated reads.
##
## A stream reader keeps one `RecordChunk`. Loading a chunk inflates its frame
## into a buffer the `RecordChunk` keeps from one load to the next, through the
## thread's shared decompression context, and frames the records by noting
## where each one starts and ends. A record is then read in place with
## `record`, so a point read decodes the one record it wants and allocates
## nothing to reach it.

import results
import ../codetracer_ctfs/zstd_bindings
import ./varint

export results

type
  RecordChunk* = object
    index: int          ## the chunk held, -1 when none
    raw: seq[byte]      ## its inflated bytes
    bounds: seq[int]    ## record `i` is `raw[bounds[2*i] ..< bounds[2*i + 1]]`

proc initRecordChunk*(): RecordChunk =
  RecordChunk(index: -1)

proc held*(c: RecordChunk): int =
  ## The chunk this holds, or -1.
  c.index

proc len*(c: RecordChunk): int =
  ## How many records the held chunk frames.
  c.bounds.len div 2

proc load*(c: var RecordChunk, index: int, frame: openArray[byte],
    what: string): Result[void, string] =
  ## Inflate `frame`, chunk `index` of a stream of `what` records, and frame
  ## its records. On failure nothing is held. An empty frame is a chunk with
  ## no records.
  c.index = -1
  c.bounds.setLen(0)
  c.raw.setLen(0)
  if frame.len > 0:
    let size = ZSTD_getFrameContentSize(unsafeAddr frame[0], csize_t(frame.len))
    if size == ZSTD_CONTENTSIZE_UNKNOWN or size == ZSTD_CONTENTSIZE_ERROR:
      return err("cannot determine decompressed size for " & what & " chunk")
    c.raw.setLenUninit(int(size))  # every byte is written by the inflate
    if size > 0:
      let got = zstdDecompressShared(addr c.raw[0], csize_t(size),
        unsafeAddr frame[0], csize_t(frame.len))
      if ZSTD_isError(got) != 0:
        c.raw.setLen(0)
        return err("zstd decompress failed for " & what & " chunk: " &
          $ZSTD_getErrorName(got))
      c.raw.setLen(int(got))
  var pos = 0
  while pos < c.raw.len:
    var recLen: uint64
    if not readVarint(c.raw, pos, recLen):
      c.bounds.setLen(0)
      return err(decodeVarint(c.raw, pos).error)
    if recLen > uint64(c.raw.len - pos):
      c.bounds.setLen(0)
      return err(what & " record length extends past chunk")
    c.bounds.add(pos)
    pos += int(recLen)
    c.bounds.add(pos)
  c.index = index
  ok()

template record*(c: RecordChunk, i: int): untyped =
  ## Record `i` of the held chunk, in place. `i` must be below `len`.
  c.raw.toOpenArray(c.bounds[2 * i], c.bounds[2 * i + 1] - 1)
