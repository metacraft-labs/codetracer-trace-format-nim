{.push raises: [].}

## M50: Tests for the CTFS network block fetcher.
##
## Creates a mock transport that serves blocks from an in-memory trace,
## then verifies the NetworkBlockFetcher reads correctly and caches results.

import results
import codetracer_ctfs/query_protocol
import codetracer_ctfs/network_reader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

const BlockSize = 4096

proc makeBlock(blockId: int, size: int): seq[byte] =
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte((blockId * 31 + i) mod 256)

type CallCounter* = ref object
  value*: int

proc makeMockTransport(blocks: seq[seq[byte]], callCount: CallCounter): Transport =
  ## Create a transport that responds to FetchBlock requests from in-memory data.
  result = proc(request: seq[byte]): Result[seq[byte], string] {.raises: [].} =
    callCount.value += 1

    # Decode the request
    let reqRes = decodeRequest(request)
    if reqRes.isErr:
      return ok(encodeError(0, ecInternal, "bad request: " & reqRes.error))

    let req = reqRes.get()
    case req.kind
    of rkFetchBlock:
      let bid = int(req.fetchBlock.blockId)
      if bid >= blocks.len:
        return ok(encodeError(
          uint64(requestKindToByte(rkFetchBlock)),
          ecOutOfRange,
          "block " & $bid & " out of range"))
      ok(encodeBlockData(req.fetchBlock.blockId, blocks[bid]))
    else:
      ok(encodeError(0, ecUnknownRequest, "unsupported request type"))

# ---------------------------------------------------------------------------
# test_basic_read
# ---------------------------------------------------------------------------

proc test_basic_read() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let callCount = CallCounter(value: 0)
  let transport = makeMockTransport(blocks, callCount)

  var fetcher = initNetworkBlockFetcher(transport,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 256 * 1024)

  # Read block 3
  let r1 = fetcher.readBlock(3)
  doAssert r1.isOk, "readBlock(3) failed: " & r1.error
  doAssert r1.get() == blocks[3], "data mismatch for block 3"
  doAssert callCount.value == 1, "transport should have been called once, got " & $callCount.value

  echo "PASS: test_basic_read"

# ---------------------------------------------------------------------------
# test_caching
# ---------------------------------------------------------------------------

proc test_caching() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let callCount = CallCounter(value: 0)
  let transport = makeMockTransport(blocks, callCount)

  var fetcher = initNetworkBlockFetcher(transport,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 256 * 1024)

  # First read — triggers transport call
  let r1 = fetcher.readBlock(5)
  doAssert r1.isOk, "first readBlock(5) failed: " & r1.error
  let firstCallCount = callCount.value

  # Second read — should be cached (no new transport call)
  let r2 = fetcher.readBlock(5)
  doAssert r2.isOk, "second readBlock(5) failed: " & r2.error
  doAssert r2.get() == blocks[5], "cached data mismatch"
  doAssert callCount.value == firstCallCount,
    "transport should NOT have been called again, was " & $callCount.value &
    " (first was " & $firstCallCount & ")"

  echo "PASS: test_caching"

# ---------------------------------------------------------------------------
# test_multiple_blocks
# ---------------------------------------------------------------------------

proc test_multiple_blocks() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 20:
    blocks.add(makeBlock(i, BlockSize))

  let callCount = CallCounter(value: 0)
  let transport = makeMockTransport(blocks, callCount)

  var fetcher = initNetworkBlockFetcher(transport,
    ramMaxBytes = 128 * 1024,
    diskMaxBytes = 512 * 1024)

  # Read all 20 blocks
  for i in 0'u64 ..< 20:
    let r = fetcher.readBlock(i)
    doAssert r.isOk, "readBlock(" & $i & ") failed: " & r.error
    doAssert r.get() == blocks[int(i)], "data mismatch at block " & $i

  doAssert callCount.value == 20, "expected 20 transport calls, got " & $callCount.value

  echo "PASS: test_multiple_blocks"

# ---------------------------------------------------------------------------
# test_out_of_range_error
# ---------------------------------------------------------------------------

proc test_out_of_range_error() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 5:
    blocks.add(makeBlock(i, BlockSize))

  let callCount = CallCounter(value: 0)
  let transport = makeMockTransport(blocks, callCount)

  var fetcher = initNetworkBlockFetcher(transport,
    ramMaxBytes = 64 * 1024,
    diskMaxBytes = 256 * 1024)

  # Request a block that doesn't exist
  let r = fetcher.readBlock(999)
  doAssert r.isErr, "readBlock(999) should have failed"

  echo "PASS: test_out_of_range_error"

# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------

test_basic_read()
test_caching()
test_multiple_blocks()
test_out_of_range_error()
