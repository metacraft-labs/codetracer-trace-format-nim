{.push raises: [].}

## M50: CTFS reader network backend.
##
## Provides `NetworkBlockFetcher` which wraps the query protocol and the
## cached block reader (from M23). The fetcher sends `FetchBlock` requests
## over a `Transport` abstraction and caches responses via `CachedBlockReader`.
##
## The transport is a simple callback: `proc(request: seq[byte]): Result[seq[byte], string]`.
## This allows testing with in-memory mocks and later swapping in real TCP/TLS.

import results
import ./query_protocol
import ./partial_trace_cache
import ./cached_trace_reader

type
  Transport* = proc(request: seq[byte]): Result[seq[byte], string] {.closure, raises: [].}
    ## Abstract transport: send a request payload, receive a response payload.
    ## No framing — the transport handles that internally.

  NetworkBlockFetcher* = object
    transport: Transport
    reader: CachedBlockReader
    fetchCount*: int  ## Number of times the transport was actually called

proc makeBlockFetcher(nf: var NetworkBlockFetcher): BlockFetcher =
  ## Create a BlockFetcher closure that uses the network transport.
  ## The closure captures nf by reference.
  let transport = nf.transport
  var fetchCountPtr = addr nf.fetchCount
  result = proc(blockId: uint64): Result[seq[byte], string] {.raises: [].} =
    # Build a FetchBlock request
    var req = Request(kind: rkFetchBlock)
    req.fetchBlock = FetchBlockRequest(blockId: blockId)
    let payload = encodeRequest(req)

    # Send over transport
    let respPayload = ?transport(payload)

    # Parse response
    let rtRes = responseTypeByte(respPayload)
    if rtRes.isErr:
      return err("bad response: " & rtRes.error)

    case rtRes.get()
    of rsBlockData:
      var pos = 1
      let bdRes = decodeBlockDataResponse(respPayload, pos)
      if bdRes.isErr:
        return err("decode block data: " & bdRes.error)
      let (_, data) = bdRes.get()
      fetchCountPtr[] += 1
      ok(data)
    of rsError:
      var pos = 1
      let errRes = decodeErrorResponse(respPayload, pos)
      if errRes.isErr:
        return err("decode error response: " & errRes.error)
      err("remote error: " & errRes.get().message)
    else:
      err("unexpected response type for FetchBlock")

proc initNetworkBlockFetcher*(transport: Transport,
    ramMaxBytes: uint64 = 256 * 1024 * 1024,
    diskMaxBytes: uint64 = 1024 * 1024 * 1024): NetworkBlockFetcher =
  ## Create a NetworkBlockFetcher that fetches blocks over the given transport
  ## and caches them in a two-layer cache (RAM + disk).
  result = NetworkBlockFetcher(
    transport: transport,
    fetchCount: 0
  )
  let fetcher = makeBlockFetcher(result)
  result.reader = initCachedBlockReader(fetcher,
    ramMaxBytes = ramMaxBytes,
    diskMaxBytes = diskMaxBytes)

proc readBlock*(nf: var NetworkBlockFetcher, blockId: uint64): Result[seq[byte], string] =
  ## Read a block through the cached reader (which calls the network transport
  ## on cache miss).
  nf.reader.readBlock(blockId)

proc ramHitRate*(nf: NetworkBlockFetcher): float =
  nf.reader.ramHitRate()

proc ramCacheCount*(nf: NetworkBlockFetcher): int =
  nf.reader.ramCacheCount()
