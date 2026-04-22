{.push raises: [].}

## M51: Configurable replication for CTFS storage nodes.
##
## Provides `ReplicatedReader` that queries multiple storage backends
## (transports) and returns the first successful response, subject to
## a quorum requirement.
##
## Currently works against a list of mock transports for testing.
## In production, each transport would be a network connection to a
## storage node.

import results
import ./network_reader
import ./query_protocol

type
  ReplicationConfig* = object
    replicaCount*: int    ## Total number of replicas
    readQuorum*: int      ## Minimum successful reads required
    writeQuorum*: int     ## Minimum successful writes required (for future use)

  ReplicatedReader* = object
    config*: ReplicationConfig
    transports: seq[Transport]  ## One transport per replica

proc initReplicatedReader*(config: ReplicationConfig,
    transports: seq[Transport]): ReplicatedReader =
  ## Create a replicated reader over the given transports.
  doAssert transports.len == config.replicaCount,
    "transport count must match replicaCount"
  doAssert config.readQuorum >= 1,
    "readQuorum must be >= 1"
  doAssert config.readQuorum <= config.replicaCount,
    "readQuorum must be <= replicaCount"
  ReplicatedReader(
    config: config,
    transports: transports
  )

proc readBlock*(rr: ReplicatedReader, blockId: uint64): Result[seq[byte], string] =
  ## Read a block from replicas. Tries all replicas and succeeds if at least
  ## `readQuorum` return successfully. Returns the data from the first success.
  ##
  ## This is a simple quorum strategy: we need at least `readQuorum` successes
  ## to trust the result. All replicas are queried (could be optimized to
  ## short-circuit once quorum is met).

  # Build the request payload once
  var req = Request(kind: rkFetchBlock)
  req.fetchBlock = FetchBlockRequest(blockId: blockId)
  let payload = encodeRequest(req)

  var successCount = 0
  var firstData: seq[byte]
  var lastError = ""

  for i in 0 ..< rr.transports.len:
    let respRes = rr.transports[i](payload)
    if respRes.isErr:
      lastError = respRes.error
      continue

    let respPayload = respRes.get()
    let rtRes = responseTypeByte(respPayload)
    if rtRes.isErr:
      lastError = rtRes.error
      continue

    case rtRes.get()
    of rsBlockData:
      var pos = 1
      let bdRes = decodeBlockDataResponse(respPayload, pos)
      if bdRes.isErr:
        lastError = bdRes.error
        continue
      let (_, data) = bdRes.get()
      successCount += 1
      if firstData.len == 0:
        firstData = data
    of rsError:
      var pos = 1
      let errRes = decodeErrorResponse(respPayload, pos)
      if errRes.isErr:
        lastError = errRes.error
      else:
        lastError = "remote error: " & errRes.get().message
      continue
    else:
      lastError = "unexpected response type"
      continue

  if successCount >= rr.config.readQuorum:
    ok(firstData)
  else:
    err("quorum not met: " & $successCount & "/" & $rr.config.readQuorum &
      " succeeded (last error: " & lastError & ")")
