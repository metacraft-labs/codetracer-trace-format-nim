{.push raises: [].}

## M51: Tests for configurable replication.
##
## Verifies quorum-based reads with 3 replicas and readQuorum=2.

import results
import codetracer_ctfs/query_protocol
import codetracer_ctfs/network_reader
import codetracer_ctfs/replication

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

const BlockSize = 4096

proc makeBlock(blockId: int, size: int): seq[byte] =
  result = newSeq[byte](size)
  for i in 0 ..< size:
    result[i] = byte((blockId * 31 + i) mod 256)

proc makeWorkingTransport(blocks: seq[seq[byte]]): Transport =
  ## A transport that always succeeds.
  result = proc(request: seq[byte]): Result[seq[byte], string] {.raises: [].} =
    let reqRes = decodeRequest(request)
    if reqRes.isErr:
      return ok(encodeError(0, ecInternal, "bad request"))
    let req = reqRes.get()
    case req.kind
    of rkFetchBlock:
      let bid = int(req.fetchBlock.blockId)
      if bid >= blocks.len:
        return ok(encodeError(0, ecOutOfRange, "out of range"))
      ok(encodeBlockData(req.fetchBlock.blockId, blocks[bid]))
    else:
      ok(encodeError(0, ecUnknownRequest, "unsupported"))

proc makeFailingTransport(): Transport =
  ## A transport that always fails (simulates a down replica).
  result = proc(request: seq[byte]): Result[seq[byte], string] {.raises: [].} =
    err("connection refused")

# ---------------------------------------------------------------------------
# test_all_replicas_up
# ---------------------------------------------------------------------------

proc test_all_replicas_up() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let config = ReplicationConfig(
    replicaCount: 3,
    readQuorum: 2,
    writeQuorum: 2
  )

  let transports = @[
    makeWorkingTransport(blocks),
    makeWorkingTransport(blocks),
    makeWorkingTransport(blocks),
  ]

  let rr = initReplicatedReader(config, transports)

  let r = rr.readBlock(5)
  doAssert r.isOk, "readBlock(5) should succeed with all replicas up: " & r.error
  doAssert r.get() == blocks[5], "data mismatch"

  echo "PASS: test_all_replicas_up"

# ---------------------------------------------------------------------------
# test_one_replica_down
# ---------------------------------------------------------------------------

proc test_one_replica_down() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let config = ReplicationConfig(
    replicaCount: 3,
    readQuorum: 2,
    writeQuorum: 2
  )

  # Replica 1 is down, replicas 0 and 2 are up
  let transports = @[
    makeWorkingTransport(blocks),
    makeFailingTransport(),
    makeWorkingTransport(blocks),
  ]

  let rr = initReplicatedReader(config, transports)

  let r = rr.readBlock(3)
  doAssert r.isOk, "readBlock(3) should succeed with 2/3 replicas up (quorum=2): " & r.error
  doAssert r.get() == blocks[3], "data mismatch"

  echo "PASS: test_one_replica_down"

# ---------------------------------------------------------------------------
# test_two_replicas_down
# ---------------------------------------------------------------------------

proc test_two_replicas_down() {.raises: [].} =
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let config = ReplicationConfig(
    replicaCount: 3,
    readQuorum: 2,
    writeQuorum: 2
  )

  # Only replica 2 is up — 1/3 < quorum of 2
  let transports = @[
    makeFailingTransport(),
    makeFailingTransport(),
    makeWorkingTransport(blocks),
  ]

  let rr = initReplicatedReader(config, transports)

  let r = rr.readBlock(3)
  doAssert r.isErr, "readBlock(3) should fail with only 1/3 replicas up (quorum=2)"

  echo "PASS: test_two_replicas_down"

# ---------------------------------------------------------------------------
# test_quorum_1_always_succeeds
# ---------------------------------------------------------------------------

proc test_quorum_1_always_succeeds() {.raises: [].} =
  ## With readQuorum=1, even a single working replica is enough.
  var blocks: seq[seq[byte]]
  for i in 0 ..< 10:
    blocks.add(makeBlock(i, BlockSize))

  let config = ReplicationConfig(
    replicaCount: 3,
    readQuorum: 1,
    writeQuorum: 1
  )

  let transports = @[
    makeFailingTransport(),
    makeFailingTransport(),
    makeWorkingTransport(blocks),
  ]

  let rr = initReplicatedReader(config, transports)

  let r = rr.readBlock(7)
  doAssert r.isOk, "readBlock(7) should succeed with quorum=1 and 1 replica up: " & r.error
  doAssert r.get() == blocks[7], "data mismatch"

  echo "PASS: test_quorum_1_always_succeeds"

# ---------------------------------------------------------------------------
# test_all_replicas_down
# ---------------------------------------------------------------------------

proc test_all_replicas_down() {.raises: [].} =
  let config = ReplicationConfig(
    replicaCount: 3,
    readQuorum: 1,
    writeQuorum: 1
  )

  let transports = @[
    makeFailingTransport(),
    makeFailingTransport(),
    makeFailingTransport(),
  ]

  let rr = initReplicatedReader(config, transports)

  let r = rr.readBlock(0)
  doAssert r.isErr, "readBlock should fail when all replicas are down"

  echo "PASS: test_all_replicas_down"

# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------

test_all_replicas_up()
test_one_replica_down()
test_two_replicas_down()
test_quorum_1_always_succeeds()
test_all_replicas_down()
