{.push raises: [].}

## Linehits builder: accumulates (globalLineIndex -> stepId) mappings
## during recording, then writes them to a namespace at finalize time.
##
## Each namespace entry is a concatenation of varint-encoded step_ids
## for a given global line index.

import std/tables
import results
import ../codetracer_ctfs/namespace
import ./varint

export results

type
  LinehitsBuilder* = object
    ns: Namespace
    ## Accumulated step_ids per global line index.
    ## Each value is a growing buffer of varint-encoded step_ids.
    pending: Table[uint64, seq[byte]]
    finalized: bool

proc initLinehitsBuilder*(): LinehitsBuilder =
  LinehitsBuilder(
    ns: initNamespace("linehits", ltTypeA),
    pending: initTable[uint64, seq[byte]](),
    finalized: false,
  )

proc recordHit*(b: var LinehitsBuilder, globalLineIndex: uint64,
    stepId: uint64) =
  ## Record that step `stepId` executed source line `globalLineIndex`.
  var buf = addr b.pending.mgetOrPut(globalLineIndex, newSeq[byte]())
  encodeVarint(stepId, buf[])

proc finalize*(b: var LinehitsBuilder): Result[void, string] =
  ## Flush all pending entries into the namespace.
  ## Must be called exactly once before any lookups.
  if b.finalized:
    return err("linehits builder already finalized")
  for key, data in b.pending:
    ?b.ns.append(key, data)
  b.finalized = true
  ok()

proc lookupHits*(b: LinehitsBuilder,
    globalLineIndex: uint64): Result[seq[uint64], string] =
  ## Query: return all step_ids that hit the given global line index.
  ## Only valid after finalize().
  if not b.finalized:
    return err("linehits builder not finalized")
  let dataRes = b.ns.lookup(globalLineIndex)
  if dataRes.isErr:
    return err(dataRes.error)
  let data = dataRes.get()
  var stepIds: seq[uint64]
  var pos = 0
  while pos < data.len:
    let v = ?decodeVarint(data, pos)
    stepIds.add(v)
  ok(stepIds)

proc hitCount*(b: LinehitsBuilder, globalLineIndex: uint64): int =
  ## Count hits for a line without fully decoding all step_ids.
  ## Returns 0 if not finalized or key not found.
  if not b.finalized:
    return 0
  let dataRes = b.ns.lookup(globalLineIndex)
  if dataRes.isErr:
    return 0
  let data = dataRes.get()
  var count = 0
  var pos = 0
  while pos < data.len:
    let v = decodeVarint(data, pos)
    if v.isErr:
      break
    count += 1
  count

proc lineCount*(b: LinehitsBuilder): int =
  ## Number of distinct lines that have been hit.
  b.pending.len
