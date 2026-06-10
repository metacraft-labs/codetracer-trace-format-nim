{.push raises: [].}

## Step event encoding for the execution stream.
##
## Events are tagged with a single byte, followed by varint-encoded fields.
## DeltaStep uses signed (zigzag) varint for the line delta, enabling most
## sequential steps to encode in just 2 bytes (1 tag + 1 varint byte).
##
## # Column-aware steps (P6.3 / P6.4)
##
## Tag ``0x07`` (``sekDeltaColumn``) advances the column-only axis of a
## column-aware trace.  Its presence on the wire is gated by the
## ``FLAG_HAS_COLUMN_AWARE_STEPS`` (bit 4) flag in ``meta.dat`` — see the
## canonical spec at ``codetracer-trace-format-spec/trace-events.md`` §
## "Column Encoding — `DeltaColumn` (chosen)".  Writers MUST NOT emit tag
## 0x07 when the trace's column-aware flag is clear; readers MUST refuse
## any trace whose ``meta.dat`` flag bit set rejects the trace at
## metadata-parse time before the step stream is touched.
##
## The Nim field names ``globalLineIndex`` (on ``sekAbsoluteStep``) and
## ``lineDelta`` (on ``sekDeltaStep``) are spec-renamed to
## ``global_position_index`` / step ``delta``.  We keep the original
## field names so the existing in-workspace consumers compile unchanged.
## When ``FLAG_HAS_COLUMN_AWARE_STEPS`` is set the same bytes address a
## one-dimensional ``(line, column)`` position; when clear they address a
## line in the per-file contiguous range scheme.  The on-wire bytes are
## identical in both modes — only the interpretation changes.

import results
import ./varint

const
  TagAbsoluteStep*: byte = 0x00
  TagDeltaStep*: byte = 0x01
  TagRaise*: byte = 0x02
  TagCatch*: byte = 0x03
  TagThreadSwitch*: byte = 0x04
  TagThreadStart*: byte = 0x05
  TagThreadExit*: byte = 0x06
  TagDeltaColumn*: byte = 0x07
    ## Column-only step within the current line.  Allowed on the wire only
    ## when the trace's ``meta.dat`` ``FLAG_HAS_COLUMN_AWARE_STEPS`` flag
    ## is set.  See spec §"Column Encoding — `DeltaColumn` (chosen)".

type
  StepEventKind* = enum
    sekAbsoluteStep
    sekDeltaStep
    sekRaise
    sekCatch
    sekThreadSwitch
    sekThreadStart
    sekThreadExit
    sekDeltaColumn

  StepEvent* = object
    case kind*: StepEventKind
    of sekAbsoluteStep:
      globalLineIndex*: uint64
        ## Spec name: ``global_position_index``.  Addresses a line when
        ## the trace is column-unaware, a ``(line, column)`` pair when the
        ## column-aware flag is set.  Field name kept for back-compat.
    of sekDeltaStep:
      lineDelta*: int64
        ## Spec name: step ``delta``.  Signed delta over
        ## ``global_position_index``.  When the column-aware flag is set
        ## this delta may cross line boundaries (resetting the column to
        ## 1 in the decoder) or stay within a line (column unchanged).
    of sekRaise:
      exceptionTypeId*: uint64
      message*: seq[byte]
    of sekCatch:
      catchExceptionTypeId*: uint64
    of sekThreadSwitch:
      threadId*: uint64
    of sekThreadStart:
      startThreadId*: uint64
    of sekThreadExit:
      exitThreadId*: uint64
    of sekDeltaColumn:
      columnDelta*: int64
        ## Signed zigzag delta over the current column position.  Line is
        ## unchanged.  Column-aware traces only (tag 0x07).

proc encodeStepEvent*(event: StepEvent, output: var seq[byte]) =
  ## Encode a step event to binary.
  case event.kind
  of sekAbsoluteStep:
    output.add(TagAbsoluteStep)
    encodeVarint(event.globalLineIndex, output)
  of sekDeltaStep:
    output.add(TagDeltaStep)
    encodeSignedVarint(event.lineDelta, output)
  of sekRaise:
    output.add(TagRaise)
    encodeVarint(event.exceptionTypeId, output)
    encodeVarint(uint64(event.message.len), output)
    output.add(event.message)
  of sekCatch:
    output.add(TagCatch)
    encodeVarint(event.catchExceptionTypeId, output)
  of sekThreadSwitch:
    output.add(TagThreadSwitch)
    encodeVarint(event.threadId, output)
  of sekThreadStart:
    output.add(TagThreadStart)
    encodeVarint(event.startThreadId, output)
  of sekThreadExit:
    output.add(TagThreadExit)
    encodeVarint(event.exitThreadId, output)
  of sekDeltaColumn:
    output.add(TagDeltaColumn)
    encodeSignedVarint(event.columnDelta, output)

proc decodeStepEvent*(data: openArray[byte], pos: var int): Result[StepEvent, string] =
  ## Decode one step event from data starting at pos.
  if pos >= data.len:
    return err("unexpected end of step stream")
  let tag = data[pos]
  pos += 1
  case tag
  of TagAbsoluteStep:
    let gli = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekAbsoluteStep, globalLineIndex: gli))
  of TagDeltaStep:
    let delta = ?decodeSignedVarint(data, pos)
    ok(StepEvent(kind: sekDeltaStep, lineDelta: delta))
  of TagRaise:
    let typeId = ?decodeVarint(data, pos)
    let msgLen = ?decodeVarint(data, pos)
    if pos + int(msgLen) > data.len:
      return err("raise message truncated")
    var msg = newSeq[byte](int(msgLen))
    for i in 0 ..< int(msgLen):
      msg[i] = data[pos + i]
    pos += int(msgLen)
    ok(StepEvent(kind: sekRaise, exceptionTypeId: typeId, message: msg))
  of TagCatch:
    let typeId = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekCatch, catchExceptionTypeId: typeId))
  of TagThreadSwitch:
    let tid = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekThreadSwitch, threadId: tid))
  of TagThreadStart:
    let tid = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekThreadStart, startThreadId: tid))
  of TagThreadExit:
    let tid = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekThreadExit, exitThreadId: tid))
  of TagDeltaColumn:
    let delta = ?decodeSignedVarint(data, pos)
    ok(StepEvent(kind: sekDeltaColumn, columnDelta: delta))
  else:
    err("unknown step event tag: " & $tag)
