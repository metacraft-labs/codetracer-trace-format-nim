{.push raises: [].}

## Step event encoding for the execution stream.
##
## Events are tagged with a single byte, followed by varint-encoded fields.
## DeltaStep uses signed (zigzag) varint for the line delta, enabling most
## sequential steps to encode in just 2 bytes (1 tag + 1 varint byte).

import results
import ./varint

const
  TagAbsoluteStep*: byte = 0x00
  TagDeltaStep*: byte = 0x01
  TagRaise*: byte = 0x02
  TagCatch*: byte = 0x03
  TagThreadSwitch*: byte = 0x04

type
  StepEventKind* = enum
    sekAbsoluteStep
    sekDeltaStep
    sekRaise
    sekCatch
    sekThreadSwitch

  StepEvent* = object
    case kind*: StepEventKind
    of sekAbsoluteStep:
      globalLineIndex*: uint64
    of sekDeltaStep:
      lineDelta*: int64
    of sekRaise:
      exceptionTypeId*: uint64
      message*: seq[byte]
    of sekCatch:
      catchExceptionTypeId*: uint64
    of sekThreadSwitch:
      threadId*: uint64

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
  else:
    err("unknown step event tag: " & $tag)
