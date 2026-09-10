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
import ./gdh2_arms

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
  TagSourceReload*: byte = 0x08
    ## GDH-M2 — a source-version transition (design §6.3 of
    ## ``codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.md``).
    ##
    ## Marks the point in the step stream at which one or more source
    ## files were reloaded and re-registered under NEW path indices.  It
    ## is NOT a position: it carries no global position index, is not
    ## steppable-to, and does not advance the running absolute address.
    ##
    ## Payload::
    ##
    ##   reload_ordinal:   varint   (1-based, monotonic within the trace)
    ##   changed_count:    varint
    ##   changed[]:        { old_path_id, new_path_id, generation } varints
    ##   in_flight_frames: varint
    ##
    ## Allowed on the wire ONLY when the container declares
    ## ``FlagExtHasSourceReload`` (extended flag bit 0, meta.dat schema
    ## version 5).  ``decodeStepEvent`` refuses it BY NAME otherwise —
    ## see the ``allowSourceReload`` parameter, and the note on that
    ## parameter for why refusal rather than skipping is the only safe
    ## answer.

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
    sekSourceReload

  SourceReloadChange* = object
    ## One file's transition across a reload boundary.
    oldPathId*: uint64
      ## The ``paths.dat`` id the file's steps resolved to BEFORE the
      ## reload — the version that was live when the marker was emitted.
    newPathId*: uint64
      ## The id its steps resolve to after.  Distinct from ``oldPathId``
      ## by construction: a reload that did not mint a new index did not
      ## change what the trace can express, and design §6.3.1 rejects
      ## inference from the indices precisely so this pair is RECORDED
      ## rather than reconstructed by a consumer scanning the prefix.
    generation*: uint64
      ## The WIRE generation from the observer's ``sourceChanged``
      ## notification (design §4.3): 1 is the content the process
      ## started with, so a marker's generation is >= 2.  Deliberately
      ## off by one from ``Location.source_generation``, which is the
      ## container-side 0-based version ordinal (design §7.0); the
      ## marker carries the wire number so the mapping between the two
      ## is recorded rather than assumed.

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
    of sekSourceReload:
      reloadOrdinal*: uint64
        ## 1-based, monotonic within the trace.  A writer that emitted a
        ## constant here would make the second reload indistinguishable
        ## from the first — the `symbolGeneration: 1` defect
        ## (`repro_hcr_agent.c:1338`) this campaign refuses to inherit.
      changed*: seq[SourceReloadChange]
      inFlightFrames*: uint64
        ## Frames still executing the OLD version's bytecode when the
        ## marker was emitted (design §5.4).  Steps belonging to those
        ## frames legitimately appear AFTER the marker and carry the OLD
        ## path id.  Recorded rather than implied, so a consumer cannot
        ## read the marker as a clean cut.

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
  of sekSourceReload:
    output.add(TagSourceReload)
    # FALSIFIER (``gdh2FalsifyConstantOrdinal``, gdh2_reload_marker_round_trips):
    # write the ordinal as a literal 1.  This is not a hypothetical
    # mutation — it is `repro_hcr_agent.c:1338`'s defect transplanted,
    # where `"symbolGeneration":1` is a byte inside a format string
    # rather than a `%d`, and it survived there because nothing consumed
    # the field.  The FIRST marker is still correct under it; only the
    # second is wrong, which is why the gate must exercise two.
    when gdh2Arm(gdh2FalsifyConstantOrdinal):
      encodeVarint(1'u64, output)
    else:
      encodeVarint(event.reloadOrdinal, output)
    encodeVarint(uint64(event.changed.len), output)
    for ch in event.changed:
      # FALSIFIER (``gdh2FalsifySwappedIds``, gdh2_reload_marker_round_trips):
      # write the pair the other way round, so the marker names the file
      # the reload arrived AT as the one it came FROM.  Added by review:
      # every other arm on this gate is caught by a check the marker's own
      # bytes can answer — the ordinal is wrong, both ids are zero, the
      # record is uncounted, the flag is on a container that has none.  A
      # SWAP leaves the marker perfectly well-formed: two distinct,
      # non-zero, registered ids, the right count, the right ordinal, the
      # right generation.  Nothing but the CROSS-TIE to the ids the steps
      # at ``stepIndex ∓ 1`` independently resolve to can see it, so this
      # arm is what demonstrates GDH-G7 is asserted by cross-tie rather
      # than by presence.  It is also the likelier real defect of the two:
      # transposing a pair of same-typed adjacent arguments is an ordinary
      # slip, whereas zeroing both is not.
      when gdh2Arm(gdh2FalsifySwappedIds):
        encodeVarint(ch.newPathId, output)
        encodeVarint(ch.oldPathId, output)
      else:
        encodeVarint(ch.oldPathId, output)
        encodeVarint(ch.newPathId, output)
      encodeVarint(ch.generation, output)
    encodeVarint(event.inFlightFrames, output)

proc decodeStepEvent*(data: openArray[byte], pos: var int,
    allowSourceReload: bool = false): Result[StepEvent, string] =
  ## Decode one step event from data starting at pos.
  ##
  ## ``allowSourceReload`` mirrors the container's declaration: tag 0x08
  ## (``TagSourceReload``) is accepted only when ``meta.dat`` carries
  ## ``FlagExtHasSourceReload``.  The default is FALSE so that every
  ## caller that has not been taught to consult the flag refuses the tag
  ## rather than decoding it — the strict-rejection contract bit 13's
  ## documentation states (``meta_dat.nim``), applied one layer down.
  ##
  ## The alternative — SKIPPING an unknown tag — is what this signature
  ## exists to make impossible.  A skip cannot know the record's length,
  ## so the payload varints are re-read as further events and the stream
  ## decodes SHORTER and plausibly: wrong bytes instead of an error.
  if pos >= data.len:
    return err("unexpected end of step stream")
  let tag = data[pos]
  pos += 1
  # FALSIFIER (``gdh2FalsifyUngatedDecode``, gdh2_unknown_tag_is_refused_by_name):
  # decode tag 0x08 whatever the container declares.  The refusal is the
  # whole strict-rejection contract; without it a reader built after this
  # milestone silently accepts a stream shape the container never said it
  # had, and the rollout rule bits 13-15 record becomes unenforceable.
  #
  # FALSIFIER (``gdh2FalsifySkipUnknownTag``, same gate): SKIP the record
  # instead of refusing it — the "be liberal in what you accept" reflex.
  # A skip cannot know the record's length, so the payload varints are
  # re-read as further events: the stream decodes SHORTER and plausibly,
  # which is strictly worse than an error because nothing reports it.
  when gdh2Arm(gdh2FalsifySkipUnknownTag):
    if tag == TagSourceReload and not allowSourceReload:
      return ok(StepEvent(kind: sekThreadSwitch, threadId: 0))
  if tag == TagSourceReload and not allowSourceReload and
      not gdh2Arm(gdh2FalsifyUngatedDecode):
    return err("step event tag: 8 (0x08, TagSourceReload) is present in " &
      "the step stream, but this container does not declare it: meta.dat " &
      "carries no FlagExtHasSourceReload (extended flag bit 0, schema " &
      "version 5). Refused rather than skipped — a skipped record's " &
      "payload would be re-read as further events and the stream would " &
      "decode shorter and plausibly instead of failing")
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
  of TagSourceReload:
    let ordinal = ?decodeVarint(data, pos)
    let count = ?decodeVarint(data, pos)
    # A count is a length prefix read off the wire, so it is bounded
    # against the bytes that remain before anything is allocated: the
    # smallest possible change record is three one-byte varints.
    if int(count) > (data.len - pos) div 3:
      return err("source reload marker claims " & $count &
        " changed files, more than the remaining " & $(data.len - pos) &
        " bytes of the chunk can hold")
    var changed = newSeq[SourceReloadChange](int(count))
    for i in 0 ..< int(count):
      let oldId = ?decodeVarint(data, pos)
      let newId = ?decodeVarint(data, pos)
      let gen = ?decodeVarint(data, pos)
      changed[i] = SourceReloadChange(
        oldPathId: oldId, newPathId: newId, generation: gen)
    let inFlight = ?decodeVarint(data, pos)
    ok(StepEvent(kind: sekSourceReload, reloadOrdinal: ordinal,
      changed: changed, inFlightFrames: inFlight))
  else:
    err("unknown step event tag: " & $tag)
