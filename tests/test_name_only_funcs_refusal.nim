## A container whose `funcs.dat` holds bare names is refused by name, saying
## what the records are and where they come from.
##
## The spec's `funcs.dat` record has always been
## `global_line_index: varint, name_len: varint, name`
## (`internal-files.md` §"Interning Tables"). The Nim writer wrote BARE NAMES
## there until b891a0f (2026-09-15), with `meta.dat` bit 12 clear — under the
## current schema versions 4 and 5, so such a container is not an old version
## a version check can refuse. It is a current-version container that does
## not conform, and there is no compatibility requirement to read it. What was
## wrong is the refusal: decoding a bare name as a structured record reported
## "funcs.dat record is truncated: declares a 115-byte name", which describes
## neither the record nor what to do.
##
## Asserted: under bit 12 clear, a bare-name record's refusal names the
## record shape, the writer revision that produced it, and re-recording as
## the remedy; and the control — the same writer's structured record — reads.
##
## No mocks: a real container from the real writer, with one record written
## the way the old writer wrote it and bit 12 cleared the way it left it.

import std/strutils
import results
import codetracer_trace_types
import codetracer_ctfs/container
import codetracer_trace_writer/interning_table
import codetracer_trace_writer/meta_dat
import codetracer_trace_writer/new_trace_reader

proc containerWithBareFuncName(): seq[byte] =
  ## A container with the four interning tables, one structured `funcs.dat`
  ## record (id 0, the control) and one bare-name record (id 1, as the
  ## pre-b891a0f writer wrote every one), and a version 4 `meta.dat` with
  ## bit 12 clear, as that writer left it.
  var c = createCtfs()
  var t = initTraceInterningTables(c).get()
  doAssert c.ensureId(t.paths, "/src/game.gd").isOk
  doAssert c.appendRecord(t.funcs, encodeFuncRecord(2, "structured_fn")).isOk
  let bare = "res://game/player.gd::_physics_process_with_a_long_enough_name"
  var bareBytes = newSeq[byte](bare.len)
  for i, ch in bare: bareBytes[i] = byte(ch)
  doAssert c.appendRecord(t.funcs, bareBytes).isOk
  let meta = TraceMetadata(recordingId: "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
    program: "name_only")
  let metaBytes = writeMetaDatToBuffer(meta, ["/src/game.gd"])
  var mf = c.addFile("meta.dat").get()
  doAssert c.writeToFile(mf, metaBytes).isOk
  c.toBytes()

proc test_a_bare_name_record_is_refused_by_name() =
  var r = openNewTraceFromBytes(containerWithBareFuncName()).get()
  doAssert not r.meta.hasInterningTables, "the fixture must have bit 12 clear"
  doAssert r.function(0).get() == "structured_fn",
    "control: a structured record reads"
  let bad = r.function(1)
  doAssert bad.isErr, "a bare-name funcs.dat record must be refused"
  let msg = bad.error
  for needle in ["bare name", "b891a0f", "re-record"]:
    doAssert needle in msg,
      "the refusal must say `" & needle & "`; got: " & msg
  echo "PASS: test_a_bare_name_record_is_refused_by_name"

test_a_bare_name_record_is_refused_by_name()
echo "ALL PASS: test_name_only_funcs_refusal"
