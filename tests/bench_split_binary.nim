{.push raises: [].}

## Benchmarks for split-binary event encoding/decoding.
##
## Generates 100,000 mixed events cycling through various event types
## and measures encode/decode throughput.
##
## Output is in a parseable format:
##   BENCH: <name> <value> <unit>

import std/times
import results
import codetracer_trace_writer/split_binary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc nowMs(): float64 =
  let t = cpuTime()
  t * 1000.0

const NumEvents = 100_000

proc generateMixedEvents(): seq[TraceLowLevelEvent] =
  ## Generate NumEvents events cycling through several event types.
  result = newSeq[TraceLowLevelEvent](NumEvents)
  var idx = 0
  for i in 0 ..< NumEvents:
    let cycle = i mod 8
    case cycle
    of 0:
      result[idx] = TraceLowLevelEvent(kind: tleStep,
        step: StepRecord(pathId: PathId(uint64(i mod 50)), line: Line(int64(i mod 1000))))
    of 1:
      result[idx] = TraceLowLevelEvent(kind: tleCall,
        callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(uint64(i mod 100)), args: @[]))
    of 2:
      result[idx] = TraceLowLevelEvent(kind: tleReturn,
        returnRecord: ReturnRecord(
          returnValue: ValueRecord(kind: vrkInt, intVal: int64(i), intTypeId: TypeId(7))))
    of 3:
      result[idx] = TraceLowLevelEvent(kind: tleValue,
        fullValue: FullValueRecord(variableId: VariableId(uint64(i mod 200)),
          value: ValueRecord(kind: vrkInt, intVal: int64(i * 3), intTypeId: TypeId(7))))
    of 4:
      result[idx] = TraceLowLevelEvent(kind: tlePath,
        path: "/src/file_" & $i & ".nim")
    of 5:
      result[idx] = TraceLowLevelEvent(kind: tleFunction,
        functionRecord: FunctionRecord(
          pathId: PathId(uint64(i mod 50)),
          line: Line(int64(i mod 500)),
          name: "func_" & $i))
    of 6:
      result[idx] = TraceLowLevelEvent(kind: tleDropLastStep)
    of 7:
      result[idx] = TraceLowLevelEvent(kind: tleThreadSwitch,
        threadSwitchId: ThreadId(uint64(i mod 4)))
    else:
      result[idx] = TraceLowLevelEvent(kind: tleDropLastStep)
    idx += 1

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

proc bench_encode() =
  ## Benchmark encoding: encode 100k mixed events.
  let events = generateMixedEvents()

  let startTime = nowMs()
  var enc = SplitBinaryEncoder.init()
  for event in events:
    enc.encodeEvent(event)
  let bytes = enc.getBytes()
  let elapsed = nowMs() - startTime

  let eventsPerSec = float64(NumEvents) / (elapsed / 1000.0)
  let mbPerSec = (float64(bytes.len) / (1024.0 * 1024.0)) / (elapsed / 1000.0)

  echo "BENCH: encode_events " & $NumEvents & " events"
  echo "BENCH: encode_time_ms " & $elapsed & " ms"
  echo "BENCH: encode_output_size " & $bytes.len & " bytes"
  echo "BENCH: encode_events_per_sec " & $eventsPerSec & " events/sec"
  echo "BENCH: encode_throughput " & $mbPerSec & " MB/s"

proc bench_decode() =
  ## Benchmark decoding: decode 100k mixed events from encoded bytes.
  let events = generateMixedEvents()
  var enc = SplitBinaryEncoder.init()
  for event in events:
    enc.encodeEvent(event)
  let bytes = enc.getBytes()

  let startTime = nowMs()
  let decoded = decodeAllEvents(bytes)
  doAssert decoded.isOk, "decodeAllEvents failed"
  let decodedEvents = decoded.get()
  let elapsed = nowMs() - startTime

  doAssert decodedEvents.len == NumEvents, "decoded count mismatch: " &
    $decodedEvents.len & " vs " & $NumEvents

  let eventsPerSec = float64(NumEvents) / (elapsed / 1000.0)
  let mbPerSec = (float64(bytes.len) / (1024.0 * 1024.0)) / (elapsed / 1000.0)

  echo "BENCH: decode_events " & $NumEvents & " events"
  echo "BENCH: decode_time_ms " & $elapsed & " ms"
  echo "BENCH: decode_input_size " & $bytes.len & " bytes"
  echo "BENCH: decode_events_per_sec " & $eventsPerSec & " events/sec"
  echo "BENCH: decode_throughput " & $mbPerSec & " MB/s"

proc bench_roundtrip() =
  ## Benchmark full roundtrip: encode + decode 100k events.
  let events = generateMixedEvents()

  let startTime = nowMs()

  var enc = SplitBinaryEncoder.init()
  for event in events:
    enc.encodeEvent(event)
  let bytes = enc.getBytes()

  let decoded = decodeAllEvents(bytes)
  doAssert decoded.isOk, "roundtrip decode failed"
  let decodedEvents = decoded.get()
  let elapsed = nowMs() - startTime

  doAssert decodedEvents.len == NumEvents

  let eventsPerSec = float64(NumEvents) / (elapsed / 1000.0)
  let mbPerSec = (float64(bytes.len) / (1024.0 * 1024.0)) / (elapsed / 1000.0)

  echo "BENCH: roundtrip_events " & $NumEvents & " events"
  echo "BENCH: roundtrip_time_ms " & $elapsed & " ms"
  echo "BENCH: roundtrip_events_per_sec " & $eventsPerSec & " events/sec"
  echo "BENCH: roundtrip_throughput " & $mbPerSec & " MB/s"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "--- Split-Binary Benchmarks ---"
bench_encode()
bench_decode()
bench_roundtrip()
echo "--- Done ---"
