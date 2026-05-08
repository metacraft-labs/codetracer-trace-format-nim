## Generate a multi-stream .ct fixture for ct-print golden tests.
##
## Builds a small but representative v4 (multi-stream) trace using the
## MultiStreamTraceWriter, then writes the resulting CTFS bytes to disk
## so ct-print --full / --events can be exercised against it.
##
## The fixture includes:
##   - 2 paths, 2 functions, several varnames + types
##   - Multiple steps walking through both files
##   - One outer call ("main") with a nested call ("compute")
##   - Variable values across many ValueRecord variants:
##       Int, String, Bool, Float, None, Sequence, Tuple, Struct, Variant
##   - One stdout IO event and one stderr IO event
##   - One return value
##
## Usage:  nim c -r -p:src tests/generate_ct_print_fixture.nim <output.ct>

import std/os
import results
import codetracer_trace_writer/multi_stream_writer
import codetracer_trace_writer/value_stream
import codetracer_trace_writer/io_event_stream
import codetracer_trace_writer/call_stream
import codetracer_trace_writer/cbor
import codetracer_trace_types

proc encodeValue(v: ValueRecord): seq[byte] =
  var enc = CborEncoder.init()
  enc.encodeCborValueRecord(v)
  enc.getBytes()

proc toBytesAscii(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i in 0 ..< s.len:
    result[i] = byte(s[i])

proc main() =
  let outputPath =
    if paramCount() >= 1: paramStr(1)
    else: getTempDir() / "ct_print_fixture.ct"

  var wRes = initMultiStreamWriter(outputPath, "ct_print_demo", chunkSize = 16)
  doAssert wRes.isOk, "initMultiStreamWriter: " & wRes.error
  var w = wRes.get()
  w.metadata.args = @["--demo", "fixture"]
  w.metadata.workdir = "/workspace/demo"

  # Paths
  let p0 = w.registerPath("/workspace/demo/main.py")
  doAssert p0.isOk
  let p1 = w.registerPath("/workspace/demo/util.py")
  doAssert p1.isOk

  # Functions
  let fnMain = w.registerFunction("main").get()
  let fnCompute = w.registerFunction("compute").get()

  # Varnames
  let vnX = w.registerVarname("x").get()
  let vnName = w.registerVarname("name").get()
  let vnFlag = w.registerVarname("flag").get()
  let vnPi = w.registerVarname("pi").get()
  let vnNothing = w.registerVarname("nothing").get()
  let vnNumbers = w.registerVarname("numbers").get()
  let vnPair = w.registerVarname("pair").get()
  let vnPoint = w.registerVarname("point").get()
  let vnShape = w.registerVarname("shape").get()
  let vnArg = w.registerVarname("n").get()

  # Type ids
  let tInt = w.registerType("int").get()
  let tStr = w.registerType("str").get()
  let tBool = w.registerType("bool").get()
  let tFloat = w.registerType("float").get()
  let tNone = w.registerType("None").get()
  let tList = w.registerType("List[int]").get()
  let tTuple = w.registerType("Tuple[str,int]").get()
  let tStruct = w.registerType("Point").get()
  let tVariant = w.registerType("Shape").get()

  # ----- Step 0: enter main, x = 42 -----
  var step0Vals = @[
    VariableValue(varnameId: vnX, typeId: tInt, data: encodeValue(
      ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(tInt))))
  ]
  doAssert w.registerStep(p0.get(), 1'u64, step0Vals).isOk
  doAssert w.registerCall(fnMain, @[]).isOk

  # ----- Step 1: name = "hello" -----
  var step1Vals = @[
    VariableValue(varnameId: vnName, typeId: tStr, data: encodeValue(
      ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(tStr))))
  ]
  doAssert w.registerStep(p0.get(), 2'u64, step1Vals).isOk

  # ----- Step 2: flag = true; pi = 3.14 -----
  var step2Vals = @[
    VariableValue(varnameId: vnFlag, typeId: tBool, data: encodeValue(
      ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(tBool)))),
    VariableValue(varnameId: vnPi, typeId: tFloat, data: encodeValue(
      ValueRecord(kind: vrkFloat, floatVal: 3.14, floatTypeId: TypeId(tFloat))))
  ]
  doAssert w.registerStep(p0.get(), 3'u64, step2Vals).isOk

  # ----- Step 3: nothing = None; numbers = [1,2,3] -----
  let listVal = ValueRecord(
    kind: vrkSequence,
    seqElements: @[
      ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(tInt)),
      ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(tInt)),
      ValueRecord(kind: vrkInt, intVal: 3, intTypeId: TypeId(tInt)),
    ],
    isSlice: false,
    seqTypeId: TypeId(tList))
  var step3Vals = @[
    VariableValue(varnameId: vnNothing, typeId: tNone, data: encodeValue(
      ValueRecord(kind: vrkNone, noneTypeId: TypeId(tNone)))),
    VariableValue(varnameId: vnNumbers, typeId: tList, data: encodeValue(listVal))
  ]
  doAssert w.registerStep(p0.get(), 4'u64, step3Vals).isOk

  # ----- Step 4: pair = ("answer", 42) (tuple) -----
  let tupleVal = ValueRecord(
    kind: vrkTuple,
    tupleElements: @[
      ValueRecord(kind: vrkString, text: "answer", strTypeId: TypeId(tStr)),
      ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(tInt)),
    ],
    tupleTypeId: TypeId(tTuple))
  var step4Vals = @[
    VariableValue(varnameId: vnPair, typeId: tTuple, data: encodeValue(tupleVal))
  ]
  doAssert w.registerStep(p0.get(), 5'u64, step4Vals).isOk

  # ----- Step 5: emit stdout, then enter `compute(n=7)` -----
  doAssert w.registerStep(p0.get(), 6'u64, @[]).isOk
  doAssert w.registerIOEvent(ioStdout, "computing...\n".toBytesAscii).isOk

  let argN = ValueRecord(kind: vrkInt, intVal: 7, intTypeId: TypeId(tInt))
  let argEnc = encodeValue(argN)
  let callArgs = @[CallArg(varnameId: vnArg, value: argEnc)]
  doAssert w.registerCall(fnCompute, callArgs).isOk

  # ----- Step 6 (in util.py): point = Point{x=1, y=2} (struct) -----
  let structVal = ValueRecord(
    kind: vrkStruct,
    fieldValues: @[
      ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(tInt)),
      ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(tInt)),
    ],
    structTypeId: TypeId(tStruct))
  var step6Vals = @[
    VariableValue(varnameId: vnPoint, typeId: tStruct, data: encodeValue(structVal))
  ]
  doAssert w.registerStep(p1.get(), 1'u64, step6Vals).isOk

  # ----- Step 7: shape = Circle(radius=5) (variant) -----
  let variantVal = ValueRecord(
    kind: vrkVariant,
    discriminator: "Circle",
    contents: @[ValueRecord(
      kind: vrkStruct,
      fieldValues: @[
        ValueRecord(kind: vrkInt, intVal: 5, intTypeId: TypeId(tInt)),
      ],
      structTypeId: TypeId(tStruct))],
    variantTypeId: TypeId(tVariant))
  var step7Vals = @[
    VariableValue(varnameId: vnShape, typeId: tVariant, data: encodeValue(variantVal))
  ]
  doAssert w.registerStep(p1.get(), 2'u64, step7Vals).isOk

  # ----- Step 8: stderr write, then return from `compute` with int 49 -----
  doAssert w.registerStep(p1.get(), 3'u64, @[]).isOk
  doAssert w.registerIOEvent(ioStderr, "warning: nothing\n".toBytesAscii).isOk
  let computeRet = encodeValue(
    ValueRecord(kind: vrkInt, intVal: 49, intTypeId: TypeId(tInt)))
  doAssert w.registerReturn(computeRet).isOk

  # ----- Step 9: back in main, return 0 -----
  doAssert w.registerStep(p0.get(), 7'u64, @[]).isOk
  let mainRet = encodeValue(
    ValueRecord(kind: vrkInt, intVal: 0, intTypeId: TypeId(tInt)))
  doAssert w.registerReturn(mainRet).isOk

  # ----- Finalize and write to disk -----
  doAssert w.close().isOk
  let bytes = w.toBytes()
  w.closeCtfs()

  writeFile(outputPath, cast[string](bytes))
  echo "Wrote fixture: ", outputPath, " (", bytes.len, " bytes)"

main()
