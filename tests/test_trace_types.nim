## Tests for codetracer_trace_types: verify types compile and basic construction works.

import codetracer_trace_types

proc test_distinct_types() {.raises: [].} =
  let p = PathId(42)
  let l = Line(100)
  let s = StepId(5)
  let t = TypeId(7)
  let v = VariableId(3)
  let f = FunctionId(1)
  let th = ThreadId(99)
  let ck = CallKey(-1)
  let pl = Place(0x1000)

  doAssert p == PathId(42)
  doAssert l == Line(100)
  doAssert s == StepId(5)
  doAssert t == TypeId(7)
  doAssert v == VariableId(3)
  doAssert f == FunctionId(1)
  doAssert th == ThreadId(99)
  doAssert ck == NoKey
  doAssert pl == Place(0x1000)
  echo "PASS: test_distinct_types"

proc test_constants() {.raises: [].} =
  doAssert NoneTypeId == TypeId(0)
  doAssert TopLevelFunctionId == FunctionId(0)
  doAssert NoKey == CallKey(-1)
  doAssert NoneValue.kind == vrkNone
  doAssert NoneValue.noneTypeId == NoneTypeId
  echo "PASS: test_constants"

proc test_type_kind_enum() {.raises: [].} =
  doAssert ord(tkSeq) == 0
  doAssert ord(tkStruct) == 6
  doAssert ord(tkInt) == 7
  doAssert ord(tkSlice) == 33
  echo "PASS: test_type_kind_enum"

proc test_event_log_kind_enum() {.raises: [].} =
  doAssert ord(elkWrite) == 0
  doAssert ord(elkError) == 11
  doAssert ord(elkEvmEvent) == 13
  echo "PASS: test_event_log_kind_enum"

proc test_value_record_construction() {.raises: [].} =
  let intVal = ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))
  doAssert intVal.intVal == 42
  doAssert intVal.intTypeId == TypeId(7)

  let floatVal = ValueRecord(kind: vrkFloat, floatVal: 3.14, floatTypeId: TypeId(8))
  doAssert floatVal.floatVal == 3.14

  let boolVal = ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(12))
  doAssert boolVal.boolVal == true

  let strVal = ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(9))
  doAssert strVal.text == "hello"

  let noneVal = ValueRecord(kind: vrkNone, noneTypeId: TypeId(0))
  doAssert noneVal.noneTypeId == TypeId(0)

  let cellVal = ValueRecord(kind: vrkCell, cellPlace: Place(0xFF))
  doAssert cellVal.cellPlace == Place(0xFF)

  let charVal = ValueRecord(kind: vrkChar, charVal: 'A', charTypeId: TypeId(11))
  doAssert charVal.charVal == 'A'

  echo "PASS: test_value_record_construction"

proc test_step_record() {.raises: [].} =
  let sr = StepRecord(pathId: PathId(1), line: Line(42))
  doAssert sr.pathId == PathId(1)
  doAssert sr.line == Line(42)
  echo "PASS: test_step_record"

proc test_function_record() {.raises: [].} =
  let fr = FunctionRecord(pathId: PathId(2), line: Line(10), name: "main")
  doAssert fr.pathId == PathId(2)
  doAssert fr.line == Line(10)
  doAssert fr.name == "main"
  echo "PASS: test_function_record"

proc test_type_record() {.raises: [].} =
  let tr = TypeRecord(
    kind: tkStruct,
    langType: "MyStruct",
    specificInfo: TypeSpecificInfo(
      kind: tsikStruct,
      fields: @[
        FieldTypeRecord(name: "x", typeId: TypeId(7)),
        FieldTypeRecord(name: "y", typeId: TypeId(8)),
      ]
    )
  )
  doAssert tr.kind == tkStruct
  doAssert tr.langType == "MyStruct"
  doAssert tr.specificInfo.kind == tsikStruct
  doAssert tr.specificInfo.fields.len == 2
  doAssert tr.specificInfo.fields[0].name == "x"
  echo "PASS: test_type_record"

proc test_trace_event_construction() {.raises: [].} =
  let stepEvent = TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(1), line: Line(42)))
  doAssert stepEvent.kind == tleStep
  doAssert stepEvent.step.pathId == PathId(1)

  let pathEvent = TraceLowLevelEvent(kind: tlePath, path: "/test/file.nim")
  doAssert pathEvent.path == "/test/file.nim"

  let dropLastStep = TraceLowLevelEvent(kind: tleDropLastStep)
  doAssert dropLastStep.kind == tleDropLastStep

  let threadStart = TraceLowLevelEvent(kind: tleThreadStart, threadStartId: ThreadId(7))
  doAssert threadStart.threadStartId == ThreadId(7)

  echo "PASS: test_trace_event_construction"

proc test_event_kind_tags() {.raises: [].} =
  doAssert ord(tleStep) == 0
  doAssert ord(tlePath) == 1
  doAssert ord(tleVariableName) == 2
  doAssert ord(tleVariable) == 3
  doAssert ord(tleType) == 4
  doAssert ord(tleValue) == 5
  doAssert ord(tleFunction) == 6
  doAssert ord(tleCall) == 7
  doAssert ord(tleReturn) == 8
  doAssert ord(tleEvent) == 9
  doAssert ord(tleAsm) == 10
  doAssert ord(tleBindVariable) == 11
  doAssert ord(tleAssignment) == 12
  doAssert ord(tleDropVariables) == 13
  doAssert ord(tleCompoundValue) == 14
  doAssert ord(tleCellValue) == 15
  doAssert ord(tleAssignCompoundItem) == 16
  doAssert ord(tleAssignCell) == 17
  doAssert ord(tleVariableCell) == 18
  doAssert ord(tleDropVariable) == 19
  doAssert ord(tleThreadStart) == 20
  doAssert ord(tleThreadExit) == 21
  doAssert ord(tleThreadSwitch) == 22
  doAssert ord(tleDropLastStep) == 23
  echo "PASS: test_event_kind_tags"

proc test_equality() =
  let a = ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))
  let b = ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))
  let c = ValueRecord(kind: vrkInt, intVal: 43, intTypeId: TypeId(7))
  doAssert a == b
  doAssert not (a == c)

  let e1 = TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(1), line: Line(10)))
  let e2 = TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(1), line: Line(10)))
  let e3 = TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(2), line: Line(10)))
  doAssert e1 == e2
  doAssert not (e1 == e3)
  echo "PASS: test_equality"

test_distinct_types()
test_constants()
test_type_kind_enum()
test_event_log_kind_enum()
test_value_record_construction()
test_step_record()
test_function_record()
test_type_record()
test_trace_event_construction()
test_event_kind_tags()
test_equality()
echo "ALL PASS: test_trace_types"
