## Tests for split-binary event encoding/decoding.

import codetracer_trace_writer/split_binary

# ---------------------------------------------------------------------------
# Helper: encode then decode a single event and verify roundtrip
# ---------------------------------------------------------------------------

template getError(r: Result[TraceLowLevelEvent, string]): string =
  if r.isErr: r.unsafeError else: ""

template getValue(r: Result[TraceLowLevelEvent, string]): TraceLowLevelEvent =
  r.unsafeGet

template getErrorSeq(r: Result[seq[TraceLowLevelEvent], string]): string =
  if r.isErr: r.unsafeError else: ""

template getValueSeq(r: Result[seq[TraceLowLevelEvent], string]): seq[TraceLowLevelEvent] =
  r.unsafeGet

proc roundtrip(event: TraceLowLevelEvent): TraceLowLevelEvent =
  var enc = SplitBinaryEncoder.init()
  enc.encodeEvent(event)
  let bytes = enc.getBytes()
  var pos = 0
  let decoded = decodeEvent(bytes, pos)
  doAssert decoded.isOk, "decode failed: " & decoded.getError
  doAssert pos == bytes.len, "did not consume all bytes"
  decoded.getValue

proc assertRoundtrip(event: TraceLowLevelEvent, label: string) =
  let decoded = roundtrip(event)
  doAssert decoded == event, "roundtrip mismatch for " & label

# ---------------------------------------------------------------------------
# Test: tag numbers match spec
# ---------------------------------------------------------------------------

proc test_tag_numbers()  =
  # Verify the first byte (tag) matches the spec for each variant
  var enc = SplitBinaryEncoder.init()

  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(0))))
  doAssert enc.getBytes()[0] == 0

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tlePath, path: ""))
  doAssert enc.getBytes()[0] == 1

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleVariableName, varName: ""))
  doAssert enc.getBytes()[0] == 2

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleVariable, variable: ""))
  doAssert enc.getBytes()[0] == 3

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkInt, langType: "", specificInfo: TypeSpecificInfo(kind: tsikNone))))
  doAssert enc.getBytes()[0] == 4

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(0), value: NoneValue)))
  doAssert enc.getBytes()[0] == 5

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(0), name: "")))
  doAssert enc.getBytes()[0] == 6

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(0), args: @[])))
  doAssert enc.getBytes()[0] == 7

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(returnValue: NoneValue)))
  doAssert enc.getBytes()[0] == 8

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleEvent,
    recordEvent: RecordEvent(kind: elkWrite, metadata: "", content: "")))
  doAssert enc.getBytes()[0] == 9

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleAsm, asmLines: @[]))
  doAssert enc.getBytes()[0] == 10

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(0), place: Place(0))))
  doAssert enc.getBytes()[0] == 11

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleAssignment,
    assignment: AssignmentRecord(to: VariableId(0), passBy: pbValue,
      frm: RValue(kind: rvkSimple, simpleId: VariableId(0)))))
  doAssert enc.getBytes()[0] == 12

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropVariables, dropVarIds: @[]))
  doAssert enc.getBytes()[0] == 13

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCompoundValue,
    compoundValue: CompoundValueRecord(place: Place(0), value: NoneValue)))
  doAssert enc.getBytes()[0] == 14

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleCellValue,
    cellValue: CellValueRecord(place: Place(0), value: NoneValue)))
  doAssert enc.getBytes()[0] == 15

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleAssignCompoundItem,
    assignCompoundItem: AssignCompoundItemRecord(place: Place(0), index: 0, itemPlace: Place(0))))
  doAssert enc.getBytes()[0] == 16

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleAssignCell,
    assignCell: AssignCellRecord(place: Place(0), newValue: NoneValue)))
  doAssert enc.getBytes()[0] == 17

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleVariableCell,
    variableCell: VariableCellRecord(variableId: VariableId(0), place: Place(0))))
  doAssert enc.getBytes()[0] == 18

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropVariable, dropVarId: VariableId(0)))
  doAssert enc.getBytes()[0] == 19

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadStart, threadStartId: ThreadId(0)))
  doAssert enc.getBytes()[0] == 20

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadExit, threadExitId: ThreadId(0)))
  doAssert enc.getBytes()[0] == 21

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleThreadSwitch, threadSwitchId: ThreadId(0)))
  doAssert enc.getBytes()[0] == 22

  enc.clear()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  doAssert enc.getBytes()[0] == 23

  echo "PASS: test_tag_numbers"

# ---------------------------------------------------------------------------
# Test: roundtrip each event type
# ---------------------------------------------------------------------------

proc test_roundtrip_step()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(42), line: Line(100))), "Step")
  echo "PASS: test_roundtrip_step"

proc test_roundtrip_path()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tlePath,
    path: "/home/user/project/main.rs"), "Path")
  echo "PASS: test_roundtrip_path"

proc test_roundtrip_variable_name()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleVariableName,
    varName: "my_variable"), "VariableName")
  echo "PASS: test_roundtrip_variable_name"

proc test_roundtrip_variable()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleVariable,
    variable: "x"), "Variable")
  echo "PASS: test_roundtrip_variable"

proc test_roundtrip_type()  =
  # None specific info
  assertRoundtrip(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkInt, langType: "i32",
      specificInfo: TypeSpecificInfo(kind: tsikNone))), "Type(None)")

  # Struct specific info
  assertRoundtrip(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkStruct, langType: "Point",
      specificInfo: TypeSpecificInfo(kind: tsikStruct,
        fields: @[
          FieldTypeRecord(name: "x", typeId: TypeId(7)),
          FieldTypeRecord(name: "y", typeId: TypeId(8)),
        ]))), "Type(Struct)")

  # Pointer specific info
  assertRoundtrip(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkPointer, langType: "*i32",
      specificInfo: TypeSpecificInfo(kind: tsikPointer,
        dereferenceTypeId: TypeId(7)))), "Type(Pointer)")

  echo "PASS: test_roundtrip_type"

proc test_roundtrip_value()  =
  # Int value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(5),
      value: ValueRecord(kind: vrkInt, intVal: -42, intTypeId: TypeId(7)))),
    "Value(Int)")

  # String value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(3),
      value: ValueRecord(kind: vrkString, text: "hello world", strTypeId: TypeId(9)))),
    "Value(String)")

  # Bool value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkBool, boolVal: true, boolTypeId: TypeId(12)))),
    "Value(Bool)")

  # Float value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(2),
      value: ValueRecord(kind: vrkFloat, floatVal: 3.14, floatTypeId: TypeId(8)))),
    "Value(Float)")

  # None value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(0),
      value: NoneValue)), "Value(None)")

  echo "PASS: test_roundtrip_value"

proc test_roundtrip_function()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(2), line: Line(10), name: "main")),
    "Function")
  echo "PASS: test_roundtrip_function"

proc test_roundtrip_call()  =
  # Empty args
  assertRoundtrip(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(1), args: @[])),
    "Call(empty)")

  # With args
  assertRoundtrip(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(3), args: @[
      FullValueRecord(variableId: VariableId(1),
        value: ValueRecord(kind: vrkInt, intVal: 10, intTypeId: TypeId(7))),
      FullValueRecord(variableId: VariableId(2),
        value: ValueRecord(kind: vrkString, text: "arg", strTypeId: TypeId(9))),
    ])), "Call(with args)")
  echo "PASS: test_roundtrip_call"

proc test_roundtrip_return()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(kind: vrkInt, intVal: 0, intTypeId: TypeId(7)))),
    "Return")
  echo "PASS: test_roundtrip_return"

proc test_roundtrip_event()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleEvent,
    recordEvent: RecordEvent(kind: elkWrite, metadata: "stdout", content: "Hello\n")),
    "Event")
  echo "PASS: test_roundtrip_event"

proc test_roundtrip_asm()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleAsm,
    asmLines: @["mov eax, 1", "ret"]), "Asm")
  echo "PASS: test_roundtrip_asm"

proc test_roundtrip_bind_variable()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(5), place: Place(0x1000))),
    "BindVariable")
  echo "PASS: test_roundtrip_bind_variable"

proc test_roundtrip_assignment()  =
  # Simple RValue
  assertRoundtrip(TraceLowLevelEvent(kind: tleAssignment,
    assignment: AssignmentRecord(to: VariableId(1), passBy: pbValue,
      frm: RValue(kind: rvkSimple, simpleId: VariableId(2)))),
    "Assignment(Simple)")

  # Compound RValue
  assertRoundtrip(TraceLowLevelEvent(kind: tleAssignment,
    assignment: AssignmentRecord(to: VariableId(3), passBy: pbReference,
      frm: RValue(kind: rvkCompound, compoundIds: @[VariableId(4), VariableId(5)]))),
    "Assignment(Compound)")
  echo "PASS: test_roundtrip_assignment"

proc test_roundtrip_drop_variables()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleDropVariables,
    dropVarIds: @[VariableId(1), VariableId(2), VariableId(3)]),
    "DropVariables")
  echo "PASS: test_roundtrip_drop_variables"

proc test_roundtrip_compound_value()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleCompoundValue,
    compoundValue: CompoundValueRecord(place: Place(0x2000),
      value: ValueRecord(kind: vrkInt, intVal: 99, intTypeId: TypeId(7)))),
    "CompoundValue")
  echo "PASS: test_roundtrip_compound_value"

proc test_roundtrip_cell_value()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleCellValue,
    cellValue: CellValueRecord(place: Place(0x3000),
      value: ValueRecord(kind: vrkString, text: "cell", strTypeId: TypeId(9)))),
    "CellValue")
  echo "PASS: test_roundtrip_cell_value"

proc test_roundtrip_assign_compound_item()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleAssignCompoundItem,
    assignCompoundItem: AssignCompoundItemRecord(
      place: Place(1), index: 2, itemPlace: Place(3))),
    "AssignCompoundItem")
  echo "PASS: test_roundtrip_assign_compound_item"

proc test_roundtrip_assign_cell()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleAssignCell,
    assignCell: AssignCellRecord(place: Place(0x4000),
      newValue: ValueRecord(kind: vrkBool, boolVal: false, boolTypeId: TypeId(12)))),
    "AssignCell")
  echo "PASS: test_roundtrip_assign_cell"

proc test_roundtrip_variable_cell()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleVariableCell,
    variableCell: VariableCellRecord(variableId: VariableId(7), place: Place(8))),
    "VariableCell")
  echo "PASS: test_roundtrip_variable_cell"

proc test_roundtrip_drop_variable()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleDropVariable,
    dropVarId: VariableId(99)), "DropVariable")
  echo "PASS: test_roundtrip_drop_variable"

proc test_roundtrip_thread_events()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleThreadStart,
    threadStartId: ThreadId(1)), "ThreadStart")
  assertRoundtrip(TraceLowLevelEvent(kind: tleThreadExit,
    threadExitId: ThreadId(2)), "ThreadExit")
  assertRoundtrip(TraceLowLevelEvent(kind: tleThreadSwitch,
    threadSwitchId: ThreadId(3)), "ThreadSwitch")
  echo "PASS: test_roundtrip_thread_events"

proc test_roundtrip_drop_last_step()  =
  assertRoundtrip(TraceLowLevelEvent(kind: tleDropLastStep), "DropLastStep")
  echo "PASS: test_roundtrip_drop_last_step"

# ---------------------------------------------------------------------------
# Test: mixed sequence of 20+ events
# ---------------------------------------------------------------------------

proc test_mixed_sequence()  =
  var events: seq[TraceLowLevelEvent]

  events.add(TraceLowLevelEvent(kind: tlePath, path: "/src/main.rs"))
  events.add(TraceLowLevelEvent(kind: tlePath, path: "/src/lib.rs"))
  events.add(TraceLowLevelEvent(kind: tleVariableName, varName: "x"))
  events.add(TraceLowLevelEvent(kind: tleVariableName, varName: "y"))
  events.add(TraceLowLevelEvent(kind: tleVariable, variable: "z"))
  events.add(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkInt, langType: "i64",
      specificInfo: TypeSpecificInfo(kind: tsikNone))))
  events.add(TraceLowLevelEvent(kind: tleType,
    typeRecord: TypeRecord(kind: tkStruct, langType: "Point",
      specificInfo: TypeSpecificInfo(kind: tsikStruct,
        fields: @[FieldTypeRecord(name: "x", typeId: TypeId(0)),
                  FieldTypeRecord(name: "y", typeId: TypeId(0))]))))
  events.add(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(1), name: "main")))
  events.add(TraceLowLevelEvent(kind: tleFunction,
    functionRecord: FunctionRecord(pathId: PathId(0), line: Line(10), name: "foo")))
  events.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(1))))
  events.add(TraceLowLevelEvent(kind: tleCall,
    callRecord: codetracer_trace_types.CallRecord(functionId: FunctionId(1), args: @[
      FullValueRecord(variableId: VariableId(0),
        value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(0)))])))
  events.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(10))))
  events.add(TraceLowLevelEvent(kind: tleBindVariable,
    bindVar: BindVariableRecord(variableId: VariableId(0), place: Place(0x100))))
  events.add(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(0),
      value: ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(0)))))
  events.add(TraceLowLevelEvent(kind: tleAssignment,
    assignment: AssignmentRecord(to: VariableId(1), passBy: pbValue,
      frm: RValue(kind: rvkSimple, simpleId: VariableId(0)))))
  events.add(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(0), line: Line(11))))
  events.add(TraceLowLevelEvent(kind: tleEvent,
    recordEvent: RecordEvent(kind: elkWrite, metadata: "stdout", content: "Hello World\n")))
  events.add(TraceLowLevelEvent(kind: tleReturn,
    returnRecord: ReturnRecord(
      returnValue: ValueRecord(kind: vrkInt, intVal: 0, intTypeId: TypeId(0)))))
  events.add(TraceLowLevelEvent(kind: tleDropVariables,
    dropVarIds: @[VariableId(0), VariableId(1)]))
  events.add(TraceLowLevelEvent(kind: tleThreadStart, threadStartId: ThreadId(1)))
  events.add(TraceLowLevelEvent(kind: tleThreadSwitch, threadSwitchId: ThreadId(1)))
  events.add(TraceLowLevelEvent(kind: tleAsm, asmLines: @["nop", "ret"]))
  events.add(TraceLowLevelEvent(kind: tleDropLastStep))
  events.add(TraceLowLevelEvent(kind: tleThreadExit, threadExitId: ThreadId(1)))

  doAssert events.len >= 20, "need at least 20 events"

  # Encode all
  var enc = SplitBinaryEncoder.init()
  for event in events:
    enc.encodeEvent(event)
  let bytes = enc.getBytes()

  # Decode all
  let decoded = decodeAllEvents(bytes)
  doAssert decoded.isOk, "decodeAllEvents failed: " & decoded.getErrorSeq
  let decodedEvents = decoded.getValueSeq
  doAssert decodedEvents.len == events.len,
    "event count mismatch: " & $decodedEvents.len & " vs " & $events.len

  for i in 0 ..< events.len:
    doAssert decodedEvents[i] == events[i],
      "event mismatch at index " & $i

  echo "PASS: test_mixed_sequence (" & $events.len & " events)"

# ---------------------------------------------------------------------------
# Test: string edge cases
# ---------------------------------------------------------------------------

proc test_string_edge_cases()  =
  # Empty string
  assertRoundtrip(TraceLowLevelEvent(kind: tlePath, path: ""), "empty string")

  # Unicode string
  assertRoundtrip(TraceLowLevelEvent(kind: tlePath,
    path: "/home/user/projet/fichier.rs"), "accented path")

  # Long string
  var longStr = ""
  for i in 0 ..< 10000:
    longStr.add('a')
  assertRoundtrip(TraceLowLevelEvent(kind: tlePath, path: longStr), "long string (10000)")

  # String with null bytes (legal in strings)
  assertRoundtrip(TraceLowLevelEvent(kind: tleVariableName,
    varName: "a\x00b"), "string with null byte")

  echo "PASS: test_string_edge_cases"

# ---------------------------------------------------------------------------
# Test: complex nested ValueRecord roundtrips
# ---------------------------------------------------------------------------

proc test_complex_value_records()  =
  # Sequence value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(1),
      value: ValueRecord(kind: vrkSequence,
        seqElements: @[
          ValueRecord(kind: vrkInt, intVal: 1, intTypeId: TypeId(7)),
          ValueRecord(kind: vrkInt, intVal: 2, intTypeId: TypeId(7)),
          ValueRecord(kind: vrkInt, intVal: 3, intTypeId: TypeId(7)),
        ],
        isSlice: false,
        seqTypeId: TypeId(0)))),
    "Value(Sequence)")

  # Tuple value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(2),
      value: ValueRecord(kind: vrkTuple,
        tupleElements: @[
          ValueRecord(kind: vrkInt, intVal: 10, intTypeId: TypeId(7)),
          ValueRecord(kind: vrkString, text: "hello", strTypeId: TypeId(9)),
        ],
        tupleTypeId: TypeId(27)))),
    "Value(Tuple)")

  # Struct value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(3),
      value: ValueRecord(kind: vrkStruct,
        fieldValues: @[
          ValueRecord(kind: vrkFloat, floatVal: 1.0, floatTypeId: TypeId(8)),
          ValueRecord(kind: vrkFloat, floatVal: 2.0, floatTypeId: TypeId(8)),
        ],
        structTypeId: TypeId(6)))),
    "Value(Struct)")

  # Variant value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(4),
      value: ValueRecord(kind: vrkVariant,
        discriminator: "Some",
        contents: @[ValueRecord(kind: vrkInt, intVal: 42, intTypeId: TypeId(7))],
        variantTypeId: TypeId(28)))),
    "Value(Variant)")

  # Reference value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(5),
      value: ValueRecord(kind: vrkReference,
        dereferenced: @[ValueRecord(kind: vrkInt, intVal: 99, intTypeId: TypeId(7))],
        address: 0xDEADBEEF'u64,
        mutable: true,
        refTypeId: TypeId(14)))),
    "Value(Reference)")

  # Raw value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(6),
      value: ValueRecord(kind: vrkRaw, rawStr: "<opaque>", rawTypeId: TypeId(16)))),
    "Value(Raw)")

  # Error value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(7),
      value: ValueRecord(kind: vrkError, errorMsg: "timeout", errorTypeId: TypeId(24)))),
    "Value(Error)")

  # Cell value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(8),
      value: ValueRecord(kind: vrkCell, cellPlace: Place(-1)))),
    "Value(Cell)")

  # BigInt value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(9),
      value: ValueRecord(kind: vrkBigInt,
        bigIntBytes: @[0x01'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        negative: true,
        bigIntTypeId: TypeId(7)))),
    "Value(BigInt)")

  # Char value
  assertRoundtrip(TraceLowLevelEvent(kind: tleValue,
    fullValue: FullValueRecord(variableId: VariableId(10),
      value: ValueRecord(kind: vrkChar, charVal: 'Z', charTypeId: TypeId(11)))),
    "Value(Char)")

  echo "PASS: test_complex_value_records"

# ---------------------------------------------------------------------------
# Test: encoder clear and reuse
# ---------------------------------------------------------------------------

proc test_encoder_clear()  =
  var enc = SplitBinaryEncoder.init()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  doAssert enc.getBytes().len == 1
  enc.clear()
  doAssert enc.getBytes().len == 0
  enc.encodeEvent(TraceLowLevelEvent(kind: tleDropLastStep))
  doAssert enc.getBytes().len == 1
  echo "PASS: test_encoder_clear"

# ---------------------------------------------------------------------------
# Test: unknown tag
# ---------------------------------------------------------------------------

proc test_unknown_tag()  =
  let data = @[255'u8]
  var pos = 0
  let result_val = decodeEvent(data, pos)
  doAssert result_val.isErr
  echo "PASS: test_unknown_tag"

# ---------------------------------------------------------------------------
# Test: step encoding size (tag 1 + pathId 8 + line 8 = 17 bytes)
# ---------------------------------------------------------------------------

proc test_step_encoding_size()  =
  var enc = SplitBinaryEncoder.init()
  enc.encodeEvent(TraceLowLevelEvent(kind: tleStep,
    step: StepRecord(pathId: PathId(42), line: Line(100))))
  doAssert enc.getBytes().len == 17, "Step should be 17 bytes, got " & $enc.getBytes().len
  echo "PASS: test_step_encoding_size"

# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

test_tag_numbers()
test_roundtrip_step()
test_roundtrip_path()
test_roundtrip_variable_name()
test_roundtrip_variable()
test_roundtrip_type()
test_roundtrip_value()
test_roundtrip_function()
test_roundtrip_call()
test_roundtrip_return()
test_roundtrip_event()
test_roundtrip_asm()
test_roundtrip_bind_variable()
test_roundtrip_assignment()
test_roundtrip_drop_variables()
test_roundtrip_compound_value()
test_roundtrip_cell_value()
test_roundtrip_assign_compound_item()
test_roundtrip_assign_cell()
test_roundtrip_variable_cell()
test_roundtrip_drop_variable()
test_roundtrip_thread_events()
test_roundtrip_drop_last_step()
test_mixed_sequence()
test_string_edge_cases()
test_complex_value_records()
test_encoder_clear()
test_unknown_tag()
test_step_encoding_size()
echo "ALL PASS: test_split_binary"
