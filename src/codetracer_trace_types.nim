{.push raises: [].}

## Trace event types for CodeTracer — Nim port of codetracer_trace_types.
##
## All types are plain objects (no `ref`). Matches the Rust
## `TraceLowLevelEvent` enum and its supporting types exactly.

import results
export results

# ---------------------------------------------------------------------------
# Distinct ID types
# ---------------------------------------------------------------------------

type
  PathId* = distinct uint64
  Line* = distinct int64
  StepId* = distinct int64
  TypeId* = distinct uint64
  VariableId* = distinct uint64
  FunctionId* = distinct uint64
  ThreadId* = distinct uint64
  CallKey* = distinct int64
  Place* = distinct int64

proc `==`*(a, b: PathId): bool {.borrow.}
proc `==`*(a, b: Line): bool {.borrow.}
proc `==`*(a, b: StepId): bool {.borrow.}
proc `==`*(a, b: TypeId): bool {.borrow.}
proc `==`*(a, b: VariableId): bool {.borrow.}
proc `==`*(a, b: FunctionId): bool {.borrow.}
proc `==`*(a, b: ThreadId): bool {.borrow.}
proc `==`*(a, b: CallKey): bool {.borrow.}
proc `==`*(a, b: Place): bool {.borrow.}

proc `$`*(v: PathId): string {.borrow.}
proc `$`*(v: Line): string {.borrow.}
proc `$`*(v: StepId): string {.borrow.}
proc `$`*(v: TypeId): string {.borrow.}
proc `$`*(v: VariableId): string {.borrow.}
proc `$`*(v: FunctionId): string {.borrow.}
proc `$`*(v: ThreadId): string {.borrow.}
proc `$`*(v: CallKey): string {.borrow.}
proc `$`*(v: Place): string {.borrow.}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  NoneTypeId*: TypeId = TypeId(0)
  TopLevelFunctionId*: FunctionId = FunctionId(0)
  NoKey*: CallKey = CallKey(-1)

# ---------------------------------------------------------------------------
# TypeKind enum  (matches Rust repr(u8) ordering)
# ---------------------------------------------------------------------------

type
  TypeKind* = enum
    tkSeq = 0
    tkSet = 1
    tkHashSet = 2
    tkOrderedSet = 3
    tkArray = 4
    tkVarargs = 5
    tkStruct = 6
    tkInt = 7
    tkFloat = 8
    tkString = 9
    tkCString = 10
    tkChar = 11
    tkBool = 12
    tkLiteral = 13
    tkRef = 14
    tkRecursion = 15
    tkRaw = 16
    tkEnum = 17
    tkEnum16 = 18
    tkEnum32 = 19
    tkC = 20
    tkTableKind = 21
    tkUnion = 22
    tkPointer = 23
    tkError = 24
    tkFunctionKind = 25
    tkTypeValue = 26
    tkTuple = 27
    tkVariant = 28
    tkHtml = 29
    tkNone = 30
    tkNonExpanded = 31
    tkAny = 32
    tkSlice = 33

# ---------------------------------------------------------------------------
# EventLogKind enum  (matches Rust repr(u8) ordering)
# ---------------------------------------------------------------------------

type
  EventLogKind* = enum
    elkWrite = 0
    elkWriteFile = 1
    elkWriteOther = 2
    elkRead = 3
    elkReadFile = 4
    elkReadOther = 5
    elkReadDir = 6
    elkOpenDir = 7
    elkCloseDir = 8
    elkSocket = 9
    elkOpen = 10
    elkError = 11
    elkTraceLogEvent = 12
    elkEvmEvent = 13

# ---------------------------------------------------------------------------
# ValueRecord (tagged union matching Rust enum)
# ---------------------------------------------------------------------------

type
  ValueRecordKind* = enum
    vrkInt
    vrkFloat
    vrkBool
    vrkString
    vrkSequence
    vrkTuple
    vrkStruct
    vrkVariant
    vrkReference
    vrkRaw
    vrkError
    vrkNone
    vrkCell
    vrkBigInt
    vrkChar

  ValueRecord* = object
    case kind*: ValueRecordKind
    of vrkInt:
      intVal*: int64
      intTypeId*: TypeId
    of vrkFloat:
      floatVal*: float64
      floatTypeId*: TypeId
    of vrkBool:
      boolVal*: bool
      boolTypeId*: TypeId
    of vrkString:
      text*: string
      strTypeId*: TypeId
    of vrkSequence:
      seqElements*: seq[ValueRecord]
      isSlice*: bool
      seqTypeId*: TypeId
    of vrkTuple:
      tupleElements*: seq[ValueRecord]
      tupleTypeId*: TypeId
    of vrkStruct:
      fieldValues*: seq[ValueRecord]
      structTypeId*: TypeId
    of vrkVariant:
      discriminator*: string
      contents*: seq[ValueRecord]  # length 1 — stands in for Box<ValueRecord>
      variantTypeId*: TypeId
    of vrkReference:
      dereferenced*: seq[ValueRecord]  # length 1
      address*: uint64
      mutable*: bool
      refTypeId*: TypeId
    of vrkRaw:
      rawStr*: string
      rawTypeId*: TypeId
    of vrkError:
      errorMsg*: string
      errorTypeId*: TypeId
    of vrkNone:
      noneTypeId*: TypeId
    of vrkCell:
      cellPlace*: Place
    of vrkBigInt:
      bigIntBytes*: seq[byte]
      negative*: bool
      bigIntTypeId*: TypeId
    of vrkChar:
      charVal*: char
      charTypeId*: TypeId

const NoneValue*: ValueRecord = ValueRecord(kind: vrkNone, noneTypeId: NoneTypeId)

# ---------------------------------------------------------------------------
# TypeSpecificInfo
# ---------------------------------------------------------------------------

type
  TypeSpecificInfoKind* = enum
    tsikNone
    tsikStruct
    tsikPointer

  FieldTypeRecord* = object
    name*: string
    typeId*: TypeId

  TypeSpecificInfo* = object
    case kind*: TypeSpecificInfoKind
    of tsikNone:
      discard
    of tsikStruct:
      fields*: seq[FieldTypeRecord]
    of tsikPointer:
      dereferenceTypeId*: TypeId

# ---------------------------------------------------------------------------
# TypeRecord
# ---------------------------------------------------------------------------

type
  TypeRecord* = object
    kind*: TypeKind
    langType*: string
    specificInfo*: TypeSpecificInfo

# ---------------------------------------------------------------------------
# Record types used by events
# ---------------------------------------------------------------------------

type
  StepRecord* = object
    pathId*: PathId
    line*: Line

  FullValueRecord* = object
    variableId*: VariableId
    value*: ValueRecord

  FunctionRecord* = object
    pathId*: PathId
    line*: Line
    name*: string

  CallRecord* = object
    functionId*: FunctionId
    args*: seq[FullValueRecord]

  ReturnRecord* = object
    returnValue*: ValueRecord

  RecordEvent* = object
    kind*: EventLogKind
    metadata*: string
    content*: string

  BindVariableRecord* = object
    variableId*: VariableId
    place*: Place

  PassBy* = enum
    pbValue = 0
    pbReference = 1

  RValueKind* = enum
    rvkSimple
    rvkCompound

  RValue* = object
    case kind*: RValueKind
    of rvkSimple:
      simpleId*: VariableId
    of rvkCompound:
      compoundIds*: seq[VariableId]

  AssignmentRecord* = object
    to*: VariableId
    passBy*: PassBy
    frm*: RValue   # `from` is a keyword in Nim

  CompoundValueRecord* = object
    place*: Place
    value*: ValueRecord

  CellValueRecord* = object
    place*: Place
    value*: ValueRecord

  AssignCompoundItemRecord* = object
    place*: Place
    index*: uint64
    itemPlace*: Place

  AssignCellRecord* = object
    place*: Place
    newValue*: ValueRecord

  VariableCellRecord* = object
    variableId*: VariableId
    place*: Place

  TraceMetadata* = object
    workdir*: string
    program*: string
    args*: seq[string]

  TickSource* = enum
    tsRdtsc = 0
    tsMonotonic = 1
    tsPerfCounter = 2

  AtomicMode* = enum
    amRelaxed = 0
    amSeqCst = 1

  McrMetaFields* = object
    tickSource*: TickSource
    totalThreads*: uint32
    atomicMode*: AtomicMode

# ---------------------------------------------------------------------------
# TraceLowLevelEvent — the main tagged union
# ---------------------------------------------------------------------------

type
  TraceLowLevelEventKind* = enum
    tleStep = 0
    tlePath = 1
    tleVariableName = 2
    tleVariable = 3
    tleType = 4
    tleValue = 5
    tleFunction = 6
    tleCall = 7
    tleReturn = 8
    tleEvent = 9
    tleAsm = 10
    tleBindVariable = 11
    tleAssignment = 12
    tleDropVariables = 13
    tleCompoundValue = 14
    tleCellValue = 15
    tleAssignCompoundItem = 16
    tleAssignCell = 17
    tleVariableCell = 18
    tleDropVariable = 19
    tleThreadStart = 20
    tleThreadExit = 21
    tleThreadSwitch = 22
    tleDropLastStep = 23

  TraceLowLevelEvent* = object
    case kind*: TraceLowLevelEventKind
    of tleStep:
      step*: StepRecord
    of tlePath:
      path*: string
    of tleVariableName:
      varName*: string
    of tleVariable:
      variable*: string
    of tleType:
      typeRecord*: TypeRecord
    of tleValue:
      fullValue*: FullValueRecord
    of tleFunction:
      functionRecord*: FunctionRecord
    of tleCall:
      callRecord*: CallRecord
    of tleReturn:
      returnRecord*: ReturnRecord
    of tleEvent:
      recordEvent*: RecordEvent
    of tleAsm:
      asmLines*: seq[string]
    of tleBindVariable:
      bindVar*: BindVariableRecord
    of tleAssignment:
      assignment*: AssignmentRecord
    of tleDropVariables:
      dropVarIds*: seq[VariableId]
    of tleCompoundValue:
      compoundValue*: CompoundValueRecord
    of tleCellValue:
      cellValue*: CellValueRecord
    of tleAssignCompoundItem:
      assignCompoundItem*: AssignCompoundItemRecord
    of tleAssignCell:
      assignCell*: AssignCellRecord
    of tleVariableCell:
      variableCell*: VariableCellRecord
    of tleDropVariable:
      dropVarId*: VariableId
    of tleThreadStart:
      threadStartId*: ThreadId
    of tleThreadExit:
      threadExitId*: ThreadId
    of tleThreadSwitch:
      threadSwitchId*: ThreadId
    of tleDropLastStep:
      discard

# ---------------------------------------------------------------------------
# Equality for compound types (needed by tests)
# ---------------------------------------------------------------------------

{.pop.}  # end of {.push raises: [].} — equality procs need flexibility

proc `==`*(a, b: ValueRecord): bool {.noSideEffect.}

proc eqSeqValueRecord(a, b: seq[ValueRecord]): bool {.noSideEffect.} =
  if a.len != b.len: return false
  for i in 0 ..< a.len:
    if not (a[i] == b[i]): return false
  true

proc `==`*(a, b: ValueRecord): bool =
  if a.kind != b.kind: return false
  case a.kind
  of vrkInt: a.intVal == b.intVal and a.intTypeId == b.intTypeId
  of vrkFloat: a.floatVal == b.floatVal and a.floatTypeId == b.floatTypeId
  of vrkBool: a.boolVal == b.boolVal and a.boolTypeId == b.boolTypeId
  of vrkString: a.text == b.text and a.strTypeId == b.strTypeId
  of vrkSequence: eqSeqValueRecord(a.seqElements, b.seqElements) and a.isSlice == b.isSlice and a.seqTypeId == b.seqTypeId
  of vrkTuple: eqSeqValueRecord(a.tupleElements, b.tupleElements) and a.tupleTypeId == b.tupleTypeId
  of vrkStruct: eqSeqValueRecord(a.fieldValues, b.fieldValues) and a.structTypeId == b.structTypeId
  of vrkVariant: a.discriminator == b.discriminator and eqSeqValueRecord(a.contents, b.contents) and a.variantTypeId == b.variantTypeId
  of vrkReference: eqSeqValueRecord(a.dereferenced, b.dereferenced) and a.address == b.address and a.mutable == b.mutable and a.refTypeId == b.refTypeId
  of vrkRaw: a.rawStr == b.rawStr and a.rawTypeId == b.rawTypeId
  of vrkError: a.errorMsg == b.errorMsg and a.errorTypeId == b.errorTypeId
  of vrkNone: a.noneTypeId == b.noneTypeId
  of vrkCell: a.cellPlace == b.cellPlace
  of vrkBigInt: a.bigIntBytes == b.bigIntBytes and a.negative == b.negative and a.bigIntTypeId == b.bigIntTypeId
  of vrkChar: a.charVal == b.charVal and a.charTypeId == b.charTypeId

proc `==`*(a, b: FieldTypeRecord): bool =
  a.name == b.name and a.typeId == b.typeId

proc eqSeqFieldTypeRecord(a, b: seq[FieldTypeRecord]): bool =
  if a.len != b.len: return false
  for i in 0 ..< a.len:
    if not (a[i] == b[i]): return false
  true

proc `==`*(a, b: TypeSpecificInfo): bool =
  if a.kind != b.kind: return false
  case a.kind
  of tsikNone: true
  of tsikStruct: eqSeqFieldTypeRecord(a.fields, b.fields)
  of tsikPointer: a.dereferenceTypeId == b.dereferenceTypeId

proc `==`*(a, b: TypeRecord): bool =
  a.kind == b.kind and a.langType == b.langType and a.specificInfo == b.specificInfo

proc `==`*(a, b: StepRecord): bool =
  a.pathId == b.pathId and a.line == b.line

proc `==`*(a, b: FullValueRecord): bool =
  a.variableId == b.variableId and a.value == b.value

proc eqSeqFullValueRecord(a, b: seq[FullValueRecord]): bool =
  if a.len != b.len: return false
  for i in 0 ..< a.len:
    if not (a[i] == b[i]): return false
  true

proc `==`*(a, b: FunctionRecord): bool =
  a.pathId == b.pathId and a.line == b.line and a.name == b.name

proc `==`*(a, b: CallRecord): bool =
  a.functionId == b.functionId and eqSeqFullValueRecord(a.args, b.args)

proc `==`*(a, b: ReturnRecord): bool =
  a.returnValue == b.returnValue

proc `==`*(a, b: RecordEvent): bool =
  a.kind == b.kind and a.metadata == b.metadata and a.content == b.content

proc `==`*(a, b: BindVariableRecord): bool =
  a.variableId == b.variableId and a.place == b.place

proc eqSeqVariableId(a, b: seq[VariableId]): bool =
  if a.len != b.len: return false
  for i in 0 ..< a.len:
    if not (a[i] == b[i]): return false
  true

proc `==`*(a, b: RValue): bool =
  if a.kind != b.kind: return false
  case a.kind
  of rvkSimple: a.simpleId == b.simpleId
  of rvkCompound: eqSeqVariableId(a.compoundIds, b.compoundIds)

proc `==`*(a, b: AssignmentRecord): bool =
  a.to == b.to and a.passBy == b.passBy and a.frm == b.frm

proc `==`*(a, b: CompoundValueRecord): bool =
  a.place == b.place and a.value == b.value

proc `==`*(a, b: CellValueRecord): bool =
  a.place == b.place and a.value == b.value

proc `==`*(a, b: AssignCompoundItemRecord): bool =
  a.place == b.place and a.index == b.index and a.itemPlace == b.itemPlace

proc `==`*(a, b: AssignCellRecord): bool =
  a.place == b.place and a.newValue == b.newValue

proc `==`*(a, b: VariableCellRecord): bool =
  a.variableId == b.variableId and a.place == b.place

proc eqSeqString(a, b: seq[string]): bool =
  if a.len != b.len: return false
  for i in 0 ..< a.len:
    if a[i] != b[i]: return false
  true

proc `==`*(a, b: TraceLowLevelEvent): bool =
  if a.kind != b.kind: return false
  case a.kind
  of tleStep: a.step == b.step
  of tlePath: a.path == b.path
  of tleVariableName: a.varName == b.varName
  of tleVariable: a.variable == b.variable
  of tleType: a.typeRecord == b.typeRecord
  of tleValue: a.fullValue == b.fullValue
  of tleFunction: a.functionRecord == b.functionRecord
  of tleCall: a.callRecord == b.callRecord
  of tleReturn: a.returnRecord == b.returnRecord
  of tleEvent: a.recordEvent == b.recordEvent
  of tleAsm: eqSeqString(a.asmLines, b.asmLines)
  of tleBindVariable: a.bindVar == b.bindVar
  of tleAssignment: a.assignment == b.assignment
  of tleDropVariables: eqSeqVariableId(a.dropVarIds, b.dropVarIds)
  of tleCompoundValue: a.compoundValue == b.compoundValue
  of tleCellValue: a.cellValue == b.cellValue
  of tleAssignCompoundItem: a.assignCompoundItem == b.assignCompoundItem
  of tleAssignCell: a.assignCell == b.assignCell
  of tleVariableCell: a.variableCell == b.variableCell
  of tleDropVariable: a.dropVarId == b.dropVarId
  of tleThreadStart: a.threadStartId == b.threadStartId
  of tleThreadExit: a.threadExitId == b.threadExitId
  of tleThreadSwitch: a.threadSwitchId == b.threadSwitchId
  of tleDropLastStep: true

{.push raises: [].}  # restore for any subsequent code
