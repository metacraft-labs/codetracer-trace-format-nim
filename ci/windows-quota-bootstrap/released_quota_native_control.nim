# Additive proposed native control against the normal SDK default authority.
# Its actual server process must be the authenticated just-built daemon; an
# existing different host daemon is refused without stopping or reconfiguring it.
# Compiled with the exact authenticated staged127 source and lease closure.
import std/[os, strutils, winlean, widestrs]
import runquota_core, runquota_protocol, runquota_ipc, runquota_client, runquota_codec

when not defined(windows):
  {.error: "this control requires real Windows named-pipe execution".}

static:
  doAssert RqspProtocolMajor == 2

proc pipeServer(pipe: Handle; pid: ptr DWORD): WINBOOL {.stdcall,
  dynlib: "kernel32.dll", importc: "GetNamedPipeServerProcessId".}
proc processImage(process: Handle; flags: DWORD; buffer: WideCString;
  size: ptr DWORD): WINBOOL {.stdcall, dynlib: "kernel32.dll",
  importc: "QueryFullProcessImageNameW".}

# Expected refusal texts. The daemon this control talks to is built from the
# released reprobuild 0.2.36 bundle's `share/repro/src/runquota`, which
# `declared-released-runquota-source-manifest.json` pins file by file; its
# `libs/runquota_daemon/src/runquota_daemon.nim` is runquota 83d89e58 with CRLF
# line endings. There `possible` (runquota_daemon.nim:1495) names the amount
# asked for and the budget in force in every budget refusal; runquota's
# `libs/runquota_daemon/tests/t_denial_names_the_amounts.nim` pins those texts
# with `==`, and so does this control, because a prefix match would keep
# passing if the amounts were dropped again.

proc budgetBytesText(value: uint64): string =
  ## The daemon's byte spelling (`humanBytes`, runquota
  ## `libs/runquota_daemon/src/runquota_daemon/host_config.nim:673` at
  ## 83d89e58): `N GiB` or `N MiB` only for exact multiples, otherwise the
  ## byte count, so neither figure is rounded. Restated here rather than
  ## imported so the control checks the daemon's output against the
  ## documented format instead of against the daemon's own formatter.
  const
    MiB = 1024'u64 * 1024'u64
    GiB = MiB * 1024'u64
  if value >= GiB and value mod GiB == 0: $(value div GiB) & " GiB"
  elif value >= MiB and value mod MiB == 0: $(value div MiB) & " MiB"
  else: $value & " bytes"

const InvalidRequestRefusal =
  # runquota_daemon.nim:1520 at 83d89e58 for `milliCpu(0)`, `bytes(4096)`:
  # both halves are named, CPU in milli-CPU with an `m` suffix. The request
  # carries no client estimate and no command-stats id, so
  # `effectiveResources` leaves it unchanged and these are the figures the
  # daemon reports.
  "lease request must reserve CPU and memory (requested 0m CPU and 4096 bytes)"

proc overMemoryBudgetRefusal(budget: uint64): string =
  ## runquota_daemon.nim:1542 at 83d89e58 for a request of `budget + 1` bytes
  ## on the default machine. `resourceRequest` names no machine, so the
  ## request resolves to `local` (`DefaultMachineId`); `normalizeTopology`
  ## makes `local`'s memory the daemon's `config.memoryBytes`, which is the
  ## `memoryBytes` capability the daemon advertised in HelloOk. The budget is
  ## host-dependent, so the text is computed from that capability.
  "lease request exceeds machine memory budget: local (requested " &
    budgetBytesText(budget + 1'u64) & ", budget " & budgetBytesText(budget) & ")"

proc main() =
  doAssert paramCount() == 3, "expected 3 arguments, got " & $paramCount()
  let endpoint = if paramStr(1) == "default": defaultEndpoint() else: endpointForPath(paramStr(1))
  var client = connect(endpoint, "trace-quota-bootstrap-control", "1")
  defer: client.close()
  doAssert client.daemonId != 0, "daemon advertised daemonId 0"
  doAssert client.capabilities.protocolMajor == 2,
    "protocolMajor " & $client.capabilities.protocolMajor
  doAssert client.connection.kind == endpointNamedPipe,
    "connection kind " & $client.connection.kind
  var serverPid: DWORD
  doAssert pipeServer(Handle(client.connection.pipeHandle), addr serverPid) != 0,
    "GetNamedPipeServerProcessId failed: Windows error " & $osLastError().int32
  let server = openProcess(DWORD(0x1000), WINBOOL(0), serverPid)
  doAssert server != Handle(0), "OpenProcess(" & $serverPid &
    ") failed: Windows error " & $osLastError().int32
  defer: discard closeHandle(server)
  var created, exited, kernel, user: FILETIME
  doAssert getProcessTimes(server, created, exited, kernel, user) != 0,
    "GetProcessTimes failed: Windows error " & $osLastError().int32
  let image = newWideCString(32768)
  var imageLength = DWORD(32768)
  doAssert processImage(server, 0, image, addr imageLength) != 0,
    "QueryFullProcessImageNameW failed: Windows error " & $osLastError().int32
  doAssert absolutePath($image).toLowerAscii() == absolutePath(paramStr(3)).toLowerAscii(),
    "pipe server image " & $image & " is not the expected daemon " & paramStr(3)
  echo "actual daemon pid=", serverPid, " creation=", created.dwHighDateTime,
    ":", created.dwLowDateTime, " image=", $image
  var session = client.registerSession("trace-quota-bootstrap-control", "1")
  defer: session.closeSession()
  var lease = session.requestLease(resourceRequest("trace-quota-positive", milliCpu(1), bytes(4096)))
  doAssert lease.active, "positive lease was not active after grant"
  lease.release()
  doAssert not lease.active, "positive lease still active after release"

  var denied = false
  try:
    var invalid = session.requestLease(resourceRequest("trace-quota-invalid", milliCpu(0), bytes(4096)))
    if invalid.active: invalid.release()
  except RunQuotaClientError:
    doAssert client.lastDiagnostic.code == diagDenied,
      "invalid request diagnostic code " & $client.lastDiagnostic.code
    doAssert client.lastDiagnostic.message == InvalidRequestRefusal,
      client.lastDiagnostic.message
    denied = true
  doAssert denied, "invalid request (0m CPU) was not refused"
  doAssert client.capabilities.memoryBytes.value < high(uint64),
    "memoryBytes capability is unbounded: " & $client.capabilities.memoryBytes.value
  var overBudgetDenied = false
  try:
    var impossible = session.requestLease(resourceRequest("trace-quota-over-budget",
      milliCpu(1), bytes(client.capabilities.memoryBytes.value + 1)))
    if impossible.active: impossible.release()
  except RunQuotaClientError:
    doAssert client.lastDiagnostic.code == diagDenied,
      "over-budget diagnostic code " & $client.lastDiagnostic.code
    let expected = overMemoryBudgetRefusal(client.capabilities.memoryBytes.value)
    doAssert client.lastDiagnostic.message == expected,
      client.lastDiagnostic.message & " != " & expected
    overBudgetDenied = true
  doAssert overBudgetDenied, "over-budget request was not refused"

  var raw = connectEndpoint(endpoint)
  defer: raw.close()
  raw.sendFrame(encodeFrame(rqHello, FrameFlagRequest, 1'u64,
    encodeHello(HelloMessage(clientName: "trace-incompatible-control", clientVersion: "1",
      minProtocolMajor: 1, maxProtocolMajor: 1, processId: uint64(getCurrentProcessId()),
      userId: 0, desiredCapabilities: "m1-lease"))))
  var refusal: RqspFrame
  doAssert raw.receiveFrame(refusal, 30_000),
    "no answer to the incompatible Hello within 30 s"
  doAssert refusal.header.requestId == 1'u64,
    "incompatible Hello answered with requestId " & $refusal.header.requestId
  doAssert refusal.header.messageKind == rqError,
    "incompatible Hello answered with " & $refusal.header.messageKind
  var protocolError: ProtocolErrorMessage
  doAssert decodeProtocolError(refusal.payload, protocolError),
    "undecodable ProtocolError payload (" & $refusal.payload.len & " bytes)"
  doAssert protocolError.diagnostic.code == diagUnsupportedVersion,
    "incompatible Hello diagnostic code " & $protocolError.diagnostic.code
  doAssert protocolError.diagnostic.message == "unsupported RQSP protocol version",
    protocolError.diagnostic.message

  # The missing endpoint. The launcher passes `\\.\pipe\trace-quota-missing-<guid>`,
  # which `endpointForPath` (runquota_ipc.nim:179 at 83d89e58) uses as-is, so
  # the client opens exactly that name rather than a hashed `runquota-<token>`.
  doAssert paramStr(2).startsWith(r"\\.\pipe\"),
    "missing endpoint is not a named-pipe path: " & paramStr(2)
  let missing = endpointForPath(paramStr(2))
  doAssert missing.kind == endpointNamedPipe and missing.path == paramStr(2),
    "missing endpoint resolved to " & $missing.kind & " " & missing.path
  # What the pinned client raises for it. `connect` (runquota_client.nim:205
  # at 83d89e58) opens the transport with `connectEndpoint` before any RQSP
  # byte is sent, and the named-pipe arm (runquota_ipc.nim:1715-1758) calls
  # `CreateFileW(..., OPEN_EXISTING)` once. Only ERROR_PIPE_BUSY (231) is
  # retried through `WaitNamedPipeW`; every other error goes straight to
  # `raiseLastWinError` (runquota_ipc.nim:1638-1639), which is
  #   raise newException(OSError, prefix & ": Windows error " & $osLastError().int32)
  # So the exception is a plain `OSError` (not `EndpointTrustError`, not
  # `RunQuotaClientError`), its `errorCode` field is LEFT AT ITS DEFAULT 0, and
  # the Windows code exists only in the message. Opening a pipe name with no
  # server instance fails with ERROR_FILE_NOT_FOUND (2): CreateFileW,
  # https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
  # ("Pipes"), and the code table,
  # https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-.
  # `WaitNamedPipeW` is never reached for it -- and would return at once
  # anyway when no instance exists,
  # https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-waitnamedpipea.
  # The earlier `errorCode == 2` assertion could therefore never hold against
  # this client; the code is asserted through the exact message instead.
  const MissingCode = 2 # ERROR_FILE_NOT_FOUND
  let expectedMissing = "CreateFileW failed for " & missing.path &
    ": Windows error " & $MissingCode
  var missingDenied = false
  try:
    var unexpected = connect(missing)
    unexpected.close()
  except OSError as error:
    doAssert error.name == "OSError",
      "missing endpoint raised " & $error.name & ": " & error.msg
    doAssert int(error.errorCode) == 0,
      "missing endpoint OSError carried errorCode " & $int(error.errorCode) &
      " (the pinned raiseLastWinError leaves it 0): " & error.msg
    doAssert error.msg == expectedMissing,
      "missing endpoint: got \"" & error.msg & "\", expected \"" & expectedMissing & "\""
    missingDenied = true
  doAssert missingDenied, "connecting to the missing endpoint " & missing.path & " succeeded"
  # The missing endpoint is a distinct launcher-owned random name; no shared
  # daemon is stopped or reconfigured to manufacture this refusal.
  echo "real Windows v2 handshake; admitted/released positive lease; exact invalid request refusal"

main()
