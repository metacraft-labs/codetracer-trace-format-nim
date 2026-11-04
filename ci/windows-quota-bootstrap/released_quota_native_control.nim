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

proc main() =
  doAssert paramCount() == 3
  let endpoint = if paramStr(1) == "default": defaultEndpoint() else: endpointForPath(paramStr(1))
  var client = connect(endpoint, "trace-quota-bootstrap-control", "1")
  defer: client.close()
  doAssert client.daemonId != 0
  doAssert client.capabilities.protocolMajor == 2
  doAssert client.connection.kind == endpointNamedPipe
  var serverPid: DWORD
  doAssert pipeServer(Handle(client.connection.pipeHandle), addr serverPid) != 0
  let server = openProcess(DWORD(0x1000), WINBOOL(0), serverPid)
  doAssert server != Handle(0)
  defer: discard closeHandle(server)
  var created, exited, kernel, user: FILETIME
  doAssert getProcessTimes(server, created, exited, kernel, user) != 0
  let image = newWideCString(32768)
  var imageLength = DWORD(32768)
  doAssert processImage(server, 0, image, addr imageLength) != 0
  doAssert absolutePath($image).toLowerAscii() == absolutePath(paramStr(3)).toLowerAscii()
  echo "actual daemon pid=", serverPid, " creation=", created.dwHighDateTime,
    ":", created.dwLowDateTime, " image=", $image
  var session = client.registerSession("trace-quota-bootstrap-control", "1")
  defer: session.closeSession()
  var lease = session.requestLease(resourceRequest("trace-quota-positive", milliCpu(1), bytes(4096)))
  doAssert lease.active
  lease.release()
  doAssert not lease.active

  var denied = false
  try:
    var invalid = session.requestLease(resourceRequest("trace-quota-invalid", milliCpu(0), bytes(4096)))
    if invalid.active: invalid.release()
  except RunQuotaClientError:
    doAssert client.lastDiagnostic.code == diagDenied
    doAssert client.lastDiagnostic.message == "lease request must reserve CPU and memory"
    denied = true
  doAssert denied
  doAssert client.capabilities.memoryBytes.value < high(uint64)
  var overBudgetDenied = false
  try:
    var impossible = session.requestLease(resourceRequest("trace-quota-over-budget",
      milliCpu(1), bytes(client.capabilities.memoryBytes.value + 1)))
    if impossible.active: impossible.release()
  except RunQuotaClientError:
    doAssert client.lastDiagnostic.code == diagDenied
    doAssert client.lastDiagnostic.message.startsWith("lease request exceeds machine memory budget: ")
    overBudgetDenied = true
  doAssert overBudgetDenied

  var raw = connectEndpoint(endpoint)
  defer: raw.close()
  raw.sendFrame(encodeFrame(rqHello, FrameFlagRequest, 1'u64,
    encodeHello(HelloMessage(clientName: "trace-incompatible-control", clientVersion: "1",
      minProtocolMajor: 1, maxProtocolMajor: 1, processId: uint64(getCurrentProcessId()),
      userId: 0, desiredCapabilities: "m1-lease"))))
  var refusal: RqspFrame
  doAssert raw.receiveFrame(refusal, 30_000)
  doAssert refusal.header.requestId == 1'u64
  doAssert refusal.header.messageKind == rqError
  var protocolError: ProtocolErrorMessage
  doAssert decodeProtocolError(refusal.payload, protocolError)
  doAssert protocolError.diagnostic.code == diagUnsupportedVersion
  doAssert protocolError.diagnostic.message == "unsupported RQSP protocol version"

  var missingDenied = false
  try:
    var unexpected = connect(endpointForPath(paramStr(2)))
    unexpected.close()
  except OSError as error:
    doAssert int(error.errorCode) == 2
    missingDenied = true
  doAssert missingDenied
  # The missing endpoint is a distinct launcher-owned random name; no shared
  # daemon is stopped or reconfigured to manufacture this refusal.
  echo "real Windows v2 handshake; admitted/released positive lease; exact invalid request refusal"

main()
