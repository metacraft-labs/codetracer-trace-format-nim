## M24a-2 cross-read driver: prove a NEW Nim-written production bundle's
## `values.dat` is read correctly by the canonical RUST `ValueStreamReader`.
##
## Steps:
##   1. Build + run the fixture generator
##      (`gen_value_stream_crossread_fixture.nim`) to write a real production
##      `.ct` bundle (SPEC-canonical values.dat/values.idx, has_value_stream
##      flag set) plus a sidecar of the decoded per-step values.
##   2. Shell out to the sibling Rust reader's integration test
##      (`codetracer-trace-format/codetracer_trace_reader/tests/nim_value_stream_crossread.rs`),
##      pointing it at the bundle + sidecar via env vars. That test opens the
##      Nim bundle with the Rust `ValueStreamReader` and asserts the decoded
##      per-step `(name_id, CBOR bytes)` pairs equal the sidecar — the
##      load-bearing byte-compatibility proof (incl. the parallel-index
##      invariant: record N ↔ step N, empty for value-less steps).
##
## The Rust repo is the sibling `../codetracer-trace-format` checkout. If it is
## absent, or `cargo` is unavailable, the test skips with a clear message.

import std/[os, osproc, strutils, strtabs, streams]

const
  FixtureGen = "tests/gen_value_stream_crossread_fixture.nim"
  RustRepoRel = "../codetracer-trace-format"
  RustTestName = "nim_value_stream_crossread"

proc run(cmd: string, args: seq[string], workdir = "",
    extraEnv: seq[(string, string)] = @[]): tuple[code: int, output: string] =
  try:
    var env = newStringTable(modeStyleInsensitive)
    for k, v in envPairs():
      env[k] = v
    for (k, v) in extraEnv:
      env[k] = v
    let p = startProcess(cmd, workingDir = workdir, args = args, env = env,
      options = {poStdErrToStdOut, poUsePath})
    let outp = p.outputStream.readAll()
    let code = p.waitForExit()
    p.close()
    (code, outp)
  except OSError, IOError, Exception:
    (-1, "process error: " & getCurrentExceptionMsg())

proc which(exe: string): bool =
  findExe(exe).len > 0

proc main() =
  let repoRoot = getCurrentDir()
  let rustRepo = absolutePath(RustRepoRel)
  if not dirExists(rustRepo):
    echo "SKIP: sibling Rust repo not found at " & rustRepo &
      " (cross-read needs both checkouts)"
    return
  if not which("nim"):
    echo "SKIP: nim not on PATH"
    return
  let haveDirenv = which("direnv")
  let haveCargo = which("cargo")
  if not haveCargo and not haveDirenv:
    echo "SKIP: neither cargo nor direnv on PATH (cross-read needs the Rust toolchain)"
    return

  # 1. Build the fixture generator.
  let tmp = getTempDir() / ("ct_value_crossread_" & $getCurrentProcessId())
  try:
    createDir(tmp)
  except OSError, IOError:
    echo "SKIP: cannot create temp dir " & tmp
    return
  let genBin = tmp / "gen"
  var nimArgs = @["c", "-d:release", "-p:src", "--hints:off", "-o:" & genBin]
  let incRes = run("pkg-config", @["--variable=includedir", "libzstd"])
  let libRes = run("pkg-config", @["--variable=libdir", "libzstd"])
  if incRes.code == 0 and incRes.output.strip().len > 0:
    nimArgs.add("--passC:-I" & incRes.output.strip())
  if libRes.code == 0 and libRes.output.strip().len > 0:
    nimArgs.add("--passL:-L" & libRes.output.strip())
  nimArgs.add(FixtureGen)
  let buildRes = run("nim", nimArgs, workdir = repoRoot)
  doAssert buildRes.code == 0,
    "fixture generator build failed:\n" & buildRes.output

  # 2. Produce the bundle + sidecar.
  let bundle = tmp / "trace.ct"
  let genRes = run(genBin, @[bundle], workdir = repoRoot)
  doAssert genRes.code == 0, "fixture generation failed:\n" & genRes.output
  doAssert fileExists(bundle), "fixture bundle not written: " & bundle
  let sidecar = bundle & ".values.txt"
  doAssert fileExists(sidecar), "fixture sidecar not written: " & sidecar

  # 3. Run the Rust cross-read test against the Nim bundle.
  let cargoArgs = @["test", "-p", "codetracer_trace_reader",
    "--test", RustTestName, "--", "--nocapture"]
  let crossEnv = @[
    ("CT_NIM_VALUE_FIXTURE", bundle),
    ("CT_NIM_VALUE_FIXTURE_VALUES", sidecar),
  ]
  let rustRes =
    if haveCargo:
      run("cargo", cargoArgs, workdir = rustRepo, extraEnv = crossEnv)
    else:
      run("direnv", @["exec", rustRepo, "cargo"] & cargoArgs,
        workdir = rustRepo, extraEnv = crossEnv)
  doAssert rustRes.code == 0,
    "Rust ValueStreamReader failed to cross-read the Nim values.dat:\n" &
    rustRes.output
  doAssert rustRes.output.contains("1 passed"),
    "Rust cross-read test did not report a pass:\n" & rustRes.output

  try:
    removeDir(tmp)
  except OSError, IOError:
    discard

  echo "PASS: test_nim_value_stream_crossread (Nim values.dat read by Rust reader)"

main()
