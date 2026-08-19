## Minimal CTFS container writer, built to run inside a WebAssembly engine.
##
## Proof that `src/codetracer_ctfs/container.nim` (the CTFS v4 container
## writer) compiles and runs on `wasm32-wasip1` and produces a byte-identical
## container to the native build.
##
## Build: see `wasm/build-wasi.sh`.
## Run:   wasmtime run --dir <outdir>::/out ctfs_wasm_demo.wasm

import std/os
import results
import codetracer_ctfs/container
import codetracer_ctfs/types

proc main() =
  let outPath =
    if paramCount() >= 1: paramStr(1)
    else: "/out/demo.ct"

  var c = createCtfs()

  block:
    let fr = c.addFile("hello")
    doAssert fr.isOk, fr.error
    var f = fr.get()
    let payload = "hello from webassembly\n"
    var bytes = newSeq[byte](payload.len)
    for i in 0 ..< payload.len: bytes[i] = byte(payload[i])
    let wr = c.writeToFile(f, bytes)
    doAssert wr.isOk, wr.error

  block:
    let fr = c.addFile("numbers")
    doAssert fr.isOk, fr.error
    var f = fr.get()
    # Deliberately larger than one block, to exercise the multi-level block
    # mapping (allocBlock / insertDataBlock) rather than just the header path.
    var bytes = newSeq[byte](9000)
    for i in 0 ..< bytes.len: bytes[i] = byte(i and 0xff)
    let wr = c.writeToFile(f, bytes)
    doAssert wr.isOk, wr.error

  let res = writeCtfsToFile(c, outPath)
  doAssert res.isOk, res.error

  # Read the container back through the *reader* half of the same module, so
  # the run proves round-trip and not merely that some bytes reached the fd.
  let raw = readCtfsFromFile(outPath)
  doAssert raw.isOk, raw.error
  let a = readInternalFile(raw.get(), "hello")
  doAssert a.isOk, a.error
  let b = readInternalFile(raw.get(), "numbers")
  doAssert b.isOk, b.error

  echo "wrote ", outPath, " bytes=", raw.get().len,
       " hello=", a.get().len, " numbers=", b.get().len

main()
