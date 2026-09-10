## CTFS container + seekable-zstd compressed stream, running inside a
## WebAssembly engine.
##
## The companion `ctfs_wasm_demo.nim` proves the *container* layer works on
## wasm; the container layer imports no zstd, so it links against nothing but
## wasi-libc. This one adds the layer that does: `seekable_zstd.nim` ->
## `zstd_bindings.nim` -> libzstd, which is the piece most likely to be
## assumed impossible on wasm.
##
## libzstd itself cross-compiles to `wasm32-wasip1` with no patches: it needs
## only malloc/free/memcpy/memset, all of which wasi-libc supplies. See
## `wasm/build-wasi.sh` for the exact 26-source build.
##
## Build: see `wasm/build-wasi.sh`.

import std/os
import results
import codetracer_ctfs/container
import codetracer_ctfs/types
import codetracer_ctfs/seekable_zstd

proc main() =
  let outPath =
    if paramCount() >= 1: paramStr(1)
    else: "/out/demo_zstd.ct"

  # Build a seekable-zstd stream with several frames, so the seek table (not
  # just a single-frame degenerate case) is exercised.
  var enc = newSeekableZstdEncoder(frameThreshold = 64 * 1024)
  var expected = newSeq[byte]()
  for chunk in 0 ..< 8:
    var block0 = newSeq[byte](40_000)
    for i in 0 ..< block0.len:
      block0[i] = byte((i * 7 + chunk * 13) and 0xff)
    enc.write(block0)
    expected.add(block0)
  let compressed = enc.finish()

  # Round-trip it in-process first: compression working is not the same claim
  # as decompression working, and both run here on wasm.
  let decRes = initSeekableZstdDecoder(compressed)
  doAssert decRes.isOk, decRes.error
  let dec = decRes.get()
  var roundTripped = newSeq[byte]()
  for i in 0 ..< dec.frameCount():
    let fr = dec.decompressFrame(i)
    doAssert fr.isOk, fr.error
    roundTripped.add(fr.get())
  doAssert roundTripped == expected, "seekable zstd round trip mismatch"

  # Store the compressed stream as an internal file of a CTFS container.
  var c = createCtfs()
  let fr = c.addFile("payload.zst")
  doAssert fr.isOk, fr.error
  var f = fr.get()
  let wr = c.writeToFile(f, compressed)
  doAssert wr.isOk, wr.error

  let res = writeCtfsToFile(c, outPath)
  doAssert res.isOk, res.error

  echo "wrote ", outPath,
       " raw=", expected.len,
       " compressed=", compressed.len,
       " frames=", dec.frameCount()

main()
