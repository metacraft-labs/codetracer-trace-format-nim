## CTFS container writer as a freestanding `wasm32-unknown-unknown` module.
##
## No WASI, no libc sysroot, no emscripten runtime — the target a browser or a
## Rust `cdylib` host wants.  The container is built entirely in memory
## (`container.nim` keeps it in `c.data: seq[byte]`), so no file I/O has to
## cross the wasm boundary: the host calls `ct_build`, then reads the finished
## bytes out of linear memory via `ct_ptr` / `ct_len`.
##
## Build: see `wasm/build-standalone.sh`.

import results
import codetracer_ctfs/container
import codetracer_ctfs/types

var built: seq[byte]

proc ctBuild(): int32 {.exportc: "ct_build", cdecl.} =
  ## Build a CTFS container in linear memory. Returns 0 on success.
  var c = createCtfs()

  let fr = c.addFile("hello")
  if fr.isErr: return 1
  var f = fr.get()

  let payload = "hello from freestanding wasm\n"
  var bytes = newSeq[byte](payload.len)
  for i in 0 ..< payload.len: bytes[i] = byte(payload[i])
  if c.writeToFile(f, bytes).isErr: return 2

  # >1 block, so the multi-level block mapping runs, not just the header path.
  let br = c.addFile("numbers")
  if br.isErr: return 3
  var g = br.get()
  var big = newSeq[byte](9000)
  for i in 0 ..< big.len: big[i] = byte(i and 0xff)
  if c.writeToFile(g, big).isErr: return 4

  built = c.toBytes()
  0

proc ctPtr(): pointer {.exportc: "ct_ptr", cdecl.} =
  if built.len == 0: nil else: addr built[0]

proc ctLen(): int32 {.exportc: "ct_len", cdecl.} =
  int32(built.len)

proc nimMain() {.importc: "NimMain", cdecl.}

proc ctInit() {.exportc: "ct_init", cdecl.} =
  ## `--noMain` means the host must run module-level initialisation itself.
  nimMain()

proc ctSelftest(): int32 {.exportc: "ct_selftest", cdecl.} =
  ## Build and immediately re-read the container, entirely inside the module.
  ##
  ## Exists so the freestanding build can be adjudicated by a wasm engine with
  ## no host glue at all — `wasmtime run --invoke ct_selftest` prints the
  ## return value. 0 means the container was built and both internal files read
  ## back with the expected lengths; anything else is the failing step.
  nimMain()
  let rc = ctBuild()
  if rc != 0: return 100 + rc
  if built.len == 0: return 5

  let a = readInternalFile(built, "hello")
  if a.isErr: return 6
  if a.get().len != 29: return 7          # "hello from freestanding wasm\n"

  let b = readInternalFile(built, "numbers")
  if b.isErr: return 8
  if b.get().len != 9000: return 9
  let bb = b.get()
  for i in 0 ..< bb.len:
    if bb[i] != byte(i and 0xff): return 10

  # CTFS magic, so the answer is about a real container and not an empty seq.
  if built[0] != CtfsMagic[0] or built[1] != CtfsMagic[1] or
     built[2] != CtfsMagic[2] or built[3] != CtfsMagic[3] or
     built[4] != CtfsMagic[4]: return 11
  0
