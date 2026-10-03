## Canonical C ABI compiler flags shared by script and typed Repro producer.

proc ffiCompilerFlags*(app, target: string): seq[string] =
  result = @[
    "--app:" & app,
    "--mm:arc",
    # One process-wide Nim heap, owned by no thread. With `--threads:on` every
    # thread gets its own heap, and a writer recorded on a worker thread that
    # exits before the close is freed into a dead heap (a crash in
    # `rawDealloc`). The entry points serialise on a process lock, which is
    # what makes one heap safe to share; see
    # `src/codetracer_trace_writer_ffi_runtime.c`.
    "--threads:off",
    "--noMain",
    "-d:release",
    # Keeps the Nim runtime's entry points uniquely named, so this library can
    # be linked next to another Nim-compiled artifact (the MCR emulator)
    # without a duplicate-`NimMain` link error. MUST match the
    # `codetracerTraceWriterNimMain` importc in codetracer_trace_writer_ffi.nim.
    "--nimMainPrefix:codetracerTraceWriter",
  ]
  case target
  of "msvc":
    # An MSVC consumer needs MSVC objects, CRT and `.lib` format; run from a
    # VS developer environment.
    result.add "--cc:vcc"
  of "mingw":
    # x86_64-pc-windows-gnu consumers: MinGW gcc objects.
    result.add "--cc:gcc"
  else:
    # So the archive can be linked into shared objects (a PyO3 `.so`).
    result.add "--passC:-fPIC"

