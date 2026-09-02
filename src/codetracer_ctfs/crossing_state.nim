## Exported, ABI-stable, per-thread native<->VM crossing bookkeeping (MT7 5a).
##
## Nim binding for the C translation unit `crossing_state.c` + header
## `include/codetracer_crossing_state.h`.  The C file is COMPILED INTO the
## artifacts this module links into — `libcodetracer_trace_writer.a`, `ct-print`,
## every test binary — via the `{.compile.}` pragma below, so the exported C
## symbols travel wherever the writer travels (Godot's vendored `.a`, every
## recorder) without a separate build step.
##
## The multi-stream writer mirrors its GC-heap `pendingCrossings` seq into this
## block on `beginCrossing` / `endCrossing` so a replay-time reader (MT7 5b) can
## read the current crossing / altitude from RECREATED memory in O(1)
## (nested-trace-correlation.md §1.2; Mixed-Trace-Debugging.md §6.1).  The header
## is the ABI contract; the types below mirror it exactly (declared `importc` from
## the header so the C layout — including the explicit `reserved` pad — is used
## verbatim, never re-derived on the Nim side).

{.push raises: [].}

import std/os

# The header lives in `include/`; the C file is two directories above this
# module's dir (src/codetracer_ctfs -> src -> repo root -> include).  Resolve the
# absolute include dir at compile time so `-I` works regardless of the build's
# CWD or whether Nim copies the C file into nimcache before compiling it.
const crossingIncludeDir = currentSourcePath().parentDir / ".." / ".." / "include"
{.passC: "-I" & crossingIncludeDir.}
{.compile: "crossing_state.c".}

const
  CtCrossingMaxThreads* = 256
    ## Registry capacity — mirrors `CT_CROSSING_MAX_THREADS`.
  CtCrossingMaxDepth* = 256
    ## Per-thread crossing nesting cap — mirrors `CT_CROSSING_MAX_DEPTH`.

type
  CtCrossingFrame* {.importc: "ct_crossing_frame_t",
      header: "codetracer_crossing_state.h".} = object
    ## One open crossing frame.  16 bytes: `spanId` at offset 0, `startStep`
    ## at offset 8.
    spanId* {.importc: "span_id".}: uint64
    startStep* {.importc: "start_step".}: uint64

  CtCrossingStack* {.importc: "ct_crossing_stack_t",
      header: "codetracer_crossing_state.h".} = object
    ## Per-thread crossing stack.  `sp` is the top index (-1 = empty); when
    ## `sp >= 0`, `frames[sp]` is the innermost open crossing.
    threadId* {.importc: "thread_id".}: uint64
    sp*: int32
    frames*: array[CtCrossingMaxDepth, CtCrossingFrame]

var
  ctCrossingStackCount* {.importc: "ct_crossing_stack_count",
      header: "codetracer_crossing_state.h".}: int32
    ## Number of threads that have registered a crossing stack; entries
    ## `[0, count)` of `ctCrossingStacks` are populated.
  ctCrossingStacks* {.importc: "ct_crossing_stacks",
      header: "codetracer_crossing_state.h".}: array[CtCrossingMaxThreads,
          ptr CtCrossingStack]
    ## Registry of per-thread stack pointers (NULL until a slot's store lands).

proc ctCrossingPush*(spanId: uint64, startStep: uint64)
  {.importc: "ct_crossing_push", header: "codetracer_crossing_state.h", cdecl.}
  ## First touch inits + registers this thread's stack, then pushes a frame.

proc ctCrossingPop*()
  {.importc: "ct_crossing_pop", header: "codetracer_crossing_state.h", cdecl.}
  ## Pops the innermost open crossing on this thread (guards underflow).

{.pop.}
