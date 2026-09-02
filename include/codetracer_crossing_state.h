#ifndef CODETRACER_CROSSING_STATE_H
#define CODETRACER_CROSSING_STATE_H

/**
 * libcodetracer_crossing_state — exported, ABI-stable, per-thread
 * native<->VM crossing bookkeeping (MT7 piece 5a).
 *
 * WHY THIS EXISTS
 * ===============
 * The multi-stream trace writer keeps the set of OPEN native<->VM crossings
 * (Mixed-Trace-Debugging.md §3) on a GC-heap `seq[PendingCrossing]` inside the
 * `MultiStreamTraceWriter` object.  A GC-heap seq is unreachable to an
 * out-of-process reader that only has a memory image: it cannot find the seq's
 * data buffer, decode Nim's seq header, or trust its layout.
 *
 * This translation unit MIRRORS that state into a fixed, exported, C-ABI block
 * so a replay-time reader (MT7 piece 5b) can answer "which crossing / what
 * altitude am I at?" in O(1) by reading RECREATED process memory at a seek
 * point (nested-trace-correlation.md §1.2; Mixed-Trace-Debugging.md §6.1).
 *
 * It is modeled EXACTLY on the shadow stack
 * (`codetracer-native-recorder/ct_instrument/src/ct_instrument/shadow_stack/
 * ct_shadow_stack.c` + `.h`): a `__thread` current pointer plus a global
 * registry array of per-thread stack pointers, each thread claiming a slot with
 * a lock-free atomic on first touch, so an out-of-process reader enumerates all
 * threads via `ct_crossing_stacks[0 .. ct_crossing_stack_count)` and reads each
 * thread's stack directly.
 *
 * DETERMINISM
 * ===========
 * `ct_crossing_push` / `ct_crossing_pop` are pure in-memory stores: no syscalls,
 * no captured input, no I/O.  They re-execute identically on record and replay
 * (the same discipline as `ct_shadow_stack.c`, whose writes re-run on replay and
 * reconstruct the structure), so a memory reader on the replay path sees exactly
 * the values recording produced.
 *
 * THIS HEADER IS THE ABI CONTRACT.  MT7-5b resolves the symbols below by Mach-O
 * / ELF symbol lookup and decodes the structs by the field offsets documented
 * here.  Do not reorder fields or change the sizes without bumping the reader.
 */

#include <stdint.h>

/* Registry capacity — number of OS threads that can register a crossing stack.
 * Matches the shadow stack's `CT_SHADOW_STACK_MAX_THREADS` (256). */
#define CT_CROSSING_MAX_THREADS 256

/* Maximum crossing nesting depth per thread.  VM frame nesting is shallow (a
 * host enters the VM, the VM may re-enter the host which re-enters the VM, …),
 * so 256 is comfortably deep; `ct_crossing_push` SATURATES here rather than
 * overflowing the frames array. */
#define CT_CROSSING_MAX_DEPTH 256

/**
 * One open crossing frame.  16 bytes; 8-byte aligned.
 *
 *   offset 0  uint64_t span_id     — the 1-based span_id the writer minted for
 *                                     this crossing (`beginCrossing`).
 *   offset 8  uint64_t start_step  — materialized step index snapshotted at
 *                                     `beginCrossing` (the first step that runs
 *                                     inside the crossing).
 */
typedef struct {
    uint64_t span_id;
    uint64_t start_step;
} ct_crossing_frame_t;

/**
 * Per-thread crossing stack.  Fixed-size, position-independent, 8-byte aligned.
 *
 *   offset 0  uint64_t thread_id — this thread's slot index in
 *                                  `ct_crossing_stacks[]`.
 *   offset 8  int32_t  sp        — index of the top (innermost open) frame;
 *                                  -1 means EMPTY (no crossing → native
 *                                  altitude).  `frames[sp]` is the innermost
 *                                  open crossing when `sp >= 0`.
 *   offset 12 int32_t  reserved  — explicit padding so `frames` lands on its
 *                                  natural 8-byte boundary; the layout carries
 *                                  no implicit padding for the reader to guess.
 *   offset 16 ct_crossing_frame_t frames[CT_CROSSING_MAX_DEPTH]
 *
 * Total size = 16 + 16 * CT_CROSSING_MAX_DEPTH bytes.
 */
typedef struct {
    uint64_t thread_id;
    int32_t  sp;
    int32_t  reserved;
    ct_crossing_frame_t frames[CT_CROSSING_MAX_DEPTH];
} ct_crossing_stack_t;

/**
 * Global registry.  The reader reads these two symbols from process memory to
 * enumerate every thread's crossing stack:
 *
 *   ct_crossing_stack_count : number of threads that have registered (the atomic
 *                             high-water mark; entries [0, count) are populated).
 *   ct_crossing_stacks[i]   : pointer to thread i's stack, or NULL if the slot's
 *                             store has not landed yet.
 */
extern int32_t ct_crossing_stack_count;
extern ct_crossing_stack_t* ct_crossing_stacks[CT_CROSSING_MAX_THREADS];

/**
 * Convenience current-stack pointer for the calling thread — the reader can use
 * this symbol's presence to detect that crossing bookkeeping is compiled in, and
 * (given a thread's TLS base) to read that thread's current stack without the
 * registry.  Mirrors `ct_shadow_sp`.
 */
extern __thread ct_crossing_stack_t* ct_crossing_sp;

/**
 * Open a crossing on the calling thread: first touch initializes this thread's
 * stack (sp = -1) and registers it into `ct_crossing_stacks[]` via a lock-free
 * atomic slot claim, then pushes a frame carrying (span_id, start_step).
 * Saturates at CT_CROSSING_MAX_DEPTH rather than overflowing.
 */
void ct_crossing_push(uint64_t span_id, uint64_t start_step);

/**
 * Close the innermost open crossing on the calling thread (sp--).  Guards
 * against underflow (a pop with sp < 0 is a no-op).
 */
void ct_crossing_pop(void);

#endif /* CODETRACER_CROSSING_STATE_H */
