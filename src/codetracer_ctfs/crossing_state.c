/**
 * libcodetracer_crossing_state — implementation (MT7 piece 5a).
 *
 * Exported, ABI-stable, per-thread native<->VM crossing bookkeeping, mirrored
 * from the trace writer's GC-heap `pendingCrossings` seq so an out-of-process
 * reader can read the current crossing / altitude from RECREATED memory in O(1)
 * (nested-trace-correlation.md §1.2; Mixed-Trace-Debugging.md §6.1).
 *
 * Modeled EXACTLY on ct_shadow_stack.c: a `__thread` current pointer + a global
 * registry array populated by a lock-free atomic slot claim on first touch.
 *
 * These functions are pure in-memory stores — no syscalls, no I/O, no captured
 * input — so they re-execute identically on record and replay.  They carry
 * `no_instrument_function` so that, should this TU ever be compiled with
 * -finstrument-functions, the profiler callbacks do not recurse through them.
 */

#include "codetracer_crossing_state.h"
#include <string.h>

#if defined(_MSC_VER)
#include <windows.h>
#endif

#if defined(__GNUC__)
#define CT_CROSSING_NOINSTR __attribute__((no_instrument_function))
#else
#define CT_CROSSING_NOINSTR
#endif

/* ─── Global registry ───────────────────────────────────────────────── */

int32_t ct_crossing_stack_count = 0;
ct_crossing_stack_t* ct_crossing_stacks[CT_CROSSING_MAX_THREADS] = {0};

/* ─── Per-thread TLS ────────────────────────────────────────────────── */

CT_CROSSING_THREAD_LOCAL ct_crossing_stack_t* ct_crossing_sp = 0;

/* Backing storage for this thread's stack; one per thread on first use. */
static CT_CROSSING_THREAD_LOCAL ct_crossing_stack_t tls_stack;
static CT_CROSSING_THREAD_LOCAL int tls_initialized = 0;

/* ─── Thread initialization ─────────────────────────────────────────── */

CT_CROSSING_NOINSTR
static void ct_crossing_init_thread(void) {
    if (tls_initialized)
        return;
    tls_initialized = 1;

    memset(&tls_stack, 0, sizeof(tls_stack));
    tls_stack.sp = -1;

    /* Register in the global array (lock-free: atomic increment claims a slot,
     * release store publishes the pointer). */
    int idx;
#if defined(_MSC_VER)
    idx = (int)InterlockedIncrement((volatile LONG*)&ct_crossing_stack_count) - 1;
#else
    idx = __atomic_fetch_add(&ct_crossing_stack_count, 1, __ATOMIC_SEQ_CST);
#endif
    if (idx < CT_CROSSING_MAX_THREADS) {
        tls_stack.thread_id = (uint64_t)idx;
#if defined(_MSC_VER)
        InterlockedExchangePointer(
            (void* volatile*)&ct_crossing_stacks[idx], &tls_stack);
#else
        __atomic_store_n(&ct_crossing_stacks[idx], &tls_stack, __ATOMIC_RELEASE);
#endif
    }

    ct_crossing_sp = &tls_stack;
}

/* ─── Push / pop ────────────────────────────────────────────────────── */

CT_CROSSING_NOINSTR
void ct_crossing_push(uint64_t span_id, uint64_t start_step) {
    if (!tls_initialized)
        ct_crossing_init_thread();

    ct_crossing_stack_t* stack = ct_crossing_sp;
    if (!stack)
        return;

    /* Saturate at capacity rather than overflow the frames array. */
    if (stack->sp + 1 >= CT_CROSSING_MAX_DEPTH)
        return;

    stack->sp++;
    stack->frames[stack->sp].span_id = span_id;
    stack->frames[stack->sp].start_step = start_step;
}

CT_CROSSING_NOINSTR
void ct_crossing_pop(void) {
    ct_crossing_stack_t* stack = ct_crossing_sp;
    if (!stack)
        return;
    if (stack->sp < 0)
        return; /* guard underflow */

    stack->sp--;
}
