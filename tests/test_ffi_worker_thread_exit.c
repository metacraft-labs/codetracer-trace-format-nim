/*
 * A C host writes a trace from a worker thread, lets the worker EXIT, and
 * then closes the writer from the main thread.
 *
 * This is the shape of a multithreaded host that closes the trace at process
 * exit (the Godot fork's `gdscript_ct_close`, an atexit handler): the memory
 * the writer allocated while the worker was recording is freed by the main
 * thread after the worker is gone. Nim's own allocator is thread-local; with
 * it, that free lands on a dead thread's heap and the process crashes in
 * `rawDealloc` (SIGSEGV). The library build tasks therefore compile with
 * `-d:useMalloc`, and this test is what holds them to it: it must crash
 * against an archive built without the flag and pass against one built with
 * it (`nimble testFfiThreads` runs both).
 *
 * The worker runs on a stack the test maps itself and unmaps after the join,
 * which is what makes the crash deterministic (see the comment in `main`).
 *
 * The worker writes enough, across several chunk flushes, that the
 * container, the stream buffers and the interned strings are all allocated
 * on the worker's heap. The main thread then closes, reads the finished
 * container's length, and frees.
 *
 * No mocks: the real static archive through its C ABI.
 */
#include <pthread.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include "codetracer_trace_writer.h"

#define STEPS 20000
#define STACK_SIZE (8 * 1024 * 1024)

static trace_writer_t writer;

static void *record(void *unused) {
    (void)unused;
    char name[32];
    for (int i = 0; i < STEPS; i++) {
        trace_writer_register_step(writer, "/src/worker.c", 1 + (i % 40));
        snprintf(name, sizeof name, "v%d", i % 97);
        trace_writer_register_variable_int(writer, name, i, 7 /* Int */, "int");
    }
    return NULL;
}

int main(void) {
    codetracer_trace_writer_init();
    writer = trace_writer_new("worker_thread_exit", FFI_TRACE_FORMAT_BINARY);
    if (writer == NULL || trace_writer_begin_in_memory(writer) != 0) {
        fprintf(stderr, "FAIL: could not open the writer: %s\n", trace_writer_last_error());
        return 1;
    }
    trace_writer_start(writer, "/src/worker.c", 1);

    void *stack = mmap(NULL, STACK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (stack == MAP_FAILED) {
        fprintf(stderr, "FAIL: mmap\n");
        return 1;
    }
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstack(&attr, stack, STACK_SIZE);
    pthread_t worker;
    if (pthread_create(&worker, &attr, record, NULL) != 0) {
        fprintf(stderr, "FAIL: pthread_create\n");
        return 1;
    }
    pthread_join(worker, NULL); /* the worker has EXITED from here on */
    /* ...and its memory is gone. glibc keeps an exited thread's stack (and
     * the TLS block inside it, where Nim keeps the thread's heap
     * descriptor) in a cache, so in a small program the dead heap's
     * descriptor usually survives by luck; a long-running host with many
     * threads (Godot) exhausts the cache and the memory is unmapped. The
     * worker runs on a stack this test owns, and unmapping it after the join
     * makes that deterministic. */
    munmap(stack, STACK_SIZE);

    int rc = trace_writer_close(writer);
    if (rc != 0) {
        fprintf(stderr, "FAIL: trace_writer_close: %s\n", trace_writer_last_error());
        return 1;
    }
    size_t len = trace_writer_container_len(writer);
    trace_writer_free(writer);
    if (len == 0) {
        fprintf(stderr, "FAIL: the closed container is empty\n");
        return 1;
    }
    printf("PASS: closed from the main thread after the worker exited (%zu bytes)\n", len);
    return 0;
}
