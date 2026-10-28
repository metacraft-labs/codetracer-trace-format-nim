/*
 * Several C host threads each record into their OWN writer at the same time,
 * and one thread's error must not appear as another's.
 *
 * The host library is built with `--threads:off`: one Nim heap for the
 * process, which is only safe because every entry point holds a process-wide
 * lock (src/codetracer_trace_writer_ffi_runtime.c). Without the lock, these
 * threads allocate from that heap concurrently and corrupt it (a crash, or a
 * container that differs between threads).
 *
 * The last error is per thread by contract (`trace_writer_last_error`: "the
 * last error message for the current thread"). Under `--threads:off` a Nim
 * threadvar is one global, so the buffer is kept in C thread-local storage;
 * a thread that never failed must keep reading "".
 *
 * Asserted: every thread's container is non-empty and byte-identical to the
 * others' (same workload, same recording id), and the error isolation.
 *
 * No mocks: the real archive, the real header, real threads.
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "codetracer_trace_writer.h"

#define THREADS 4
#define STEPS 30000

typedef struct {
  int idx;
  unsigned char *bytes;
  size_t len;
  int failed;
  char err_seen[256];
} job_t;

/* A reusable barrier on a mutex and a condition variable.
 *
 * `pthread_barrier_t` is an optional POSIX feature (the Barriers option,
 * _POSIX_BARRIERS) that Apple's libc does not provide, so this host would not
 * compile on macOS. The same small barrier is used on every platform, so the
 * code the test runs is the code it runs everywhere.
 *
 * Semantics match `pthread_barrier_wait` for what this test needs: no thread
 * returns from `barrier_wait` until all `count` threads have called it, and
 * the barrier can be waited on again straight away (it is used twice). The
 * generation counter is what makes reuse safe: a woken thread leaves when the
 * generation it arrived in has ended, which also absorbs spurious wake-ups,
 * and a fast thread that re-enters for the next round cannot be counted
 * into the round the slow ones are still leaving.
 * https://pubs.opengroup.org/onlinepubs/9799919799/functions/pthread_barrier_wait.html
 */
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t all_arrived;
  unsigned count;
  unsigned waiting;
  unsigned long generation;
} barrier_t;

static int barrier_init(barrier_t *b, unsigned count) {
  if (count == 0) return -1;
  if (pthread_mutex_init(&b->mutex, NULL) != 0) return -1;
  if (pthread_cond_init(&b->all_arrived, NULL) != 0) {
    pthread_mutex_destroy(&b->mutex);
    return -1;
  }
  b->count = count;
  b->waiting = 0;
  b->generation = 0;
  return 0;
}

static void barrier_wait(barrier_t *b) {
  pthread_mutex_lock(&b->mutex);
  unsigned long arrived_in = b->generation;
  if (++b->waiting == b->count) {
    /* Last one in: open the barrier and start the next round. */
    b->waiting = 0;
    b->generation++;
    pthread_cond_broadcast(&b->all_arrived);
  } else {
    while (b->generation == arrived_in) {
      pthread_cond_wait(&b->all_arrived, &b->mutex);
    }
  }
  pthread_mutex_unlock(&b->mutex);
}

static void barrier_destroy(barrier_t *b) {
  pthread_cond_destroy(&b->all_arrived);
  pthread_mutex_destroy(&b->mutex);
}

static barrier_t start_line;

static void *record(void *arg) {
  job_t *job = (job_t *)arg;
  trace_writer_t w = trace_writer_new("concurrent", FFI_TRACE_FORMAT_BINARY);
  if (w == NULL) { job->failed = 1; return NULL; }
  /* Same recording id everywhere, so the containers can be compared. */
  if (trace_writer_set_recording_id(w, "0190f0a0-0000-7000-8000-000000000001") != 0 ||
      trace_writer_begin_in_memory(w) != 0) { job->failed = 1; return NULL; }
  barrier_wait(&start_line);
  trace_writer_start(w, "/src/main.c", 1);
  char path[32], name[32];
  for (int i = 0; i < STEPS; i++) {
    snprintf(path, sizeof path, "/src/f%d.c", i % 50);
    trace_writer_register_step(w, path, 1 + (i % 40));
    snprintf(name, sizeof name, "v%d", i % 97);
    trace_writer_register_variable_int(w, name, i, 7 /* Int */, "int");
  }
  if (job->idx == 0) {
    /* Thread 0 alone makes a call fail; the others must not see it. */
    trace_writer_clear_last_error();
    if (trace_writer_close(NULL) == 0) { job->failed = 1; return NULL; }
  }
  barrier_wait(&start_line);
  snprintf(job->err_seen, sizeof job->err_seen, "%s", trace_writer_last_error());
  if (trace_writer_close(w) != 0) { job->failed = 1; return NULL; }
  job->len = trace_writer_container_len(w);
  job->bytes = malloc(job->len);
  memcpy(job->bytes, trace_writer_container_ptr(w), job->len);
  trace_writer_free(w);
  return NULL;
}

int main(void) {
  codetracer_trace_writer_init();
  if (barrier_init(&start_line, THREADS) != 0) {
    fprintf(stderr, "FAIL: could not initialise the start barrier\n");
    return 1;
  }
  pthread_t t[THREADS];
  job_t jobs[THREADS];
  memset(jobs, 0, sizeof jobs);
  for (int i = 0; i < THREADS; i++) {
    jobs[i].idx = i;
    pthread_create(&t[i], NULL, record, &jobs[i]);
  }
  for (int i = 0; i < THREADS; i++) pthread_join(t[i], NULL);
  barrier_destroy(&start_line);
  for (int i = 0; i < THREADS; i++) {
    if (jobs[i].failed || jobs[i].len == 0) {
      fprintf(stderr, "FAIL: thread %d did not produce a container\n", i);
      return 1;
    }
    if (jobs[i].len != jobs[0].len || memcmp(jobs[i].bytes, jobs[0].bytes, jobs[0].len) != 0) {
      fprintf(stderr, "FAIL: thread %d's container differs from thread 0's (%zu vs %zu bytes)\n",
              i, jobs[i].len, jobs[0].len);
      return 1;
    }
  }
  if (jobs[0].err_seen[0] == 0) {
    fprintf(stderr, "FAIL: the failing thread reads no last error\n");
    return 1;
  }
  for (int i = 1; i < THREADS; i++) {
    if (jobs[i].err_seen[0] != 0) {
      fprintf(stderr, "FAIL: thread %d reads thread 0's error: \"%s\"\n", i, jobs[i].err_seen);
      return 1;
    }
  }
  printf("PASS: %d concurrent writers produced identical %zu-byte containers; errors stayed per thread\n",
         THREADS, jobs[0].len);
  for (int i = 0; i < THREADS; i++) free(jobs[i].bytes);
  return 0;
}
