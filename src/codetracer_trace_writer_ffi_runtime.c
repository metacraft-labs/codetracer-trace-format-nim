/* The two pieces of the C ABI's runtime that must not live in Nim memory.
 *
 * A host links this library as a static archive or a shared object and calls
 * it from whichever of its threads it likes: it creates a writer on one,
 * records on a worker, and closes on a third after the worker has exited.
 * The library is therefore built with `--threads:off`, which gives it ONE
 * process-wide Nim heap that no thread owns. (Built with `--threads:on`, every
 * thread gets its own heap whose descriptor lives in that thread's TLS; memory
 * a worker allocated is then freed into a heap that died with the worker, and
 * the host crashes in `rawDealloc`. tests/test_ffi_worker_thread_exit.c.)
 *
 * One heap is only safe if two threads never run Nim code at once, so:
 *
 * 1. `ct_ffi_lock` / `ct_ffi_unlock` -- a process-wide RECURSIVE lock that
 *    every exported entry point holds for its whole body (`ffiGuard`).
 *    Recursive because an entry point may call another. Uncontended it costs
 *    a few tens of nanoseconds per call; concurrent calls serialise.
 *
 * 2. The last-error buffer, per thread, OUTSIDE the Nim heap. Under
 *    `--threads:off` a Nim `{.threadvar.}` is one global, and the C ABI
 *    promises `trace_writer_last_error` reports the calling thread's last
 *    error. The buffer is malloc'd per thread and freed by a thread-exit
 *    destructor, so a host with many short-lived threads does not leak one per
 *    thread.
 *
 * A single-threaded WebAssembly build (no `__wasm_threads__`) has no second
 * thread to exclude: the lock is a no-op and the buffer a plain global.
 */
#include <stdlib.h>
#include <string.h>

#if defined(__wasm__) && !defined(__wasm_threads__)
#define CT_SINGLE_THREADED 1
#endif

#if defined(CT_SINGLE_THREADED)

static char *g_err;

void ct_ffi_lock(void) {}
void ct_ffi_unlock(void) {}
static char *err_get(void) { return g_err; }
static void err_put(char *p) { g_err = p; }

#elif defined(_WIN32)

#include <windows.h>

static INIT_ONCE g_once = INIT_ONCE_STATIC_INIT;
static CRITICAL_SECTION g_cs; /* recursive by definition */
static DWORD g_fls = FLS_OUT_OF_INDEXES;

static void WINAPI err_free(void *p) { free(p); }

static BOOL CALLBACK ct_init(PINIT_ONCE once, PVOID param, PVOID *ctx) {
  (void)once; (void)param; (void)ctx;
  InitializeCriticalSection(&g_cs);
  g_fls = FlsAlloc(err_free);
  return TRUE;
}

static void ensure_init(void) { InitOnceExecuteOnce(&g_once, ct_init, NULL, NULL); }
void ct_ffi_lock(void) { ensure_init(); EnterCriticalSection(&g_cs); }
void ct_ffi_unlock(void) { LeaveCriticalSection(&g_cs); }
static char *err_get(void) { ensure_init(); return (char *)FlsGetValue(g_fls); }
static void err_put(char *p) { ensure_init(); FlsSetValue(g_fls, p); }

#else

#include <pthread.h>

static pthread_once_t g_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t g_mu;
static pthread_key_t g_key;

static void ct_init(void) {
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&g_mu, &attr);
  pthread_mutexattr_destroy(&attr);
  pthread_key_create(&g_key, free);
}

void ct_ffi_lock(void) { pthread_once(&g_once, ct_init); pthread_mutex_lock(&g_mu); }
void ct_ffi_unlock(void) { pthread_mutex_unlock(&g_mu); }
static char *err_get(void) { pthread_once(&g_once, ct_init); return (char *)pthread_getspecific(g_key); }
static void err_put(char *p) { pthread_once(&g_once, ct_init); pthread_setspecific(g_key, p); }

#endif

/* Replace the calling thread's last error with `len` bytes of `msg`.
 * Returns 0, or -1 when the copy could not be allocated (the old message is
 * then kept, rather than reporting no error at all). */
int ct_ffi_set_last_error(const char *msg, size_t len) {
  char *copy = (char *)malloc(len + 1);
  if (!copy) return -1;
  if (len) memcpy(copy, msg, len);
  copy[len] = 0;
  free(err_get());
  err_put(copy);
  return 0;
}

/* The calling thread's last error; "" when it has none. Valid until this
 * thread's next call that sets or clears it. */
const char *ct_ffi_last_error(void) {
  const char *p = err_get();
  return p ? p : "";
}
