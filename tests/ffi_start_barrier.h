/* The FFI stress host needs a reusable start barrier. Darwin supplies mutexes
 * and condition variables but not pthread_barrier_t. The portable arm is also
 * selectable by the component control on POSIX hosts; no host API is spoofed.
 */
#ifndef CT_FFI_START_BARRIER_H
#define CT_FFI_START_BARRIER_H
#include <errno.h>
#include <pthread.h>

#if defined(__APPLE__) || defined(FFI_FORCE_PORTABLE_BARRIER)
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t changed;
  unsigned participants;
  unsigned arrived;
  unsigned generation;
} ffi_start_barrier;

static int ffi_start_barrier_init(ffi_start_barrier *barrier, unsigned count) {
  if (count == 0) return EINVAL;
  int rc = pthread_mutex_init(&barrier->mutex, NULL);
  if (rc != 0) return rc;
  rc = pthread_cond_init(&barrier->changed, NULL);
  if (rc != 0) {
    int cleanup = pthread_mutex_destroy(&barrier->mutex);
    return cleanup != 0 ? cleanup : rc;
  }
  barrier->participants = count;
  barrier->arrived = 0;
  barrier->generation = 0;
  return 0;
}

static int ffi_start_barrier_wait(ffi_start_barrier *barrier) {
  int rc = pthread_mutex_lock(&barrier->mutex);
  if (rc != 0) return rc;
  unsigned generation = barrier->generation;
  barrier->arrived++;
  if (barrier->arrived == barrier->participants) {
    barrier->arrived = 0;
    barrier->generation++;
    rc = pthread_cond_broadcast(&barrier->changed);
  } else {
    /* A wakeup alone never releases a participant. It must observe the next
     * generation, including when this barrier is reused immediately. */
    while (generation == barrier->generation && rc == 0)
      rc = pthread_cond_wait(&barrier->changed, &barrier->mutex);
  }
  int unlock = pthread_mutex_unlock(&barrier->mutex);
  return rc != 0 ? rc : unlock;
}

static int ffi_start_barrier_destroy(ffi_start_barrier *barrier) {
  /* The caller joins every participant before destroying the barrier. */
  int rc = pthread_cond_destroy(&barrier->changed);
  int mutex = pthread_mutex_destroy(&barrier->mutex);
  return rc != 0 ? rc : mutex;
}
#else
typedef pthread_barrier_t ffi_start_barrier;
static int ffi_start_barrier_init(ffi_start_barrier *barrier, unsigned count) {
  return pthread_barrier_init(barrier, NULL, count);
}
static int ffi_start_barrier_wait(ffi_start_barrier *barrier) {
  int rc = pthread_barrier_wait(barrier);
  return rc == PTHREAD_BARRIER_SERIAL_THREAD ? 0 : rc;
}
static int ffi_start_barrier_destroy(ffi_start_barrier *barrier) {
  return pthread_barrier_destroy(barrier);
}
#endif
#endif
