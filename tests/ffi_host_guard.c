/* Real Unix direct-child fixture guardian. No mocks, shell or process-name kill.
 * The audited C hosts use pthreads and no subprocesses. This is not an escaped
 * descendant guarantee. Timeout is harness failure, never a causal-control pass.
 * Parent retains its unreaped fork child, preventing PID reuse before signals.
 * Linux executes the held regular image descriptor; Darwin's path check is only
 * an observation and requires separate native platform qualification.
 */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define HOST_SECONDS 120.0
#define TERM_GRACE_SECONDS 2.0
#define KILL_REAP_SECONDS 2.0
#define HARNESS_TIMEOUT 125
#define HARNESS_ERROR 126
extern char **environ;
static int receipt_fd = -1;
static int finish(const char *kind, int code) {
  if (receipt_fd >= 0) {
    char body[96];
    int length = snprintf(body, sizeof body, "FFI-GUARD-RESULT-v1\n%s\n%d\n", kind, code);
    int written = 0;
    while (written < length) {
      ssize_t n = pwrite(receipt_fd, body + written, (size_t)(length - written), written);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) { close(receipt_fd); return HARNESS_ERROR; }
      written += (int)n;
    }
    if (ftruncate(receipt_fd, length) != 0 || fsync(receipt_fd) != 0) {
      close(receipt_fd); return HARNESS_ERROR;
    }
    close(receipt_fd);
    receipt_fd = -1;
  }
  return code;
}

static int now(double *value) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t) != 0) return -1;
  *value = (double)t.tv_sec + (double)t.tv_nsec / 1000000000.0;
  return 0;
}
static void tick(void) {
  struct timespec t = {0, 10000000};
  while (nanosleep(&t, &t) != 0 && errno == EINTR) {}
}
static int observe(pid_t child, int *status) {
  pid_t result;
  do { result = waitpid(child, status, WNOHANG); } while (result < 0 && errno == EINTR);
  if (result == child) return 1;
  if (result == 0) return 0;
  return -1;
}
static void retain_until_reaped(pid_t child, int *status) {
  /* Unknown reap is explicit failure; ownership is retained, never reported idle.
   * Kernel-uninterruptible children can exceed a userspace cleanup deadline. */
  for (;;) {
    int result = observe(child, status);
    if (result == 1) return;
    if (result < 0) {
      fprintf(stderr, "FFI-GUARD: unknown child wait authority; no further signals or clean claim\n");
      return;
    }
    tick();
  }
}
static void stop_owned_child(pid_t child, int *status) {
  double started, current;
  int result = observe(child, status);
  if (result == 1) return;
  if (result < 0) { retain_until_reaped(child, status); return; }
  if (kill(child, SIGTERM) != 0 && errno != ESRCH)
    fprintf(stderr, "FFI-GUARD: owned TERM failed errno=%d\n", errno);
  if (now(&started) == 0) {
    for (;;) {
      result = observe(child, status);
      if (result == 1) return;
      if (result < 0) { retain_until_reaped(child, status); return; }
      if (now(&current) != 0 || current - started >= TERM_GRACE_SECONDS) break;
      tick();
    }
  }
  if (kill(child, SIGKILL) != 0 && errno != ESRCH)
    fprintf(stderr, "FFI-GUARD: owned KILL failed errno=%d\n", errno);
  if (now(&started) == 0) {
    for (;;) {
      result = observe(child, status);
      if (result == 1) return;
      if (result < 0) { retain_until_reaped(child, status); return; }
      if (now(&current) != 0 || current - started >= KILL_REAP_SECONDS) break;
      tick();
    }
  }
  fprintf(stderr, "FFI-GUARD: cleanup deadline exceeded; retaining child authority\n");
  retain_until_reaped(child, status);
}
int main(int argc, char **argv) {
  struct stat image, path;
  double started, current;
  int pipes[2], status = 0, launch_error = 0, launched = 0;
  if (argc == 3 && strcmp(argv[1], "--reserve") == 0) {
    struct stat directory;
    if (lstat(argv[2], &directory) != 0 || !S_ISDIR(directory.st_mode) ||
        directory.st_uid != geteuid()) return finish("harness-error", HARNESS_ERROR);
    char name[4096];
    if (snprintf(name, sizeof name, "%s/guardian-result-XXXXXX", argv[2]) >= (int)sizeof name)
      return finish("harness-error", HARNESS_ERROR);
    int reserved = mkstemp(name);
    if (reserved < 0) return finish("harness-error", HARNESS_ERROR);
    const char pending[] = "FFI-GUARD-RESULT-v1\npending\n";
    if (write(reserved, pending, sizeof pending - 1) != (ssize_t)(sizeof pending - 1) ||
        fsync(reserved) != 0 || close(reserved) != 0) return finish("harness-error", HARNESS_ERROR);
    puts(name);
    return 0;
  }
  if (argc != 3) return finish("harness-error", HARNESS_ERROR);
  receipt_fd = open(argv[2], O_RDWR | O_NOFOLLOW);
  struct stat receipt;
  char pending[64];
  const char expected_pending[] = "FFI-GUARD-RESULT-v1\npending\n";
  if (receipt_fd < 0 || fstat(receipt_fd, &receipt) != 0 ||
      !S_ISREG(receipt.st_mode) || receipt.st_uid != geteuid() ||
      (receipt.st_mode & 0777) != 0600 || receipt.st_nlink != 1 ||
      receipt.st_size != (off_t)(sizeof expected_pending - 1) ||
      pread(receipt_fd, pending, sizeof expected_pending - 1, 0) != (ssize_t)(sizeof expected_pending - 1) ||
      memcmp(pending, expected_pending, sizeof expected_pending - 1) != 0 ||
      fcntl(receipt_fd, F_SETFD, FD_CLOEXEC) != 0) {
    if (receipt_fd >= 0) close(receipt_fd);
    receipt_fd = -1;
    fprintf(stderr, "FFI-GUARD: invalid exclusive pending receipt\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  if (lstat(argv[1], &path) != 0 || !S_ISREG(path.st_mode) ||
      path.st_uid != geteuid() || !(path.st_mode & 0111)) {
    fprintf(stderr, "FFI-GUARD: invalid owned regular host image\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  int fd = open(argv[1], O_RDONLY | O_NOFOLLOW);
  if (fd < 0 || fstat(fd, &image) != 0 || image.st_dev != path.st_dev ||
      image.st_ino != path.st_ino || image.st_mode != path.st_mode) {
    if (fd >= 0) close(fd);
    fprintf(stderr, "FFI-GUARD: host image identity changed\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  if (now(&started) != 0 || pipe(pipes) != 0) {
    close(fd);
    fprintf(stderr, "FFI-GUARD: clock/launch-pipe acquisition failed\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  if (fcntl(pipes[1], F_SETFD, FD_CLOEXEC) != 0 ||
      fcntl(pipes[0], F_SETFL, O_NONBLOCK) != 0 ||
      fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
    close(fd); close(pipes[0]); close(pipes[1]);
    fprintf(stderr, "FFI-GUARD: launch descriptor setup failed\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  struct sigaction action;
  memset(&action, 0, sizeof action);
  action.sa_handler = SIG_DFL;
  sigemptyset(&action.sa_mask);
  if (sigaction(SIGCHLD, &action, NULL) != 0) {
    close(fd); close(pipes[0]); close(pipes[1]);
    fprintf(stderr, "FFI-GUARD: child-wait disposition setup failed\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  pid_t child = fork();
  if (child < 0) {
    close(fd); close(pipes[0]); close(pipes[1]);
    fprintf(stderr, "FFI-GUARD: fork failed\n");
    return finish("harness-error", HARNESS_ERROR);
  }
  if (child == 0) {
    close(pipes[0]);
    char *child_argv[] = {argv[1], NULL};
#if defined(__linux__)
    fexecve(fd, child_argv, environ);
#else
    struct stat current_image;
    if (lstat(argv[1], &current_image) == 0 &&
        current_image.st_dev == image.st_dev && current_image.st_ino == image.st_ino &&
        current_image.st_mode == image.st_mode) execv(argv[1], child_argv);
    else errno = ESTALE;
#endif
    int saved = errno;
    ssize_t sent;
    do { sent = write(pipes[1], &saved, sizeof saved); } while (sent < 0 && errno == EINTR);
    _exit(HARNESS_ERROR);
  }
  close(fd); close(pipes[1]);
  fprintf(stderr, "FFI-GUARD: owned child pid=%ld uid=%ld budget=120s\n",
          (long)child, (long)geteuid());
  for (;;) {
    ssize_t n = read(pipes[0], &launch_error, sizeof launch_error);
    if (n == 0) launched = 1;
    else if (n > 0) {
      fprintf(stderr, "FFI-GUARD: launch failed errno=%d\n", launch_error);
      stop_owned_child(child, &status); close(pipes[0]);
      return finish("harness-error", HARNESS_ERROR);
    } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
      fprintf(stderr, "FFI-GUARD: launch observation failed\n");
      stop_owned_child(child, &status); close(pipes[0]);
      return finish("harness-error", HARNESS_ERROR);
    }
    int result = observe(child, &status);
    if (result < 0) {
      fprintf(stderr, "FFI-GUARD: unknown terminal authority\n");
      retain_until_reaped(child, &status); close(pipes[0]);
      return finish("harness-error", HARNESS_ERROR);
    }
    if (now(&current) != 0) {
      fprintf(stderr, "FFI-GUARD: monotonic clock failed\n");
      if (result == 0) stop_owned_child(child, &status);
      close(pipes[0]); return finish("harness-error", HARNESS_ERROR);
    }
    if (result == 1) {
      if (!launched) {
        ssize_t final_bytes;
        do { final_bytes = read(pipes[0], &launch_error, sizeof launch_error); }
        while (final_bytes < 0 && errno == EINTR);
        if (final_bytes == 0) launched = 1;
        else if (final_bytes > 0)
          fprintf(stderr, "FFI-GUARD: terminal launch failure errno=%d\n", launch_error);
      }
      close(pipes[0]);
      if (!launched) {
        fprintf(stderr, "FFI-GUARD: no confirmed exec\n");
        return finish("harness-error", HARNESS_ERROR);
      }
      if (current - started >= HOST_SECONDS) {
        fprintf(stderr, "FFI-GUARD: host exceeded execution budget\n");
        return finish("timeout", HARNESS_TIMEOUT);
      }
      fprintf(stderr, "FFI-GUARD: natural terminal elapsed=%.6f\n", current - started);
      if (WIFEXITED(status)) return finish("natural", WEXITSTATUS(status));
      if (WIFSIGNALED(status)) return finish("natural", 128 + WTERMSIG(status));
      return finish("harness-error", HARNESS_ERROR);
    }
    if (current - started >= HOST_SECONDS) {
      fprintf(stderr, "FFI-GUARD: execution timeout; harness failure\n");
      stop_owned_child(child, &status); close(pipes[0]);
      return finish("timeout", HARNESS_TIMEOUT);
    }
    tick();
  }
}
