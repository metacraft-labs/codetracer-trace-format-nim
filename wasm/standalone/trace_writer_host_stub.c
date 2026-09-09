/* The extra host symbols the full TraceWriter needs on wasm32-unknown-unknown.
 *
 * `host_stub.c` covers the allocator, the freestanding `mem*`, and the stdio
 * symbols `codetracer_ctfs` reaches. Pulling in the whole TraceWriter — rather
 * than just the container layer `ctfs_standalone.nim` exercises — adds four
 * more undefined symbols:
 *
 *   ct_host_unix_ms   the UUIDv7 timestamp source, in place of `epochTime()`.
 *                     Declared by `uuid_v7.nim` under `-d:ctHostClock`.
 *   getentropy        the UUIDv7 entropy source. `-d:ctLeanRecord` already
 *                     routes to it; on wasm32-wasip1 it resolves to wasi-libc's,
 *                     which is a `random_get` import.
 *   fclose, fflush    referenced by `std/syncio`, which `host_stub.c` does not
 *                     stub because the container layer never reaches them.
 *
 * They are DEFINED here rather than imported so that the module's zero-import
 * property is a property of the module and not of a generous host. A browser
 * embedding would instead import `Date.now()` and `crypto.getRandomValues` at a
 * cost of two imports; a Rust host forwards them from its own std.
 *
 * ---------------------------------------------------------------------------
 * THE GENERATOR BELOW IS DETERMINISTIC AND IS NOT FIT FOR A RECORDING IDENTITY.
 *
 * The bytes it returns feed the UUIDv7 that becomes the trace's `recording_id`,
 * so two traces recorded by two modules built from this file collide. That is
 * correct for measuring an import surface and for a selftest whose output must
 * be reproducible, and wrong for anything that keeps the container.
 *
 * A caller that supplies its own `recordingId` to `newTraceWriterInMemory`
 * never reaches either symbol: that path validates the string it was given and
 * does not mint a UUID. The symbols must still RESOLVE, because the minting
 * branch is live code in the same module — which is why they are here rather
 * than absent. An embedding that keeps its containers must supply a CSPRNG, or
 * pass a `recordingId` and rely on the branch never being taken.
 * ---------------------------------------------------------------------------
 */

typedef unsigned long size_t;

unsigned long long ct_host_unix_ms(void) {
  /* A fixed instant. A real host returns Date.now(). */
  return 1700000000000ULL;
}

int getentropy(void *buf, size_t n) {
  static unsigned long long s = 0x9E3779B97F4A7C15ULL;
  unsigned char *p = (unsigned char *)buf;
  for (size_t i = 0; i < n; i++) {
    s = s * 6364136223846793005ULL + 1442695040888963407ULL;
    p[i] = (unsigned char)(s >> 33);
  }
  return 0;
}

typedef struct _IO_FILE FILE;

int fclose(FILE *f) { (void)f; return 0; }
int fflush(FILE *f) { (void)f; return 0; }
