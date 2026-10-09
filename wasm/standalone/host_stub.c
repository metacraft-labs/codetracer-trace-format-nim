/* Minimal freestanding runtime for the wasm32-unknown-unknown build.
 *
 * `-nostdlib` means there is no libc at all, so this file supplies the handful
 * of symbols Nim (with -d:useMalloc) and clang's own lowering actually
 * reference.  In a real embedding these come from the host instead: a Rust
 * cdylib forwards malloc/free/realloc to std::alloc (Rust's wasm32 std already
 * links dlmalloc), and ct_host_write / ct_host_abort become JS or Rust
 * callbacks.  This allocator exists so the Nim side can be proven on its own,
 * without a Rust host in the loop.
 */

typedef unsigned long size_t;

/* --- allocator ---------------------------------------------------------- */
/* Power-of-two size classes with one free list per class.
 *
 * `free` must reclaim.  A writer grows and drops buffers for every chunk it
 * compresses, so the memory it has ever allocated is many times what it holds
 * at once; an allocator that never reuses a block runs out of linear memory
 * on a trace of ordinary size (150,000 events) while the live set is a few
 * MiB.
 *
 * A block's capacity is the power of two at or above the request, so a freed
 * block serves any later request of its class exactly, a `realloc` within the
 * capacity stays in place, and the doubling growth of a Nim seq walks up the
 * classes one at a time.  Blocks are never split or coalesced; the cost is at
 * most 2x the live bytes, which a test shim can afford and a production host
 * replaces with its own allocator anyway. */

extern unsigned char __heap_base;

#define WASM_PAGE 65536u
#define MIN_CLASS 4u  /* 16-byte payloads */
#define MAX_CLASS 31u /* 2 GiB payloads; linear memory is 4 GiB at most */

static size_t heap_ptr; /* next never-used address; 0 means "not started yet" */
static size_t heap_end; /* one past the last addressable byte we own */

/* The header is 16 bytes so the payload stays 16-byte aligned. It records the
    block's class, which is all `free` and `realloc` need: the capacity is
    `1 << cls`, and `realloc` never copies more than that out of a block. */
typedef struct {
  size_t cls;
  size_t pad[3];
} hdr_t;

/* A free block's payload holds the link to the next free block of its class. */
static void *free_list[MAX_CLASS + 1];

static hdr_t *hdr_of(void *p) {
  return (hdr_t *)((unsigned char *)p - sizeof(hdr_t));
}

static size_t class_of(size_t n) {
  size_t cls = MIN_CLASS;
  while (((size_t)1 << cls) < n) cls++;
  return cls;
}

static void *carve(size_t cls) {
  if (heap_ptr == 0) {
    heap_ptr = ((size_t)&__heap_base + 15u) & ~(size_t)15u;
    heap_end = (size_t)__builtin_wasm_memory_size(0) * WASM_PAGE;
  }
  size_t total = sizeof(hdr_t) + ((size_t)1 << cls);
  if (total > heap_end - heap_ptr) {
    /* The module is linked with -Wl,--no-entry and no libc, so nothing else
        grows linear memory. Grow generously to keep the call count down. */
    size_t need = total - (heap_end - heap_ptr);
    size_t pages = (need + WASM_PAGE - 1u) / WASM_PAGE;
    if (pages < 16u) pages = 16u;
    if (__builtin_wasm_memory_grow(0, pages) == (size_t)-1) {
      /* The generous step may be what does not fit; retry with the need. */
      pages = (need + WASM_PAGE - 1u) / WASM_PAGE;
      if (__builtin_wasm_memory_grow(0, pages) == (size_t)-1) return 0;
    }
    heap_end += pages * WASM_PAGE;
  }
  hdr_t *h = (hdr_t *)heap_ptr;
  h->cls = cls;
  heap_ptr += total;
  return (void *)((unsigned char *)h + sizeof(hdr_t));
}

void *malloc(size_t n) {
  if (n == 0) n = 1;
  if (n > ((size_t)1 << MAX_CLASS)) return 0;
  size_t cls = class_of(n);
  void *p = free_list[cls];
  if (p) {
    free_list[cls] = *(void **)p;
    return p;
  }
  return carve(cls);
}

void free(void *p) {
  if (!p) return;
  size_t cls = hdr_of(p)->cls;
  *(void **)p = free_list[cls];
  free_list[cls] = p;
}

void *calloc(size_t n, size_t m) {
  /* A product that wraps would allocate a short block the caller believes is
      long. */
  if (m != 0 && n > (size_t)-1 / m) return 0;
  size_t total = n * m;
  unsigned char *p = (unsigned char *)malloc(total);
  if (p)
    for (size_t i = 0; i < total; i++) p[i] = 0;
  return p;
}

void *realloc(void *p, size_t n) {
  if (!p) return malloc(n);
  size_t cap = (size_t)1 << hdr_of(p)->cls;
  if (n <= cap) return p;
  unsigned char *q = (unsigned char *)malloc(n);
  if (!q) return 0; /* the original block stays valid, as C requires */
  unsigned char *s = (unsigned char *)p;
  for (size_t i = 0; i < cap; i++) q[i] = s[i];
  free(p);
  return q;
}

/* --- freestanding mem* -------------------------------------------------- */
/* clang lowers struct copies and loops to these even with -nostdlib. */

void *memcpy(void *d, const void *s, size_t n) {
  unsigned char *dd = d;
  const unsigned char *ss = s;
  for (size_t i = 0; i < n; i++) dd[i] = ss[i];
  return d;
}

void *memmove(void *d, const void *s, size_t n) {
  unsigned char *dd = d;
  const unsigned char *ss = s;
  if (dd < ss)
    for (size_t i = 0; i < n; i++) dd[i] = ss[i];
  else
    for (size_t i = n; i > 0; i--) dd[i - 1] = ss[i - 1];
  return d;
}

void *memset(void *d, int c, size_t n) {
  unsigned char *dd = d;
  for (size_t i = 0; i < n; i++) dd[i] = (unsigned char)c;
  return d;
}

int memcmp(const void *a, const void *b, size_t n) {
  const unsigned char *x = a, *y = b;
  for (size_t i = 0; i < n; i++)
    if (x[i] != y[i]) return (int)x[i] - (int)y[i];
  return 0;
}

size_t strlen(const char *s) {
  size_t n = 0;
  while (s[n]) n++;
  return n;
}

/* --- stdio / process stubs --------------------------------------------- */
/* Nim's system module pulls in std/syncio unconditionally (it is what `echo`
 * and the `File` type live in), so these are referenced from the object files
 * even though the CTFS container path never calls them.  Stubbing them is
 * what makes -nostdlib link: without these, wasm-ld reports exactly
 *
 *   undefined symbol: fseeko / ferror / errno / strerror / clearerr / fwrite
 *   undefined symbol: exit
 *
 * A real host would forward these to its own I/O instead. */

int errno;

void exit(int code) {
  (void)code;
  __builtin_trap();
}

typedef struct _IO_FILE FILE;

size_t fwrite(const void *p, size_t sz, size_t n, FILE *f) {
  (void)p; (void)sz; (void)f;
  return n;
}
int fseeko(FILE *f, long long off, int whence) {
  (void)f; (void)off; (void)whence;
  return -1;
}
int ferror(FILE *f) { (void)f; return 0; }
void clearerr(FILE *f) { (void)f; }
char *strerror(int e) { (void)e; return (char *)"error"; }
