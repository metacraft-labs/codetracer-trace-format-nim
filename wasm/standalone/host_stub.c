/* Minimal freestanding runtime for the wasm32-unknown-unknown build.
 *
 * `-nostdlib` means there is no libc at all, so this file supplies the handful
 * of symbols Nim (with -d:useMalloc) and clang's own lowering actually
 * reference.  In a real embedding these come from the host instead: a Rust
 * cdylib forwards malloc/free/realloc to std::alloc (Rust's wasm32 std already
 * links dlmalloc), and ct_host_write / ct_host_abort become JS or Rust
 * callbacks.  This bump allocator exists so the Nim side can be proven on its
 * own, without a Rust host in the loop.
 */

typedef unsigned long size_t;

/* --- allocator ---------------------------------------------------------- */

extern unsigned char __heap_base;

#define WASM_PAGE 65536u

static size_t heap_ptr; /* next free address; 0 means "not started yet" */
static size_t heap_end; /* one past the last addressable byte we own */

/* Every block carries its own size, so realloc knows how much to copy.  A
   bump allocator can never grow in place, and copying `n` bytes out of a
   block that might be smaller than `n` would read past the end of linear
   memory — which is exactly the trap this replaced:

     memory fault at wasm address 0x20000 in linear memory of size 0x20000
     wasm trap: out of bounds memory access

   The header is 16 bytes so the payload stays 16-byte aligned. */
typedef struct {
  size_t len;
  size_t pad[3];
} hdr_t;

static void *bump_alloc(size_t n) {
  if (heap_ptr == 0) {
    heap_ptr = (size_t)&__heap_base;
    heap_end = (size_t)__builtin_wasm_memory_size(0) * WASM_PAGE;
  }
  size_t total = (sizeof(hdr_t) + n + 15u) & ~(size_t)15u;
  if (heap_ptr + total > heap_end) {
    /* The module is linked with -Wl,--no-entry and no libc, so nothing else
       grows linear memory; doing it here is the whole memory management
       story.  Grow generously to keep the syscall count down. */
    size_t need = heap_ptr + total - heap_end;
    size_t pages = (need + WASM_PAGE - 1u) / WASM_PAGE;
    if (pages < 16u) pages = 16u;
    if (__builtin_wasm_memory_grow(0, pages) == (size_t)-1) return 0;
    heap_end += pages * WASM_PAGE;
  }
  hdr_t *h = (hdr_t *)heap_ptr;
  h->len = n;
  heap_ptr += total;
  return (void *)((unsigned char *)h + sizeof(hdr_t));
}

void *malloc(size_t n) { return bump_alloc(n ? n : 1); }

void free(void *p) { (void)p; /* bump allocator: never reclaims */ }

void *calloc(size_t n, size_t m) {
  size_t total = n * m;
  unsigned char *p = (unsigned char *)bump_alloc(total ? total : 1);
  if (p)
    for (size_t i = 0; i < total; i++) p[i] = 0;
  return p;
}

void *realloc(void *p, size_t n) {
  void *q = bump_alloc(n);
  if (!q || !p) return q;
  hdr_t *h = (hdr_t *)((unsigned char *)p - sizeof(hdr_t));
  size_t copy = h->len < n ? h->len : n;
  unsigned char *s = (unsigned char *)p, *d = (unsigned char *)q;
  for (size_t i = 0; i < copy; i++) d[i] = s[i];
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
