/* WASI entry-point shim for Nim-generated C.
 *
 * Nim's C backend emits the POSIX three-parameter entry point
 *
 *     int main(int argc, char **args, char **env)
 *
 * Clang's WebAssembly ABI only rewrites the zero- and two-parameter forms of
 * `main` into `__main_argc_argv`.  The three-parameter form keeps the plain
 * name `main`, so wasi-libc's `crt1-command.o` is left calling an
 * undefined-weak `__main_argc_argv`, which wasm-ld resolves to a trapping
 * stub.  The module links clean and then dies on the first instruction of the
 * program with:
 *
 *     0: 0x226 - demo.wasm!undefined_weak:main
 *     1: 0x2fd - demo.wasm!__main_void
 *     2: 0x260 - demo.wasm!_start
 *     wasm trap: wasm `unreachable` instruction executed
 *
 * The Nim-generated translation units are therefore compiled with
 * `-Dmain=nimWasiMain`, which renames Nim's entry point out of the way, and
 * this file supplies the two-parameter `main` that clang does rewrite.  The
 * `#undef` is required because Nim applies `--passC` flags to `{.compile.}`
 * files as well, so this file is compiled with `-Dmain=nimWasiMain` too.
 *
 * `environ` is wasi-libc's; passing it keeps `os.getEnv` working, which the
 * three-parameter signature exists to support.
 */
#undef main

extern char **environ;

int nimWasiMain(int argc, char **argv, char **env);

int main(int argc, char **argv) { return nimWasiMain(argc, argv, environ); }
