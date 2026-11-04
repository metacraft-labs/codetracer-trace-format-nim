# codetracer-trace-format-nim

Nim implementation of the CTFS (CodeTracer File System) container format.

CTFS is a lightweight binary container that stores multiple named files in a single archive using multi-level block mapping. It is designed for high-throughput streaming writes (e.g., recording program traces) where concurrent readers need to observe data as it is written.

## Features

- **Base40 filename encoding** -- filenames up to 12 characters packed into a single `uint64`
- **Multi-level block mapping** -- up to 5 levels of indirection, supporting files up to terabytes
- **Streaming mode** -- writes are flushed to disk incrementally so concurrent readers see data in real time
- **Inline chunk index** -- encode/decode compressed chunk headers for chunked streams
- **GC-free** -- no `ref` types; suitable for use in `{.push raises: [].}` codebases

## Usage

```nim
import codetracer_ctfs

# Create an in-memory container
var c = createCtfs()
var f = c.addFile("events.log").get()

var data: array[1024, byte]
discard c.writeToFile(f, data)
discard c.writeCtfsToFile("output.ctfs")

# Or use streaming mode for concurrent readers
var cs = createCtfsStreaming("output.ctfs").get()
var fs = cs.addFile("events.log").get()
discard cs.writeToFile(fs, data)
cs.syncEntry(fs)  # flush to disk for readers
cs.closeCtfs()
```

## Specification

The format is documented in [codetracer-trace-format-spec](https://github.com/metacraft-labs/codetracer-trace-format-spec).

## Dependencies

- `stew >= 0.1.0` (for `stew/endians2`)
- `results`

## Testing

```
nimble test
```

For the typed Reprobuild graph, enter the owning shell:

```sh
nix develop --command repro test
```

The shell selects a JSON tool-path manifest through the `toolPathsFile`
variant. Both the selection and the file's contents invalidate cached graphs.
Re-entering the shell replaces its previous selection and preserves unrelated
variants. Override it after shell activation with
`repro test --variant toolPathsFile=/absolute/path/paths.json`.

The manifest has this shape on Linux and macOS:

```json
{
  "schema": 1,
  "paths": {
    "CT_ZSTD_INCLUDE_DIR": "/absolute/zstd/include",
    "CT_ZSTD_LIB_DIR": "/absolute/zstd/lib",
    "CT_PCRE_LIB_DIR": "/absolute/pcre/lib",
    "CT_NIM_STEW_SRC": "/absolute/stew",
    "CT_NIM_RESULTS_SRC": "/absolute/results",
    "CT_NIM_UNITTEST2_SRC": "/absolute/unittest2"
  }
}
```

Windows prerequisite binders accumulate the same source fields and Zstd include
directory, with `CT_ZSTD_LIBRARY_FILE` instead of the two POSIX library-directory
fields. Windows may also supply an optional `CT_PCRE_LIB_DIR` linker search directory.
Paths inside JSON may contain spaces or commas; the manifest's own path
must contain neither commas nor newlines because variants are comma-delimited.
The typed graph requires these fields and does not fall back to ambient `CT_*`
variables. Those exports remain available to native Nim and Nimble commands.

`just test-repro-tool-paths` exercises shell re-entry, both warm graph caches,
manifest edits and selection changes against the real recipe and compiler.

## License

MIT
