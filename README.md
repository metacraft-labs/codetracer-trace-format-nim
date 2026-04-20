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

## License

MIT
