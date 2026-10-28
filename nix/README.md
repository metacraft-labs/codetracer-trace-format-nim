# Schema-6 inspector

`schema6-reader.nix` builds `ct-print` for consumers that still produce
`meta.dat` schema 6. Import it with the consumer's pinned `pkgs`:

```nix
import "${readerSource}/nix/schema6-reader.nix" { inherit pkgs; }
```

The package pins source `051efd22b00ec3b88676fb07755a65c5a4ca8fde` and
backports the container-profile routing fix. The generic layout probe reads
the actual full or compact directory, and stored whole-file compression is
reconstructed before member lookup. The metadata reader's version checks
remain unchanged, including the native event decoder’s version-5-only guard. The current inspector continues to read its current schema;
this historical package is a separate reader role.

The install check records a trace with the production writer and compares
`--full`, `--summary`, `--meta-json` and `--events` across the full, compact
and compressed compact representations. Invalid profile and metadata-version
controls must still fail. The installed `share/ct-print/provenance.json`
identifies the source and backport.
