when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS type definitions, constants, and low-level helpers.

import stew/endians2
export endians2

const
  CtfsMagic*: array[5, byte] = [0xC0'u8, 0xDE, 0x72, 0xAC, 0xE2]
  CtfsVersion*: uint8 = 5
    ## `ctfs-container.md` §1: writers write 5 and readers refuse every other
    ## version, naming it (§2, "Older versions are refused").
  CtfsVersionV6*: uint8 = 6
    ## The 24-byte header that carries `Profile` and whole-file `Compression`
    ## (`ctfs-container.md` §1, §1a, §1b). Version 6 is version 5's body plus
    ## those eight bytes, so everything §2 says about `MapBlock`'s three forms
    ## holds in a version-6 full-profile container unchanged.
    ##
    ## **This library does NOT read version 6, and that is deliberate.**
    ## `CtfsVersion` is 5 and `ctfsVersionError` refuses everything else by
    ## name, version 6 included. A version-6 container's `FileEntry` array
    ## starts at `24 + R` rather than `16 + R`, so a reader that accepted the
    ## version without implementing the body would resolve every entry out of
    ## the reserved bytes — the same shape of defect as reading a
    ## pre-correction container because its version stamp was trusted
    ## (`meta_dat.nim`'s `LastShiftedGlobalIndexVersion`). The compact body is
    ## the next milestone's work; the constant exists so the refusal, the
    ## header offsets below and the parsers in `container.nim` all name one
    ## number from one place.
  V6HeaderSize* = 24
    ## Size of the version-6 container header, in bytes: the 16 bytes every
    ## earlier version has, plus `profile`, `compression` and six reserved
    ## bytes. The six exist so that, in an unsharded container, the
    ## `FileEntry` array still starts 8-byte aligned (`ctfs-container.md` §1).
  V6ProfileOffset* = 16
  V6CompressionOffset* = 17
  V6ReservedOffset* = 18
  V6ReservedLen* = 6
  CtfsDirect*: uint64 = 1'u64 shl 63
    ## `ctfs-container.md` §2, "`MapBlock` has three forms": a `MapBlock` with
    ## this bit set names the member's only data block (`MapBlock and not
    ## CtfsDirect`); the member owns no mapping block. `0` is an empty member,
    ## any other value a level-1 mapping block.
  DefaultMaxShards*: uint8 = 0
    ## `ctfs-container.md`: a container that is not sharded writes `0`, and `1`
    ## is not a synonym for it. This wrote `1` while the Rust writer wrote `0`
    ## for containers that were otherwise the same, which is how a field with
    ## two spellings of one state presents: neither writer was wrong against the
    ## text as it stood, so the text was pinned and both now write `0`.
  DefaultBlockSize*: uint32 = 4096
  DefaultMaxRootEntries*: uint32 = 31
  HeaderSize* = 8
  ExtHeaderSize* = 8
  FileEntrySize* = 24  # 8 (size) + 8 (mapBlock) + 8 (name)
  MaxChainLevels* = 5  ## Maximum depth of multi-level mapping

type
  CtfsCompressionMethod* = enum
    cmNone = 0        ## No compression
    cmZstd = 1        ## Zstd compression
    cmLz4 = 2         ## LZ4 compression (reserved, not yet implemented)

  CtfsEncryptionMethod* = enum
    emNone = 0        ## No encryption
    emAes256Gcm = 1   ## AES-256-GCM encryption (reserved, not yet implemented)

  CtfsProfile* = enum
    ## `ctfs-container.md` §1a. Byte 16 of a version-6 header. CLOSED SET:
    ## these are the only two defined values and anything else is a refusal,
    ## so the parser is `parseCtfsProfile`, which returns a `Result`, and not
    ## a cast from the byte.
    cpFull = 0        ## Block 0, FileEntry array, block map
    cpCompact = 1     ## Directory + concatenated raw members, no block map

  CtfsWholeFileCompression* = enum
    ## `ctfs-container.md` §1b. Byte 17 of a version-6 header: the scheme
    ## applied to the container image from offset 24 to the end of the stored
    ## object.
    ##
    ## **This is a different field from `CtfsCompressionMethod`**, which is
    ## per-member and lives inside the region a whole-file scheme covers. The
    ## set is two members because a member of it is a promise that every
    ## reader of the format implements the scheme, and zstd is the only one
    ## both this package and the Rust db-backend already decode (`ruzstd` on
    ## `wasm32-unknown-unknown`). It deliberately does NOT carry the
    ## `reserved, not yet implemented` member that `CtfsCompressionMethod`
    ## carries for LZ4 — an enumerated scheme with no implementation is a
    ## capability a consumer can read and cannot rely on.
    wfcNone = 0       ## Stored as-is. What a BlockTracer archive declares
    wfcZstd = 1       ## One zstd frame over the body

  ## Inline chunk header for chunked compressed streams.
  ## Written before each compressed chunk in the stream:
  ##   [ChunkHeader: 16 bytes][compressed data: compressedSize bytes]
  ChunkIndexEntry* = object
    compressedSize*: uint32    ## Size of the compressed data following this header
    eventCount*: uint32        ## Number of events in this chunk
    firstGeid*: uint64         ## GEID of the first event in this chunk

const
  ChunkIndexEntrySize* = 16  ## 4 (compressed_size) + 4 (count) + 8 (first_geid)
  DefaultChunkSize* = 4096   ## Default number of events per chunk

type
  CtfsInternalFile* = object
    entryIndex*: int        ## Index in the file entry array
    writePos*: uint64       ## Current write position within the file
    dataBlockCount*: uint64 ## Number of full data blocks written

  Ctfs* = object
    data*: seq[byte]        ## In-memory container data
    blockSize*: uint32
    maxRootEntries*: uint32
    nextFreeBlock*: uint64  ## Next block to allocate
    encryption*: CtfsEncryptionMethod    ## Header encryption tag (byte 6)
    maxShards*: uint8                    ## Max shards (byte 7)
    # Streaming support
    streaming*: bool        ## True if streaming writes to disk
    streamPath*: string     ## File path when streaming (empty if not)
    streamFile*: File       ## Open file handle when streaming
    deferWrites*: bool
      ## Streaming writes are recorded as dirty blocks and handed to the
      ## operating system by `publish` — at every sealed chunk, at
      ## `meta.dat`, at close — instead of written through on every append.
      ## `ctfs-container.md` §6, "Durability", allows buffering between seals
      ## and requires everything up to the last seal to be written.
    dirtyBlocks*: seq[uint64]
      ## Blocks written since the last `publish` (deferred mode).
    dirtyMark*: seq[bool]
      ## Indexed by block number: already in `dirtyBlocks`.

proc entriesPerBlock*(c: Ctfs): uint64 =
  uint64(c.blockSize) div 8

proc usableEntries*(c: Ctfs): uint64 =
  ## Usable entries per mapping block (last entry reserved for chain pointer).
  c.entriesPerBlock() - 1

proc fileEntryOffset*(c: Ctfs, index: int): int =
  ## Byte offset of a file entry from the start of the container.  The entry
  ## array starts in block 0 and, when it is larger than block 0, continues
  ## into the blocks after it (`rootBlockCount`), so this is a plain byte
  ## offset and may lie past the end of block 0.
  HeaderSize + ExtHeaderSize + index * FileEntrySize

proc rootBlockCount*(blockSize: uint32, maxRootEntries: uint32,
                     maxShards: uint8): uint64 =
  ## `ctfs-container.md` §1: the number of contiguous blocks, starting at
  ## block 0, that hold the header, the free-list roots and the file-entry
  ## array.
  ##
  ##     R           = 7 * max_shards * 6
  ##     root_blocks = ceil((16 + R + max_root_entries * 24) / block_size)
  ##
  ## Data block allocation begins at block `root_blocks`.  It is 1 whenever
  ## the entries fit block 0 (every container written before the overflow was
  ## implemented) and for `max_root_entries = 0` (auto-fill of block 0).
  ## 0 for a zero block size, which no reader accepts.
  if blockSize == 0'u32:
    return 0
  let rootBytes = uint64(HeaderSize + ExtHeaderSize) +
    7'u64 * uint64(maxShards) * 6'u64 +
    uint64(maxRootEntries) * uint64(FileEntrySize)
  max(1'u64, (rootBytes + uint64(blockSize) - 1) div uint64(blockSize))

proc rootBlockCount*(c: Ctfs): uint64 =
  rootBlockCount(c.blockSize, c.maxRootEntries, c.maxShards)

proc readU64LE*(data: openArray[byte], offset: int): uint64 =
  fromBytesLE(uint64, data.toOpenArray(offset, offset + 7))

proc writeU64LE*(data: var openArray[byte], offset: int, val: uint64) =
  let le = toBytesLE(val)
  for i in 0 ..< 8:
    data[offset + i] = le[i]

proc readU32LE*(data: openArray[byte], offset: int): uint32 =
  fromBytesLE(uint32, data.toOpenArray(offset, offset + 3))

proc writeU32LE*(data: var openArray[byte], offset: int, val: uint32) =
  let le = toBytesLE(val)
  for i in 0 ..< 4:
    data[offset + i] = le[i]

proc blockOffset*(c: Ctfs, blockNum: uint64): int =
  ## Byte offset of a given block number.
  int(blockNum) * int(c.blockSize)

proc isDirectMapBlock*(mapBlock: uint64): bool {.inline.} =
  ## True when a file entry's `MapBlock` names the member's only data block
  ## (`ctfs-container.md` §2) rather than a mapping block.
  (mapBlock and CtfsDirect) != 0

proc directDataBlock*(mapBlock: uint64): uint64 {.inline.} =
  ## The data block a tagged `MapBlock` names.
  mapBlock and not CtfsDirect

proc ctfsVersionError*(data: openArray[byte]): string =
  ## Empty when `data` carries the CTFS version this library reads (5);
  ## otherwise the refusal, naming the version found and the one read
  ## (`ctfs-container.md` §2, "Older versions are refused").
  if data.len < 6:
    return "CTFS container too short for a header (" & $data.len & " bytes)"
  if data[5] != CtfsVersion:
    return "CTFS container version " & $data[5] & " is not supported: this " &
      "reader reads version " & $CtfsVersion & " only (older containers are " &
      "re-recorded; ctfs-container.md §2, \"Older versions are refused\")"
  ""
