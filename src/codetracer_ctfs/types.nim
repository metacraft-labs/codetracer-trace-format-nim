when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## CTFS type definitions, constants, and low-level helpers.

import stew/endians2
export endians2

const
  CtfsMagic*: array[5, byte] = [0xC0'u8, 0xDE, 0x72, 0xAC, 0xE2]
  CtfsVersion*: uint8 = 3
  CtfsVersionV2*: uint8 = 2  ## Previous version, accepted by v3 readers
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
    compression*: CtfsCompressionMethod  ## Header compression tag
    encryption*: CtfsEncryptionMethod    ## Header encryption tag
    # Streaming support
    streaming*: bool        ## True if streaming writes to disk
    streamPath*: string     ## File path when streaming (empty if not)
    streamFile*: File       ## Open file handle when streaming

proc entriesPerBlock*(c: Ctfs): uint64 =
  uint64(c.blockSize) div 8

proc usableEntries*(c: Ctfs): uint64 =
  ## Usable entries per mapping block (last entry reserved for chain pointer).
  c.entriesPerBlock() - 1

proc fileEntryOffset*(c: Ctfs, index: int): int =
  ## Byte offset of a file entry in block 0.
  HeaderSize + ExtHeaderSize + index * FileEntrySize

proc readU64LE*(data: openArray[byte], offset: int): uint64 =
  var arr: array[8, byte]
  for i in 0 ..< 8:
    arr[i] = data[offset + i]
  fromBytesLE(uint64, arr)

proc writeU64LE*(data: var openArray[byte], offset: int, val: uint64) =
  let le = toBytesLE(val)
  for i in 0 ..< 8:
    data[offset + i] = le[i]

proc writeU32LE*(data: var openArray[byte], offset: int, val: uint32) =
  let le = toBytesLE(val)
  for i in 0 ..< 4:
    data[offset + i] = le[i]

proc blockOffset*(c: Ctfs, blockNum: uint64): int =
  ## Byte offset of a given block number.
  int(blockNum) * int(c.blockSize)
