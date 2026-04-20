{.push raises: [].}

## Seekable Zstd compression — pure Nim implementation.
##
## Splits data into independently-decompressible Zstd frames with a seek table
## appended as a Zstd skippable frame. This enables random access to any
## decompressed byte offset without decompressing the entire file.
##
## The seek table is written in "foot" format (entries before integrity,
## placed at the end of the file), which is compatible with the Rust `zeekstd`
## crate and the original Facebook/Meta seekable Zstd specification.

import stew/endians2
import results

import ./zstd_bindings

export results

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

const
  SkippableFrameMagic* = 0x184D2A5E'u32
  SeekableMagicNumber* = 0x8F92EAB1'u32
  SeekTableEntrySize = 8   # 4 (compressed) + 4 (decompressed)
  SeekTableFooterSize = 9  # 4 (num_frames) + 1 (descriptor) + 4 (seekable magic)
  SkippableHeaderSize = 8  # 4 (magic) + 4 (frame_size)
  DefaultFrameThreshold* = 2 * 1024 * 1024  # 2 MiB
  DefaultCompressionLevel* = 3

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  SeekTableEntry* = object
    compressedSize*: uint32
    decompressedSize*: uint32

  SeekTable* = object
    entries*: seq[SeekTableEntry]

  SeekableZstdEncoder* = object
    buffer: seq[byte]           # Accumulation buffer for current frame (pre-allocated)
    bufferPos: int              # Write cursor into buffer
    output: seq[byte]           # Single output buffer (all frames written here)
    outputPos: int              # Write cursor into output
    compressBuffer: seq[byte]   # Reusable compressed frame buffer (allocated once)
    seekTable: SeekTable        # Built incrementally
    frameThreshold: int         # Uncompressed bytes per frame
    compressionLevel: int       # Zstd compression level
    cctx: pointer               # Reusable ZSTD compression context

  SeekableZstdDecoder* = object
    data: seq[byte]             # Complete compressed data including seek table
    seekTable*: SeekTable       # Parsed from footer
    frameOffsets: seq[uint64]   # Cumulative compressed offsets for each frame
    decompOffsets: seq[uint64]  # Cumulative decompressed offsets for each frame
    dctx: pointer               # Reusable ZSTD decompression context

# ---------------------------------------------------------------------------
# SeekTable operations
# ---------------------------------------------------------------------------

proc totalDecompressedSize*(table: SeekTable): uint64 =
  ## Sum of all decompressed frame sizes.
  for entry in table.entries:
    result += uint64(entry.decompressedSize)

proc totalCompressedSize*(table: SeekTable): uint64 =
  ## Sum of all compressed frame sizes.
  for entry in table.entries:
    result += uint64(entry.compressedSize)

proc serializeSeekTableHead*(table: SeekTable): seq[byte] =
  ## Serialize a seek table in "head" format (v0.1.1) as a Zstd skippable frame.
  ##
  ## Layout:
  ##   [Skippable_Magic: 4 LE] [Frame_Size: 4 LE]
  ##   [Num_Frames: 4 LE] [Descriptor: 1 byte = 0x00] [Seekable_Magic: 4 LE]
  ##   [Entry_0: c_size(4 LE) + d_size(4 LE)] ... [Entry_N-1]
  let numEntries = table.entries.len
  let frameSize = uint32(numEntries * SeekTableEntrySize + SeekTableFooterSize)
  let totalSize = SkippableHeaderSize + int(frameSize)
  result = newSeq[byte](totalSize)

  var pos = 0

  # Skippable magic (0x184D2A5E)
  let magicBytes = toBytesLE(SkippableFrameMagic)
  copyMem(addr result[pos], unsafeAddr magicBytes[0], 4)
  pos += 4

  # Frame size
  let frameSizeBytes = toBytesLE(frameSize)
  copyMem(addr result[pos], unsafeAddr frameSizeBytes[0], 4)
  pos += 4

  # Integrity: num_frames
  let numFramesBytes = toBytesLE(uint32(numEntries))
  copyMem(addr result[pos], unsafeAddr numFramesBytes[0], 4)
  pos += 4

  # Integrity: descriptor (0x00 = no checksums)
  result[pos] = 0x00
  pos += 1

  # Integrity: seekable magic (0x8F92EAB1)
  let seekMagicBytes = toBytesLE(SeekableMagicNumber)
  copyMem(addr result[pos], unsafeAddr seekMagicBytes[0], 4)
  pos += 4

  # Entries
  for entry in table.entries:
    let cBytes = toBytesLE(entry.compressedSize)
    copyMem(addr result[pos], unsafeAddr cBytes[0], 4)
    pos += 4
    let dBytes = toBytesLE(entry.decompressedSize)
    copyMem(addr result[pos], unsafeAddr dBytes[0], 4)
    pos += 4

proc serializeSeekTable*(table: SeekTable): seq[byte] =
  ## Serialize a seek table in "foot" format as a Zstd skippable frame.
  ##
  ## Layout:
  ##   [Skippable_Magic: 4 LE] [Frame_Size: 4 LE]
  ##   [Entry_0: c_size(4 LE) + d_size(4 LE)] ... [Entry_N-1]
  ##   [Num_Frames: 4 LE] [Descriptor: 1 byte = 0x00] [Seekable_Magic: 4 LE]
  let numEntries = table.entries.len
  let frameSize = uint32(numEntries * SeekTableEntrySize + SeekTableFooterSize)
  let totalSize = SkippableHeaderSize + int(frameSize)
  result = newSeq[byte](totalSize)

  var pos = 0

  # Skippable magic (0x184D2A5E)
  let magicBytes = toBytesLE(SkippableFrameMagic)
  copyMem(addr result[pos], unsafeAddr magicBytes[0], 4)
  pos += 4

  # Frame size
  let frameSizeBytes = toBytesLE(frameSize)
  copyMem(addr result[pos], unsafeAddr frameSizeBytes[0], 4)
  pos += 4

  # Entries
  for entry in table.entries:
    let cBytes = toBytesLE(entry.compressedSize)
    copyMem(addr result[pos], unsafeAddr cBytes[0], 4)
    pos += 4
    let dBytes = toBytesLE(entry.decompressedSize)
    copyMem(addr result[pos], unsafeAddr dBytes[0], 4)
    pos += 4

  # Footer: num_frames
  let numFramesBytes = toBytesLE(uint32(numEntries))
  copyMem(addr result[pos], unsafeAddr numFramesBytes[0], 4)
  pos += 4

  # Footer: descriptor (0x00 = no checksums)
  result[pos] = 0x00
  pos += 1

  # Footer: seekable magic (0x8F92EAB1)
  let seekMagicBytes = toBytesLE(SeekableMagicNumber)
  copyMem(addr result[pos], unsafeAddr seekMagicBytes[0], 4)

proc parseSeekTableFoot(data: openArray[byte]): Result[SeekTable, string] =
  ## Parse a seek table in "foot" format from the end of the data.
  if data.len < SkippableHeaderSize + SeekTableFooterSize:
    return err("data too small to contain a seek table")

  # Read the footer (last 9 bytes)
  let footerStart = data.len - SeekTableFooterSize

  # Check seekable magic
  var magic4: array[4, byte]
  magic4[0] = data[footerStart + 5]
  magic4[1] = data[footerStart + 6]
  magic4[2] = data[footerStart + 7]
  magic4[3] = data[footerStart + 8]
  let seekMagic = fromBytesLE(uint32, magic4)
  if seekMagic != SeekableMagicNumber:
    return err("seekable magic number mismatch: expected 0x8F92EAB1")

  # Check descriptor — reserved bits must be zero
  let descriptor = data[footerStart + 4]
  if ((descriptor shr 2) and 0x1F'u8) != 0:
    return err("reserved descriptor bits are set")

  # Read num_frames
  var nf4: array[4, byte]
  nf4[0] = data[footerStart + 0]
  nf4[1] = data[footerStart + 1]
  nf4[2] = data[footerStart + 2]
  nf4[3] = data[footerStart + 3]
  let numFrames = fromBytesLE(uint32, nf4)

  # Compute expected seek table skippable frame size
  let entriesSize = int(numFrames) * SeekTableEntrySize
  let skippableFrameTotal = SkippableHeaderSize + entriesSize + SeekTableFooterSize

  if data.len < skippableFrameTotal:
    return err("data too small for declared number of frames")

  # Verify skippable header
  let headerStart = data.len - skippableFrameTotal

  var hdr4: array[4, byte]
  hdr4[0] = data[headerStart + 0]
  hdr4[1] = data[headerStart + 1]
  hdr4[2] = data[headerStart + 2]
  hdr4[3] = data[headerStart + 3]
  let skippableMagic = fromBytesLE(uint32, hdr4)
  if skippableMagic != SkippableFrameMagic:
    return err("skippable frame magic mismatch")

  hdr4[0] = data[headerStart + 4]
  hdr4[1] = data[headerStart + 5]
  hdr4[2] = data[headerStart + 6]
  hdr4[3] = data[headerStart + 7]
  let declaredFrameSize = fromBytesLE(uint32, hdr4)
  let expectedFrameSize = uint32(entriesSize + SeekTableFooterSize)
  if declaredFrameSize != expectedFrameSize:
    return err("frame size mismatch in skippable header")

  # Parse entries
  var entries = newSeq[SeekTableEntry](numFrames)
  let entriesStart = headerStart + SkippableHeaderSize
  for i in 0 ..< int(numFrames):
    let offset = entriesStart + i * SeekTableEntrySize
    var c4, d4: array[4, byte]
    c4[0] = data[offset + 0]
    c4[1] = data[offset + 1]
    c4[2] = data[offset + 2]
    c4[3] = data[offset + 3]
    d4[0] = data[offset + 4]
    d4[1] = data[offset + 5]
    d4[2] = data[offset + 6]
    d4[3] = data[offset + 7]
    entries[i] = SeekTableEntry(
      compressedSize: fromBytesLE(uint32, c4),
      decompressedSize: fromBytesLE(uint32, d4),
    )

  ok(SeekTable(entries: entries))

proc parseSeekTableHead*(data: openArray[byte]): Result[SeekTable, string] =
  ## Parse a seek table in "head" format (v0.1.1) from the beginning of the data.
  ##
  ## Head format layout:
  ##   [Skippable_Magic: 4 LE] [Frame_Size: 4 LE]
  ##   [Num_Frames: 4 LE] [Descriptor: 1] [Seekable_Magic: 4 LE]
  ##   [Entry_0: c_size(4 LE) + d_size(4 LE)] ... [Entry_N-1]
  if data.len < SkippableHeaderSize + SeekTableFooterSize:
    return err("data too small to contain a head seek table")

  # Check skippable magic at start
  var hdr4: array[4, byte]
  hdr4[0] = data[0]
  hdr4[1] = data[1]
  hdr4[2] = data[2]
  hdr4[3] = data[3]
  let skippableMagic = fromBytesLE(uint32, hdr4)
  if skippableMagic != SkippableFrameMagic:
    return err("head format: skippable frame magic mismatch")

  # Frame size
  hdr4[0] = data[4]
  hdr4[1] = data[5]
  hdr4[2] = data[6]
  hdr4[3] = data[7]
  let declaredFrameSize = fromBytesLE(uint32, hdr4)

  # Integrity starts at offset 8 (right after header)
  let integrityStart = SkippableHeaderSize

  if data.len < integrityStart + SeekTableFooterSize:
    return err("head format: data too small for integrity")

  # Read num_frames
  var nf4: array[4, byte]
  nf4[0] = data[integrityStart + 0]
  nf4[1] = data[integrityStart + 1]
  nf4[2] = data[integrityStart + 2]
  nf4[3] = data[integrityStart + 3]
  let numFrames = fromBytesLE(uint32, nf4)

  # Check descriptor — reserved bits must be zero
  let descriptor = data[integrityStart + 4]
  if ((descriptor shr 2) and 0x1F'u8) != 0:
    return err("head format: reserved descriptor bits are set")

  # Check seekable magic
  var magic4: array[4, byte]
  magic4[0] = data[integrityStart + 5]
  magic4[1] = data[integrityStart + 6]
  magic4[2] = data[integrityStart + 7]
  magic4[3] = data[integrityStart + 8]
  let seekMagic = fromBytesLE(uint32, magic4)
  if seekMagic != SeekableMagicNumber:
    return err("head format: seekable magic number mismatch")

  # Verify frame size
  let entriesSize = int(numFrames) * SeekTableEntrySize
  let expectedFrameSize = uint32(entriesSize + SeekTableFooterSize)
  if declaredFrameSize != expectedFrameSize:
    return err("head format: frame size mismatch")

  let entriesStart = integrityStart + SeekTableFooterSize
  let totalSkippable = SkippableHeaderSize + int(declaredFrameSize)
  if data.len < totalSkippable:
    return err("head format: data too small for entries")

  # Parse entries
  var entries = newSeq[SeekTableEntry](numFrames)
  for i in 0 ..< int(numFrames):
    let offset = entriesStart + i * SeekTableEntrySize
    var c4, d4: array[4, byte]
    c4[0] = data[offset + 0]
    c4[1] = data[offset + 1]
    c4[2] = data[offset + 2]
    c4[3] = data[offset + 3]
    d4[0] = data[offset + 4]
    d4[1] = data[offset + 5]
    d4[2] = data[offset + 6]
    d4[3] = data[offset + 7]
    entries[i] = SeekTableEntry(
      compressedSize: fromBytesLE(uint32, c4),
      decompressedSize: fromBytesLE(uint32, d4),
    )

  ok(SeekTable(entries: entries))

proc parseSeekTable*(data: openArray[byte]): Result[SeekTable, string] =
  ## Parse a seek table, auto-detecting foot vs head format.
  ##
  ## First tries foot format (seek table at end). If that fails, tries
  ## head format (seek table at beginning).
  let footRes = parseSeekTableFoot(data)
  if footRes.isOk:
    return footRes
  # Try head format
  let headRes = parseSeekTableHead(data)
  if headRes.isOk:
    return headRes
  # Return the foot error as the primary one, since foot is the default format
  err(footRes.error)

# ---------------------------------------------------------------------------
# Encoder
# ---------------------------------------------------------------------------

proc newSeekableZstdEncoder*(
    frameThreshold: int = DefaultFrameThreshold,
    compressionLevel: int = DefaultCompressionLevel
): SeekableZstdEncoder =
  let bound = int(ZSTD_compressBound(csize_t(frameThreshold)))
  # Pre-allocate output for ~2x compressed estimate + some headroom
  let initialOutputCap = max(bound * 4, 65536)
  var output = newSeq[byte](initialOutputCap)
  var buffer = newSeq[byte](frameThreshold)
  var compressBuffer = newSeq[byte](bound)
  let cctx = ZSTD_createCCtx()
  SeekableZstdEncoder(
    buffer: buffer,
    bufferPos: 0,
    output: output,
    outputPos: 0,
    compressBuffer: compressBuffer,
    seekTable: SeekTable(entries: @[]),
    frameThreshold: frameThreshold,
    compressionLevel: compressionLevel,
    cctx: cctx,
  )

proc ensureOutputCapacity(enc: var SeekableZstdEncoder, needed: int) {.inline.} =
  ## Ensure output buffer has room for `needed` more bytes.
  let required = enc.outputPos + needed
  if required > enc.output.len:
    var newCap = enc.output.len * 2
    while newCap < required:
      newCap = newCap * 2
    enc.output.setLen(newCap)

proc flushFrame(enc: var SeekableZstdEncoder) =
  ## Compress the current buffer into the output buffer directly.
  if enc.bufferPos == 0:
    return

  let srcSize = csize_t(enc.bufferPos)

  # Compress into reusable compressBuffer using context
  let compressedSize = ZSTD_compressCCtx(
    enc.cctx,
    addr enc.compressBuffer[0], csize_t(enc.compressBuffer.len),
    addr enc.buffer[0], srcSize,
    cint(enc.compressionLevel),
  )

  # Check for error
  if ZSTD_isError(compressedSize) != 0:
    return

  # Copy compressed data directly into output
  enc.ensureOutputCapacity(int(compressedSize))
  copyMem(addr enc.output[enc.outputPos], addr enc.compressBuffer[0], int(compressedSize))
  enc.outputPos += int(compressedSize)

  enc.seekTable.entries.add(SeekTableEntry(
    compressedSize: uint32(compressedSize),
    decompressedSize: uint32(enc.bufferPos),
  ))

  enc.bufferPos = 0

proc write*(enc: var SeekableZstdEncoder, data: openArray[byte]) =
  ## Append data to the encoder. When the internal buffer exceeds the frame
  ## threshold, a new compressed frame is flushed automatically.
  var offset = 0
  while offset < data.len:
    let remaining = enc.frameThreshold - enc.bufferPos
    let chunk = min(remaining, data.len - offset)
    copyMem(addr enc.buffer[enc.bufferPos], unsafeAddr data[offset], chunk)
    enc.bufferPos += chunk
    offset += chunk

    if enc.bufferPos >= enc.frameThreshold:
      enc.flushFrame()

proc finish*(enc: var SeekableZstdEncoder): seq[byte] =
  ## Flush remaining buffered data and produce the complete seekable Zstd output.
  ## Returns all compressed frames followed by the seek table skippable frame.
  enc.flushFrame()

  let seekTableData = serializeSeekTable(enc.seekTable)

  # Append seek table to output
  enc.ensureOutputCapacity(seekTableData.len)
  if seekTableData.len > 0:
    copyMem(addr enc.output[enc.outputPos], unsafeAddr seekTableData[0], seekTableData.len)
  enc.outputPos += seekTableData.len

  # Return truncated output (no copy, just set length)
  enc.output.setLen(enc.outputPos)
  result = move enc.output

  # Free the compression context
  if enc.cctx != nil:
    discard ZSTD_freeCCtx(enc.cctx)
    enc.cctx = nil

# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

proc buildOffsets(dec: var SeekableZstdDecoder) =
  ## Build cumulative compressed and decompressed offset arrays.
  let n = dec.seekTable.entries.len
  dec.frameOffsets = newSeq[uint64](n + 1)
  dec.decompOffsets = newSeq[uint64](n + 1)
  dec.frameOffsets[0] = 0
  dec.decompOffsets[0] = 0
  for i in 0 ..< n:
    dec.frameOffsets[i + 1] = dec.frameOffsets[i] + uint64(dec.seekTable.entries[i].compressedSize)
    dec.decompOffsets[i + 1] = dec.decompOffsets[i] + uint64(dec.seekTable.entries[i].decompressedSize)

proc initSeekableZstdDecoder*(data: openArray[byte]): Result[SeekableZstdDecoder, string] =
  ## Create a decoder by parsing the seek table from the footer of the data.
  let tableRes = parseSeekTable(data)
  if tableRes.isErr:
    return err(tableRes.error)

  var dec = SeekableZstdDecoder(
    data: @data,
    seekTable: tableRes.get(),
    dctx: ZSTD_createDCtx(),
  )
  dec.buildOffsets()

  # Verify that total compressed size + seek table frame size == data length
  let totalComp = dec.seekTable.totalCompressedSize()
  let seekTableFrameSize = SkippableHeaderSize +
    dec.seekTable.entries.len * SeekTableEntrySize + SeekTableFooterSize
  let expectedLen = uint64(totalComp) + uint64(seekTableFrameSize)
  if expectedLen != uint64(data.len):
    if dec.dctx != nil:
      discard ZSTD_freeDCtx(dec.dctx)
    return err("data length mismatch: compressed frames + seek table != total size")

  ok(dec)

proc frameCount*(dec: SeekableZstdDecoder): int =
  ## Number of compressed frames.
  dec.seekTable.entries.len

proc decompressFrame*(dec: SeekableZstdDecoder, frameIndex: int): Result[seq[byte], string] =
  ## Decompress a single frame by index.
  if frameIndex < 0 or frameIndex >= dec.seekTable.entries.len:
    return err("frame index out of range")

  let entry = dec.seekTable.entries[frameIndex]
  let compOffset = int(dec.frameOffsets[frameIndex])
  let compSize = int(entry.compressedSize)
  let decompSize = int(entry.decompressedSize)

  if decompSize == 0:
    return ok(newSeq[byte](0))

  var output = newSeq[byte](decompSize)

  let actualSize = ZSTD_decompressDCtx(
    dec.dctx,
    addr output[0], csize_t(decompSize),
    unsafeAddr dec.data[compOffset], csize_t(compSize),
  )

  if ZSTD_isError(actualSize) != 0:
    let errName = ZSTD_getErrorName(actualSize)
    return err("zstd decompression error: " & $errName)

  if int(actualSize) != decompSize:
    return err("decompressed size mismatch: expected " & $decompSize & " got " & $actualSize)

  ok(output)

proc decompressAll*(dec: SeekableZstdDecoder): Result[seq[byte], string] =
  ## Decompress all frames and concatenate the result.
  let totalDecomp = dec.seekTable.totalDecompressedSize()
  if totalDecomp == 0:
    return ok(newSeq[byte](0))

  var output = newSeq[byte](int(totalDecomp))
  var pos = 0
  for i in 0 ..< dec.seekTable.entries.len:
    let entry = dec.seekTable.entries[i]
    let compOffset = int(dec.frameOffsets[i])
    let compSize = int(entry.compressedSize)
    let decompSize = int(entry.decompressedSize)

    if decompSize == 0:
      continue

    let actualSize = ZSTD_decompressDCtx(
      dec.dctx,
      addr output[pos], csize_t(decompSize),
      unsafeAddr dec.data[compOffset], csize_t(compSize),
    )

    if ZSTD_isError(actualSize) != 0:
      let errName = ZSTD_getErrorName(actualSize)
      return err("zstd decompression error: " & $errName)

    if int(actualSize) != decompSize:
      return err("decompressed size mismatch: expected " & $decompSize & " got " & $actualSize)

    pos += decompSize

  ok(output)

proc seekToOffset*(dec: SeekableZstdDecoder, decompressedOffset: uint64): Result[(int, uint64), string] =
  ## Given a decompressed byte offset, return (frameIndex, offsetWithinFrame).
  let totalDecomp = dec.seekTable.totalDecompressedSize()
  if decompressedOffset >= totalDecomp:
    return err("decompressed offset out of range: " & $decompressedOffset & " >= " & $totalDecomp)

  # Binary search for the frame containing this offset
  var lo = 0
  var hi = dec.seekTable.entries.len
  while lo + 1 < hi:
    let mid = (lo + hi) div 2
    if dec.decompOffsets[mid] <= decompressedOffset:
      lo = mid
    else:
      hi = mid

  let offsetWithinFrame = decompressedOffset - dec.decompOffsets[lo]
  ok((lo, offsetWithinFrame))
