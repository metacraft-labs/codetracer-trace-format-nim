{.push raises: [].}

## Namespace entry descriptors for CTFS namespace B-tree key index.
##
## Type A descriptors are 8 bytes (a single uint64 LE).
## Type B descriptors are 16 bytes (two uint64 LE words).
##
## Both types distinguish between sub-block entries (small data stored in a
## pooled block) and graduated entries (large data with their own block mapping).

# -----------------------------------------------------------------------------
# Pool / slot helpers
# -----------------------------------------------------------------------------

proc poolSize*(poolClass: uint8): int {.inline.} =
  ## Returns the pool size in bytes for the given pool class (0..6).
  ## poolClass 0 = 32 B, 1 = 64 B, ..., 6 = 2048 B.
  32 shl int(poolClass)

proc slotIndexBits*(poolClass: uint8): int {.inline.} =
  ## Number of bits used for slot_index in the 12-bit slot_and_used field.
  7 - int(poolClass)

proc usedBytesBits*(poolClass: uint8): int {.inline.} =
  ## Number of bits used for used_bytes in the 12-bit slot_and_used field.
  5 + int(poolClass)

proc encodeSlotAndUsed*(poolClass: uint8, slotIndex: uint16,
                        usedBytes: uint16): uint16 {.inline.} =
  ## Pack slot_index and used_bytes into a 12-bit value.
  let ubBits = usedBytesBits(poolClass)
  (slotIndex shl uint16(ubBits)) or usedBytes

proc decodeSlotAndUsed*(poolClass: uint8,
                        combined: uint16): (uint16, uint16) {.inline.} =
  ## Unpack a 12-bit slot_and_used into (slot_index, used_bytes).
  let ubBits = usedBytesBits(poolClass)
  let usedMask = (1'u16 shl ubBits) - 1
  let usedBytes = combined and usedMask
  let slotIndex = combined shr ubBits
  (slotIndex, usedBytes)

# -----------------------------------------------------------------------------
# Type A descriptor (8 bytes)
# -----------------------------------------------------------------------------

type
  TypeADescriptor* = object
    raw*: uint64

  TypeASubBlock* = object
    blockNum*: uint64    ## 48 bits
    poolClass*: uint8    ## 3 bits (0-6)
    slotIndex*: uint16   ## variable bits
    usedBytes*: uint16   ## variable bits

  TypeAGraduated* = object
    mapBlock*: uint32    ## 31 bits
    dataSize*: uint32    ## 32 bits

proc isGraduated*(d: TypeADescriptor): bool {.inline.} =
  (d.raw and (1'u64 shl 63)) != 0

proc encodeTypeASubBlock*(blockNum: uint64, poolClass: uint8,
                          slotIndex: uint16,
                          usedBytes: uint16): TypeADescriptor =
  ## Encode a Type A sub-block descriptor.
  ##
  ## Layout (bit 63 = 0):
  ##   bits 62-15: block_num (48 bits)
  ##   bits 14-12: pool_class (3 bits)
  ##   bits 11-0:  slot_and_used (12 bits)
  let slotAndUsed = encodeSlotAndUsed(poolClass, slotIndex, usedBytes)
  var v: uint64 = 0
  v = v or (blockNum shl 15)
  v = v or (uint64(poolClass) shl 12)
  v = v or uint64(slotAndUsed)
  TypeADescriptor(raw: v)

proc decodeTypeASubBlock*(d: TypeADescriptor): TypeASubBlock =
  let raw = d.raw
  let blockNum = (raw shr 15) and ((1'u64 shl 48) - 1)
  let poolClass = uint8((raw shr 12) and 0x7)
  let slotAndUsed = uint16(raw and 0xFFF)
  let (slotIndex, usedBytes) = decodeSlotAndUsed(poolClass, slotAndUsed)
  TypeASubBlock(blockNum: blockNum, poolClass: poolClass,
                slotIndex: slotIndex, usedBytes: usedBytes)

proc encodeTypeAGraduated*(
    mapBlock: uint32, dataSize: uint32): TypeADescriptor =
  ## Encode a Type A graduated descriptor.
  ##
  ## Layout (bit 63 = 1):
  ##   bits 62-32: map_block (31 bits)
  ##   bits 31-0:  data_size (32 bits)
  var v: uint64 = 1'u64 shl 63
  v = v or (uint64(mapBlock) shl 32)
  v = v or uint64(dataSize)
  TypeADescriptor(raw: v)

proc decodeTypeAGraduated*(d: TypeADescriptor): TypeAGraduated =
  let raw = d.raw
  let mapBlock = uint32((raw shr 32) and 0x7FFFFFFF'u64)
  let dataSize = uint32(raw and 0xFFFFFFFF'u64)
  TypeAGraduated(mapBlock: mapBlock, dataSize: dataSize)

# -----------------------------------------------------------------------------
# Type B descriptor (16 bytes)
# -----------------------------------------------------------------------------

type
  TypeBDescriptor* = object
    word0*: uint64  ## map_block (0 for sub-block)
    word1*: uint64  ## packed second word or data_size

  TypeBSubBlock* = object
    blockNum*: uint64    ## 49 bits
    poolClass*: uint8    ## 3 bits
    slotIndex*: uint16
    usedBytes*: uint16

  TypeBGraduated* = object
    mapBlock*: uint64
    dataSize*: uint64

proc isGraduated*(d: TypeBDescriptor): bool {.inline.} =
  d.word0 != 0

proc encodeTypeBSubBlock*(blockNum: uint64, poolClass: uint8,
                          slotIndex: uint16,
                          usedBytes: uint16): TypeBDescriptor =
  ## Encode a Type B sub-block descriptor.
  ##
  ## Layout (word0 == 0):
  ##   word1 bits 63-15: block_num (49 bits)
  ##   word1 bits 14-12: pool_class (3 bits)
  ##   word1 bits 11-0:  slot_and_used (12 bits)
  let slotAndUsed = encodeSlotAndUsed(poolClass, slotIndex, usedBytes)
  var w1: uint64 = 0
  w1 = w1 or (blockNum shl 15)
  w1 = w1 or (uint64(poolClass) shl 12)
  w1 = w1 or uint64(slotAndUsed)
  TypeBDescriptor(word0: 0, word1: w1)

proc decodeTypeBSubBlock*(d: TypeBDescriptor): TypeBSubBlock =
  let w1 = d.word1
  let blockNum = (w1 shr 15) and ((1'u64 shl 49) - 1)
  let poolClass = uint8((w1 shr 12) and 0x7)
  let slotAndUsed = uint16(w1 and 0xFFF)
  let (slotIndex, usedBytes) = decodeSlotAndUsed(poolClass, slotAndUsed)
  TypeBSubBlock(blockNum: blockNum, poolClass: poolClass,
                slotIndex: slotIndex, usedBytes: usedBytes)

proc encodeTypeBGraduated*(
    mapBlock: uint64, dataSize: uint64): TypeBDescriptor =
  ## Encode a Type B graduated descriptor.
  ##
  ## word0: map_block (must be != 0)
  ## word1: data_size
  TypeBDescriptor(word0: mapBlock, word1: dataSize)

proc decodeTypeBGraduated*(d: TypeBDescriptor): TypeBGraduated =
  TypeBGraduated(mapBlock: d.word0, dataSize: d.word1)
