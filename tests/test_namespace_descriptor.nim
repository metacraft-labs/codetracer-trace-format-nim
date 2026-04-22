{.push raises: [].}

## Tests for namespace entry descriptor bit-packing (Type A and Type B).

import codetracer_ctfs/namespace_descriptor

proc test_type_a_descriptor_sub_block_roundtrip() =
  ## For each pool_class (0-6), encode with min/max block_num and slot_index,
  ## decode, verify exact roundtrip.
  for pc in 0'u8 .. 6'u8:
    let siBits = slotIndexBits(pc)
    let ubBits = usedBytesBits(pc)
    let maxSlot = uint16((1 shl siBits) - 1)
    let maxUsed = uint16((1 shl ubBits) - 1)

    # Min values
    block:
      let d = encodeTypeASubBlock(0'u64, pc, 0'u16, 0'u16)
      doAssert not d.isGraduated, "sub-block should not be graduated"
      let s = decodeTypeASubBlock(d)
      doAssert s.blockNum == 0
      doAssert s.poolClass == pc
      doAssert s.slotIndex == 0
      doAssert s.usedBytes == 0

    # Max values
    block:
      let maxBlock = (1'u64 shl 48) - 1
      let d = encodeTypeASubBlock(maxBlock, pc, maxSlot, maxUsed)
      doAssert not d.isGraduated
      let s = decodeTypeASubBlock(d)
      doAssert s.blockNum == maxBlock,
        "blockNum mismatch for pc=" & $pc
      doAssert s.poolClass == pc
      doAssert s.slotIndex == maxSlot,
        "slotIndex mismatch for pc=" & $pc & ": got " & $s.slotIndex & " want " & $maxSlot
      doAssert s.usedBytes == maxUsed,
        "usedBytes mismatch for pc=" & $pc & ": got " & $s.usedBytes & " want " & $maxUsed

  echo "PASS: test_type_a_descriptor_sub_block_roundtrip"

proc test_type_a_descriptor_graduated_roundtrip() =
  ## Encode with min/max map_block and data_size. Verify bit 63 is set.
  block:
    let d = encodeTypeAGraduated(0'u32, 0'u32)
    doAssert d.isGraduated, "graduated should have bit 63 set"
    let g = decodeTypeAGraduated(d)
    doAssert g.mapBlock == 0
    doAssert g.dataSize == 0

  block:
    let maxMap = uint32((1'u64 shl 31) - 1)
    let maxData = uint32(0xFFFFFFFF'u64)
    let d = encodeTypeAGraduated(maxMap, maxData)
    doAssert d.isGraduated
    let g = decodeTypeAGraduated(d)
    doAssert g.mapBlock == maxMap,
      "mapBlock mismatch: got " & $g.mapBlock & " want " & $maxMap
    doAssert g.dataSize == maxData,
      "dataSize mismatch: got " & $g.dataSize & " want " & $maxData

  echo "PASS: test_type_a_descriptor_graduated_roundtrip"

proc test_type_a_descriptor_tag_discrimination() =
  ## Create sub-block and graduated descriptors, verify isGraduated
  ## correctly identifies each.
  let sub = encodeTypeASubBlock(12345'u64, 3'u8, 5'u16, 100'u16)
  let grad = encodeTypeAGraduated(999'u32, 65536'u32)
  doAssert not sub.isGraduated, "sub-block should not be graduated"
  doAssert grad.isGraduated, "graduated should be graduated"
  echo "PASS: test_type_a_descriptor_tag_discrimination"

proc test_type_b_descriptor_sub_block_roundtrip() =
  ## For each pool_class, encode with max slot_index and used_bytes,
  ## decode, verify roundtrip.
  for pc in 0'u8 .. 6'u8:
    let siBits = slotIndexBits(pc)
    let ubBits = usedBytesBits(pc)
    let maxSlot = uint16((1 shl siBits) - 1)
    let maxUsed = uint16((1 shl ubBits) - 1)
    let maxBlock = (1'u64 shl 49) - 1

    let d = encodeTypeBSubBlock(maxBlock, pc, maxSlot, maxUsed)
    doAssert not d.isGraduated, "sub-block should not be graduated"
    let s = decodeTypeBSubBlock(d)
    doAssert s.blockNum == maxBlock,
      "blockNum mismatch for pc=" & $pc
    doAssert s.poolClass == pc
    doAssert s.slotIndex == maxSlot,
      "slotIndex mismatch for pc=" & $pc
    doAssert s.usedBytes == maxUsed,
      "usedBytes mismatch for pc=" & $pc

  echo "PASS: test_type_b_descriptor_sub_block_roundtrip"

proc test_type_b_slot_and_used_12bit_invariant() =
  ## For each pool_class, verify slot_index_bits + used_bytes_bits = 12.
  ## Encode max values, verify no overflow (stays in 12 bits).
  for pc in 0'u8 .. 6'u8:
    let siBits = slotIndexBits(pc)
    let ubBits = usedBytesBits(pc)
    doAssert siBits + ubBits == 12,
      "invariant broken for pc=" & $pc & ": " & $siBits & "+" & $ubBits

    let maxSlot = uint16((1 shl siBits) - 1)
    let maxUsed = uint16((1 shl ubBits) - 1)
    let combined = encodeSlotAndUsed(pc, maxSlot, maxUsed)
    doAssert combined <= 0xFFF'u16,
      "overflow for pc=" & $pc & ": combined=" & $combined

    let (decSlot, decUsed) = decodeSlotAndUsed(pc, combined)
    doAssert decSlot == maxSlot
    doAssert decUsed == maxUsed

  echo "PASS: test_type_b_slot_and_used_12bit_invariant"

proc test_type_b_descriptor_graduated_roundtrip() =
  ## Encode with large map_block and data_size, verify roundtrip.
  let mapBlock = 0xDEADBEEFCAFE'u64
  let dataSize = 0x123456789ABCDEF0'u64
  let d = encodeTypeBGraduated(mapBlock, dataSize)
  doAssert d.isGraduated, "graduated should have word0 != 0"
  let g = decodeTypeBGraduated(d)
  doAssert g.mapBlock == mapBlock
  doAssert g.dataSize == dataSize
  echo "PASS: test_type_b_descriptor_graduated_roundtrip"

proc test_type_b_descriptor_map_block_zero_tag() =
  ## Verify map_block == 0 means sub-block, != 0 means graduated.
  ## Edge: block_num=0 (valid sub-block), map_block=1 (smallest graduated).
  let sub = encodeTypeBSubBlock(0'u64, 0'u8, 0'u16, 0'u16)
  doAssert not sub.isGraduated, "word0=0 should be sub-block"
  doAssert sub.word0 == 0

  let grad = encodeTypeBGraduated(1'u64, 0'u64)
  doAssert grad.isGraduated, "word0=1 should be graduated"
  doAssert grad.word0 == 1

  echo "PASS: test_type_b_descriptor_map_block_zero_tag"

proc test_descriptor_block_num_max_capacity() =
  ## Type A with block_num = 2^48 - 1, Type B with block_num = 2^49 - 1,
  ## verify roundtrip at limits.
  block:
    let maxA = (1'u64 shl 48) - 1
    let d = encodeTypeASubBlock(maxA, 0'u8, 0'u16, 0'u16)
    let s = decodeTypeASubBlock(d)
    doAssert s.blockNum == maxA,
      "Type A max blockNum failed: got " & $s.blockNum

  block:
    let maxB = (1'u64 shl 49) - 1
    let d = encodeTypeBSubBlock(maxB, 0'u8, 0'u16, 0'u16)
    let s = decodeTypeBSubBlock(d)
    doAssert s.blockNum == maxB,
      "Type B max blockNum failed: got " & $s.blockNum

  echo "PASS: test_descriptor_block_num_max_capacity"

when isMainModule:
  test_type_a_descriptor_sub_block_roundtrip()
  test_type_a_descriptor_graduated_roundtrip()
  test_type_a_descriptor_tag_discrimination()
  test_type_b_descriptor_sub_block_roundtrip()
  test_type_b_slot_and_used_12bit_invariant()
  test_type_b_descriptor_graduated_roundtrip()
  test_type_b_descriptor_map_block_zero_tag()
  test_descriptor_block_num_max_capacity()
  echo "All namespace descriptor tests passed."
