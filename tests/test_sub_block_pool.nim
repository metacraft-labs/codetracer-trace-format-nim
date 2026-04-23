{.push raises: [].}

## Tests for sub-block pool allocation, free list management, promotion,
## and graduation.

import std/[algorithm, sets, times]
import results
import codetracer_ctfs/sub_block_pool
import codetracer_ctfs/namespace_descriptor
import codetracer_ctfs/types

proc test_sub_block_allocate_free() =
  ## Allocate 10K slots of varying pool classes, verify uniqueness,
  ## free half, re-allocate, verify reuse, write/read data integrity.
  var pm = initSubBlockPoolManager()

  type AllocKey = tuple[blockNum: uint64, slotIndex: uint16, poolClass: uint8]
  var allocs: seq[SubBlockAllocation]
  var seen: HashSet[AllocKey]

  # Allocate 10K slots across pool classes.
  for i in 0 ..< 10_000:
    let pc = uint8(i mod NumPoolClasses)
    let res = pm.allocate(pc)
    doAssert res.isOk, "allocate failed at i=" & $i & ": " & res.error
    let a = res.get()
    let key: AllocKey = (a.blockNum, a.slotIndex, a.poolClass)
    doAssert key notin seen, "duplicate allocation at i=" & $i
    seen.incl(key)
    allocs.add(a)

  doAssert allocs.len == 10_000

  # Free the first half.
  var freed: seq[AllocKey]
  for i in 0 ..< 5_000:
    let a = allocs[i]
    freed.add((a.blockNum, a.slotIndex, a.poolClass))
    let res = pm.free(a)
    doAssert res.isOk, "free failed at i=" & $i & ": " & res.error

  # Re-allocate 5K slots — freed slots should be reused.
  var reusedCount = 0
  for i in 0 ..< 5_000:
    let pc = uint8(i mod NumPoolClasses)
    let res = pm.allocate(pc)
    doAssert res.isOk, "re-allocate failed at i=" & $i & ": " & res.error
    let a = res.get()
    let key: AllocKey = (a.blockNum, a.slotIndex, a.poolClass)
    # Check if this was a previously freed slot.
    for f in freed:
      if f == key:
        reusedCount += 1
        break
    allocs.add(a)

  doAssert reusedCount > 0, "expected some freed slots to be reused"

  # Write unique data to all live allocations (indices 5000..14999), read back.
  for i in 5_000 ..< allocs.len:
    let a = allocs[i]
    let pSize = poolSize(a.poolClass)
    var data = newSeq[byte](pSize)
    # Fill with a pattern based on index.
    for j in 0 ..< pSize:
      data[j] = byte((i + j) mod 256)
    let wRes = pm.writeSlot(a, data)
    doAssert wRes.isOk, "writeSlot failed at i=" & $i & ": " & wRes.error

  for i in 5_000 ..< allocs.len:
    let a = allocs[i]
    let pSize = poolSize(a.poolClass)
    var output = newSeq[byte](pSize)
    let rRes = pm.readSlot(a, output)
    doAssert rRes.isOk, "readSlot failed at i=" & $i & ": " & rRes.error
    for j in 0 ..< pSize:
      doAssert output[j] == byte((i + j) mod 256),
        "data corruption at i=" & $i & " j=" & $j

  echo "PASS: test_sub_block_allocate_free"

proc test_sub_block_promotion() =
  ## Allocate a 32B slot, write data, promote through every class up to 2048B,
  ## then graduate. Verify data preserved at each step.
  var pm = initSubBlockPoolManager()

  # Start with pool class 0 (32B).
  let allocRes = pm.allocate(0'u8)
  doAssert allocRes.isOk
  var alloc = allocRes.get()

  # Write 30 bytes of data.
  var data: array[30, byte]
  for i in 0 ..< 30:
    data[i] = byte(0xA0 + i)
  let wRes = pm.writeSlot(alloc, data)
  doAssert wRes.isOk, "initial writeSlot failed: " & wRes.error

  # Promote through each class: 0->1->2->3->4->5->6
  for newPc in 1'u8 .. 6'u8:
    let promRes = pm.promote(alloc, newPc)
    doAssert promRes.isOk, "promote to class " & $newPc & " failed: " & promRes.error
    alloc = promRes.get()
    doAssert alloc.poolClass == newPc

    # Verify original 30 bytes are preserved.
    let pSize = poolSize(newPc)
    var output = newSeq[byte](pSize)
    let rRes = pm.readSlot(alloc, output)
    doAssert rRes.isOk
    for i in 0 ..< 30:
      doAssert output[i] == byte(0xA0 + i),
        "data lost after promote to class " & $newPc & " at byte " & $i

  # Graduate to standalone data.
  let gradRes = pm.graduate(alloc)
  doAssert gradRes.isOk, "graduate failed: " & gradRes.error
  let gradData = gradRes.get()
  # Verify original 30 bytes.
  for i in 0 ..< 30:
    doAssert gradData[i] == byte(0xA0 + i),
      "data lost after graduation at byte " & $i

  echo "PASS: test_sub_block_promotion"

proc test_free_list_next_pointer_roundtrip() =
  ## For each pool class, encode a next pointer, decode it, verify roundtrip.
  ## Also verify end-of-list marker.
  for pc in 0'u8 .. 6'u8:
    let pSize = poolSize(pc)
    doAssert pSize >= 6, "pool size must fit 6-byte next pointer"

    var buf = newSeq[byte](pSize)

    # Test with a specific value.
    writeNextPtr(buf, 0, 12345'u32, 42'u16)
    let (bk, si) = readNextPtr(buf, 0)
    doAssert bk == 12345'u32,
      "blockIdx mismatch for pc=" & $pc & ": got " & $bk
    doAssert si == 42'u16,
      "slotIndex mismatch for pc=" & $pc & ": got " & $si

    # Test end-of-list marker (0, 0).
    writeNextPtr(buf, 0, 0'u32, 0'u16)
    let (bk0, si0) = readNextPtr(buf, 0)
    doAssert bk0 == 0'u32
    doAssert si0 == 0'u16

  echo "PASS: test_free_list_next_pointer_roundtrip"

proc test_slot_and_used_12bit_roundtrip() =
  ## For each pool class, verify the 12-bit split matches expectations
  ## in the pool context.
  for pc in 0'u8 .. 6'u8:
    let siBits = slotIndexBits(pc)
    let ubBits = usedBytesBits(pc)
    doAssert siBits + ubBits == 12, "12-bit invariant broken for pc=" & $pc

    let maxSlot = uint16((1 shl siBits) - 1)
    let maxUsed = uint16((1 shl ubBits) - 1)

    # Verify max slot matches SlotsPerBlock - 1.
    doAssert int(maxSlot) >= SlotsPerBlock[int(pc)] - 1,
      "max slot index too small for pc=" & $pc &
      ": maxSlot=" & $maxSlot & " slotsPerBlock=" & $SlotsPerBlock[int(pc)]

    let combined = encodeSlotAndUsed(pc, maxSlot, maxUsed)
    doAssert combined <= 0xFFF'u16

    let (decSlot, decUsed) = decodeSlotAndUsed(pc, combined)
    doAssert decSlot == maxSlot
    doAssert decUsed == maxUsed

  echo "PASS: test_slot_and_used_12bit_roundtrip"

proc test_sub_block_slot_index_max() =
  ## For each pool class, allocate the maximum number of slots in one block,
  ## verify slot indices, write unique data, read back, verify no overlap.
  var pm = initSubBlockPoolManager()

  for pc in 0'u8 .. 6'u8:
    let slots = SlotsPerBlock[int(pc)]
    var allocs: seq[SubBlockAllocation]

    # Allocate exactly one block's worth of slots.
    for i in 0 ..< slots:
      let res = pm.allocate(pc)
      doAssert res.isOk, "allocate failed for pc=" & $pc & " i=" & $i
      let a = res.get()
      allocs.add(a)

    # Verify slot indices are 0..slots-1 (all within the same block).
    var slotIndices: seq[uint16]
    for a in allocs:
      slotIndices.add(a.slotIndex)
    slotIndices.sort()
    for i in 0 ..< slots:
      doAssert slotIndices[i] == uint16(i),
        "slot index mismatch for pc=" & $pc & ": expected " & $i & " got " & $slotIndices[i]

    # Write unique data to each slot.
    let pSize = poolSize(pc)
    for i in 0 ..< slots:
      var data = newSeq[byte](pSize)
      for j in 0 ..< pSize:
        data[j] = byte((i * 37 + j) mod 256)
      let wRes = pm.writeSlot(allocs[i], data)
      doAssert wRes.isOk

    # Read back and verify.
    for i in 0 ..< slots:
      var output = newSeq[byte](pSize)
      let rRes = pm.readSlot(allocs[i], output)
      doAssert rRes.isOk
      for j in 0 ..< pSize:
        doAssert output[j] == byte((i * 37 + j) mod 256),
          "data overlap for pc=" & $pc & " slot=" & $i & " byte=" & $j

  echo "PASS: test_sub_block_slot_index_max"

proc test_sub_block_byte_offset_from_slot() =
  ## For each pool class and slot index, verify the byte offset is within
  ## [0, 4095] (i.e., within a single block).
  for pc in 0'u8 .. 6'u8:
    let pSize = poolSize(pc)
    let slots = SlotsPerBlock[int(pc)]
    for si in 0 ..< slots:
      let offset = si * pSize
      doAssert offset >= 0 and offset < int(DefaultBlockSize),
        "offset out of block for pc=" & $pc & " si=" & $si & ": " & $offset
      doAssert offset + pSize <= int(DefaultBlockSize),
        "slot extends past block for pc=" & $pc & " si=" & $si

  echo "PASS: test_sub_block_byte_offset_from_slot"

proc bench_sub_block_allocation_throughput() =
  ## Allocate 1M sub-blocks (alternating pool classes), measure throughput.
  const N = 1_000_000
  var pm = initSubBlockPoolManager()

  let start = cpuTime()
  for i in 0 ..< N:
    let pc = uint8(i mod NumPoolClasses)
    let res = pm.allocate(pc)
    doAssert res.isOk
  let elapsed = cpuTime() - start

  let throughput = float(N) / elapsed
  echo "{\"benchmark\": \"sub_block_allocation_throughput\", " &
    "\"allocations\": " & $N & ", " &
    "\"elapsed_sec\": " & $elapsed & ", " &
    "\"allocs_per_sec\": " & $throughput & "}"
  # Algorithmic correctness threshold: 500K allocs/sec catches O(n²) regressions.
  # Local target is 1M+; CI runners are 40-70% slower due to shared resources.
  doAssert throughput > 500_000.0,
    "allocation throughput too low: " & $throughput & " allocs/sec (min 500K)"
  echo "PASS: bench_sub_block_allocation_throughput"

when isMainModule:
  test_sub_block_allocate_free()
  test_sub_block_promotion()
  test_free_list_next_pointer_roundtrip()
  test_slot_and_used_12bit_roundtrip()
  test_sub_block_slot_index_max()
  test_sub_block_byte_offset_from_slot()
  bench_sub_block_allocation_throughput()
  echo "All sub-block pool tests passed."
