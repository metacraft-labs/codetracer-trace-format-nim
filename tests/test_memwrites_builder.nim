{.push raises: [].}

import std/unittest
import codetracer_trace_writer/memwrites_builder

suite "memwrites CoW namespace builder":
  test "serializes nsb1 and decodes payloads by address":
    let records = @[
      MemwriteCowRecord(intervalId: 7, tick: 70, pc: 0x7000, address: 0x4000,
                        size: 8, oldValue: 0x10, newValue: 0x11),
      MemwriteCowRecord(intervalId: 3, tick: 30, pc: 0x3000, address: 0x4000,
                        size: 4, oldValue: 0x20, newValue: 0x21),
      MemwriteCowRecord(intervalId: 7, tick: 71, pc: 0x7001, address: 0x4008,
                        size: 1, oldValue: 0x30, newValue: 0x31),
    ]

    let imageRes = serializeMemwritesCowNamespace(records)
    require imageRes.isOk
    let image = imageRes.get()
    check image.len >= 4
    check image[0] == byte('N')
    check image[1] == byte('S')
    check image[2] == byte('B')
    check image[3] == byte('1')

    let addrRes = decodeCowMemwritesPayloadForTest(image, 0x4000)
    require addrRes.isOk
    let addrWrites = addrRes.get()
    check addrWrites.len == 2
    check addrWrites[0].intervalId == 3
    check addrWrites[0].tick == 30
    check addrWrites[0].pc == 0x3000
    check addrWrites[0].size == 4
    check addrWrites[0].oldValue == 0x20
    check addrWrites[0].newValue == 0x21
    check addrWrites[1].intervalId == 7
    check addrWrites[1].tick == 70

    let allRes = decodeCowMemwritesNamespace(image)
    require allRes.isOk
    let all = allRes.get()
    check all.len == 3
    check all[0].address == 0x4000
    check all[0].tick == 30
    check all[1].address == 0x4000
    check all[1].tick == 70
    check all[2].address == 0x4008
    check all[2].tick == 71

  test "empty input still emits readable nsb1 namespace":
    let imageRes = serializeMemwritesCowNamespace([])
    require imageRes.isOk
    let image = imageRes.get()
    check image.len >= 4
    check image[0] == byte('N')
    check image[1] == byte('S')
    check image[2] == byte('B')
    check image[3] == byte('1')

    let allRes = decodeCowMemwritesNamespace(image)
    require allRes.isOk
    check allRes.get().len == 0

