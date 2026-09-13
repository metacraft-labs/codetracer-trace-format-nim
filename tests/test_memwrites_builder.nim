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

  test "image stays proportional to the address count":
    ## A SPACE gate, because correctness cannot see this defect.
    ##
    ## The builder is a two-pass construction over a known, sorted, unique key
    ## set, and it must build each B-tree with `bulkLoad`. Built instead with a
    ## per-key `insertAndCommit`, every key publishes a copy-on-write commit and
    ## leaves its superseded spine pages in the image: the tree carried ~7.6 KiB
    ## per address rather than ~64 B, and 100k addresses cost 1.18 GB of image,
    ## 8.2 GiB of RSS and 41 s instead of 6.3 MB, 71 MiB and 0.13 s. Every
    ## correctness test above passed in both states, which is exactly why this
    ## one measures bytes.
    ##
    ## A ladder, not a single point: the failure mode is that cost per key
    ## GROWS with the key count, and one N cannot show that. Deliberately a
    ## SPACE assertion and not a timing one — image size is a deterministic
    ## function of the input, so unlike this repo's throughput gates it cannot
    ## flake on a busy host.
    for n in [1_000, 10_000]:
      var records = newSeq[MemwriteCowRecord](n)
      for i in 0 ..< n:
        records[i] = MemwriteCowRecord(
          intervalId: 1, tick: uint64(i), pc: 0x400000'u64 + uint64(i),
          address: 0x7f0000000000'u64 + uint64(i) * 8,
          size: 8, oldValue: uint64(i), newValue: uint64(i) + 1)

      let imageRes = serializeMemwritesCowNamespace(records)
      require imageRes.isOk
      let image = imageRes.get()
      let bytesPerKey = image.len div n

      # One 40-byte record plus a 16-byte descriptor per address is 56 B; the
      # B-tree pages and the page-aligned tail put the bulk-loaded figure at
      # ~64-73 B. 512 B leaves ample room for packing changes while still
      # sitting ~15x below the ~7.6 KiB the per-key build produced.
      check bytesPerKey < 512

      # Content still round-trips at scale.
      let allRes = decodeCowMemwritesNamespace(image)
      require allRes.isOk
      check allRes.get().len == n

