{.push raises: [].}

## CTFS space analyzer: diagnostic functions for analyzing container space
## utilization and namespace memory usage.

import results
import ./types
import ./container
import ./namespace
import ./namespace_descriptor
import ./sub_block_pool
import ./btree
import ./base40
import std/[algorithm, json]

type
  FileStats* = object
    name*: string
    blockCount*: int
    dataBytes*: uint64
    allocatedBytes*: uint64  # blockCount * blockSize
    utilization*: float

  NamespaceStats* = object
    name*: string
    entryCount*: uint64
    sizeMin*, sizeMedian*, sizeP95*, sizeP99*, sizeMax*: int
    sizeHistogram*: seq[int]  # all entry sizes for histogram

  PoolStats* = object
    poolClass*: uint8
    poolSize*: int
    allocatedSlots*: int
    freeSlots*: int
    totalUsedBytes*: uint64
    avgUsedBytes*: float
    internalFragmentation*: float  # (allocated - used) / allocated

  BTreeStatsObj* = object
    depth*: int
    nodeCount*: int
    totalEntries*: uint64

  SpaceReport* = object
    totalBlocks*: int
    totalBytes*: uint64
    headerBytes*: int
    fileEntryBytes*: int
    files*: seq[FileStats]
    namespaces*: seq[NamespaceStats]
    pools*: seq[PoolStats]
    btreeStats*: seq[BTreeStatsObj]

proc countFileBlocks(
    data: openArray[byte], mapBlock: uint64,
    blockSize: int): int =
  ## Count how many data blocks a file uses by walking the mapping chain.
  let usable = uint64(blockSize) div 8 - 1
  var count = 0
  var blockIdx: uint64 = 0

  # Count blocks by iterating through all possible indices
  # until we hit a null pointer or go out of bounds.
  block walkBlocks:
    while true:
      # Navigate to the data block for blockIdx using same logic as readInternalFile.
      var idx = blockIdx
      var navLevelBlock = mapBlock
      var navLevel: uint32 = 1

      block findLevel:
        while true:
          var cap: uint64 = 1
          for l in 0'u32 ..< navLevel:
            cap = cap * usable
          if idx < cap:
            break findLevel
          idx -= cap
          navLevel += 1
          if navLevel > MaxChainLevels:
            break walkBlocks
          let chainOff = int(navLevelBlock) * blockSize + int(usable) * 8
          if chainOff + 8 > data.len:
            break walkBlocks
          let chainPtr = readU64LE(data, chainOff)
          if chainPtr == 0:
            break walkBlocks
          navLevelBlock = chainPtr

      var navBlock = navLevelBlock
      var curLevel = navLevel
      var navIdx = idx
      while curLevel > 1:
        var subCap: uint64 = 1
        for l in 0'u32 ..< (curLevel - 1):
          subCap = subCap * usable
        let entryIdx = navIdx div subCap
        let subIdx = navIdx mod subCap
        let childOff = int(navBlock) * blockSize + int(entryIdx) * 8
        if childOff + 8 > data.len:
          break walkBlocks
        let childBlock = readU64LE(data, childOff)
        if childBlock == 0:
          break walkBlocks
        navBlock = childBlock
        navIdx = subIdx
        curLevel -= 1

      let ptrOff = int(navBlock) * blockSize + int(navIdx) * 8
      if ptrOff + 8 > data.len:
        break walkBlocks
      let dataBlock = readU64LE(data, ptrOff)
      if dataBlock == 0:
        break walkBlocks

      count += 1
      blockIdx += 1

  count

proc analyzeCtfs*(data: openArray[byte],
                  blockSize: int = int(DefaultBlockSize)): Result[SpaceReport, string] =
  ## Analyze a raw .ct file's container structure.
  ## Reports per-file block count, data bytes, utilization.
  if data.len < HeaderSize + ExtHeaderSize:
    return err("data too small for CTFS header")

  if not hasCtfsMagic(data):
    return err("invalid CTFS magic")

  # Read max root entries from extended header.
  var maxEntries = int(DefaultMaxRootEntries)
  if data.len >= HeaderSize + ExtHeaderSize:
    var arr: array[4, byte]
    for i in 0 ..< 4:
      arr[i] = data[HeaderSize + 4 + i]
    maxEntries = int(fromBytesLE(uint32, arr))

  var report: SpaceReport
  report.totalBytes = uint64(data.len)
  report.totalBlocks = data.len div blockSize
  report.headerBytes = HeaderSize + ExtHeaderSize
  report.fileEntryBytes = maxEntries * FileEntrySize

  for i in 0 ..< maxEntries:
    let off = HeaderSize + ExtHeaderSize + i * FileEntrySize
    if off + FileEntrySize > data.len:
      break

    let fileSize = readU64LE(data, off)
    let mapBlock = readU64LE(data, off + 8)
    let encodedName = readU64LE(data, off + 16)

    # Skip empty entries.
    if fileSize == 0 and mapBlock == 0 and encodedName == 0:
      continue

    let name = base40Decode(encodedName)
    let blocks = countFileBlocks(data, mapBlock, blockSize)

    var fs: FileStats
    fs.name = name
    fs.dataBytes = fileSize
    fs.blockCount = blocks
    fs.allocatedBytes = uint64(blocks) * uint64(blockSize)
    if fs.allocatedBytes > 0:
      fs.utilization = float(fs.dataBytes) / float(fs.allocatedBytes)
    else:
      fs.utilization = 0.0

    report.files.add(fs)

  ok(report)

proc analyzeNamespace*(ns: Namespace): NamespaceStats =
  ## Analyze an in-memory namespace's entry size distribution.
  var stats: NamespaceStats
  stats.name = ns.name
  stats.entryCount = ns.count

  if ns.count == 0:
    return stats

  # Iterate all entries via the B-tree range scan to collect sizes.
  var sizes: seq[int]
  for entry in ns.items(0'u64, high(uint64)):
    sizes.add(entry.data.len)

  sort(sizes)

  stats.sizeHistogram = sizes
  stats.sizeMin = sizes[0]
  stats.sizeMax = sizes[^1]
  stats.sizeMedian = sizes[sizes.len div 2]

  let p95Idx = min(sizes.len - 1, (sizes.len * 95) div 100)
  let p99Idx = min(sizes.len - 1, (sizes.len * 99) div 100)
  stats.sizeP95 = sizes[p95Idx]
  stats.sizeP99 = sizes[p99Idx]

  stats

proc analyzePool*(pm: SubBlockPoolManager): seq[PoolStats] =
  ## Analyze sub-block pool utilization for all pool classes.
  for pc in 0'u8 ..< uint8(NumPoolClasses):
    let total = pm.totalAllocatedSlots(pc)
    if total == 0:
      continue

    let free = pm.totalFreeSlots(pc)
    let used = total - free
    let pSize = poolSize(pc)

    var ps: PoolStats
    ps.poolClass = pc
    ps.poolSize = pSize
    ps.allocatedSlots = used  # slots that are in use
    ps.freeSlots = free
    # We don't have per-slot used bytes tracking at the pool level,
    # so we report based on the pool class size.
    ps.totalUsedBytes = uint64(used) * uint64(pSize)
    ps.avgUsedBytes = if used > 0: float(ps.totalUsedBytes) / float(used)
                      else: 0.0
    # Internal fragmentation cannot be computed precisely without per-slot
    # used byte tracking; report 0 here. Callers who have namespace-level
    # info can compute fragmentation from entry sizes vs pool sizes.
    ps.internalFragmentation = 0.0

    result.add(ps)

proc analyzePoolWithSizes*(
    pm: SubBlockPoolManager,
    entrySizes: openArray[int]): seq[PoolStats] =
  ## Analyze sub-block pool utilization using known entry sizes to compute
  ## internal fragmentation accurately.
  # Group entry sizes by pool class.
  var classSizes: array[7, seq[int]]

  for size in entrySizes:
    var pc: uint8 = 0
    while pc < 6 and (poolSize(pc) < size or
          size > int((1'u16 shl usedBytesBits(pc)) - 1)):
      pc += 1
    classSizes[int(pc)].add(size)

  for pc in 0'u8 ..< uint8(NumPoolClasses):
    let total = pm.totalAllocatedSlots(pc)
    if total == 0:
      continue

    let free = pm.totalFreeSlots(pc)
    let used = total - free
    let pSize = poolSize(pc)
    let sizes = classSizes[int(pc)]

    var ps: PoolStats
    ps.poolClass = pc
    ps.poolSize = pSize
    ps.allocatedSlots = used
    ps.freeSlots = free

    var totalUsed: uint64 = 0
    for s in sizes:
      totalUsed += uint64(s)
    ps.totalUsedBytes = totalUsed

    ps.avgUsedBytes = if sizes.len > 0: float(totalUsed) / float(sizes.len)
                      else: 0.0

    let totalAllocBytes = uint64(used) * uint64(pSize)
    if totalAllocBytes > 0:
      ps.internalFragmentation = float(totalAllocBytes - totalUsed) / float(totalAllocBytes)
    else:
      ps.internalFragmentation = 0.0

    result.add(ps)

proc analyzeBTree*(tree: BTree): BTreeStatsObj =
  ## Analyze B-tree structure.
  BTreeStatsObj(
    depth: tree.depth,
    nodeCount: tree.nodeCount,
    totalEntries: tree.count,
  )

proc toJson*(report: SpaceReport): string {.raises: [].} =
  ## Convert report to JSON string.
  var j = newJObject()

  j["totalBlocks"] = newJInt(report.totalBlocks)
  j["totalBytes"] = newJInt(int(report.totalBytes))
  j["headerBytes"] = newJInt(report.headerBytes)
  j["fileEntryBytes"] = newJInt(report.fileEntryBytes)

  var filesArr = newJArray()
  for f in report.files:
    var fj = newJObject()
    fj["name"] = newJString(f.name)
    fj["blockCount"] = newJInt(f.blockCount)
    fj["dataBytes"] = newJInt(int(f.dataBytes))
    fj["allocatedBytes"] = newJInt(int(f.allocatedBytes))
    fj["utilization"] = newJFloat(f.utilization)
    filesArr.add(fj)
  j["files"] = filesArr

  var nsArr = newJArray()
  for ns in report.namespaces:
    var nj = newJObject()
    nj["name"] = newJString(ns.name)
    nj["entryCount"] = newJInt(int(ns.entryCount))
    nj["sizeMin"] = newJInt(ns.sizeMin)
    nj["sizeMedian"] = newJInt(ns.sizeMedian)
    nj["sizeP95"] = newJInt(ns.sizeP95)
    nj["sizeP99"] = newJInt(ns.sizeP99)
    nj["sizeMax"] = newJInt(ns.sizeMax)
    nsArr.add(nj)
  j["namespaces"] = nsArr

  var poolsArr = newJArray()
  for p in report.pools:
    var pj = newJObject()
    pj["poolClass"] = newJInt(int(p.poolClass))
    pj["poolSize"] = newJInt(p.poolSize)
    pj["allocatedSlots"] = newJInt(p.allocatedSlots)
    pj["freeSlots"] = newJInt(p.freeSlots)
    pj["totalUsedBytes"] = newJInt(int(p.totalUsedBytes))
    pj["avgUsedBytes"] = newJFloat(p.avgUsedBytes)
    pj["internalFragmentation"] = newJFloat(p.internalFragmentation)
    poolsArr.add(pj)
  j["pools"] = poolsArr

  var btArr = newJArray()
  for bt in report.btreeStats:
    var bj = newJObject()
    bj["depth"] = newJInt(bt.depth)
    bj["nodeCount"] = newJInt(bt.nodeCount)
    bj["totalEntries"] = newJInt(int(bt.totalEntries))
    btArr.add(bj)
  j["btreeStats"] = btArr

  $j

proc formatFloat(f: float, decimals: int = 2): string {.raises: [].} =
  ## Simple float formatting without exceptions.
  # Use integer math for percentage display.
  let factor =
    if decimals == 1: 10
    elif decimals == 2: 100
    elif decimals == 3: 1000
    else: 100
  let scaled = int(f * float(factor) + 0.5)
  let intPart = scaled div factor
  let fracPart = scaled mod factor
  var fracStr = $fracPart
  while fracStr.len < decimals:
    fracStr = "0" & fracStr
  $intPart & "." & fracStr

proc toText*(report: SpaceReport): string {.raises: [].} =
  ## Convert report to human-readable text.
  var lines: seq[string]
  lines.add("=== CTFS Space Report ===")
  lines.add("")
  lines.add("Total blocks: " & $report.totalBlocks)
  lines.add("Total bytes:  " & $report.totalBytes)
  lines.add("Header bytes: " & $report.headerBytes)
  lines.add("File entry bytes: " & $report.fileEntryBytes)
  lines.add("")

  if report.files.len > 0:
    lines.add("--- Files ---")
    for f in report.files:
      let pct = formatFloat(f.utilization * 100.0, 1)
      lines.add("  " & f.name & ": " & $f.dataBytes & " bytes, " &
                $f.blockCount & " blocks, " &
                $f.allocatedBytes & " allocated, " &
                pct & "% utilization")
    lines.add("")

  if report.namespaces.len > 0:
    lines.add("--- Namespaces ---")
    for ns in report.namespaces:
      lines.add("  " & ns.name & ": " & $ns.entryCount & " entries")
      lines.add("    size min=" & $ns.sizeMin & " median=" & $ns.sizeMedian &
                " p95=" & $ns.sizeP95 & " p99=" & $ns.sizeP99 &
                " max=" & $ns.sizeMax)
    lines.add("")

  if report.pools.len > 0:
    lines.add("--- Pools ---")
    for p in report.pools:
      let fragPct = formatFloat(p.internalFragmentation * 100.0, 1)
      lines.add("  class " & $p.poolClass & " (" & $p.poolSize & "B): " &
                $p.allocatedSlots & " used, " & $p.freeSlots & " free, " &
                fragPct & "% fragmentation")
    lines.add("")

  if report.btreeStats.len > 0:
    lines.add("--- B-Trees ---")
    for bt in report.btreeStats:
      lines.add("  depth=" & $bt.depth & " nodes=" & $bt.nodeCount &
                " entries=" & $bt.totalEntries)
    lines.add("")

  var output = ""
  for i, line in lines:
    if i > 0:
      output.add("\n")
    output.add(line)
  output
