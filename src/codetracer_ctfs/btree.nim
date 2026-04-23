{.push raises: [].}

## In-memory B-tree for namespace key lookup.
##
## Maps uint64 keys to fixed-size descriptor values (8 bytes for Type A,
## 16 bytes for Type B). Each node conceptually represents one 4096-byte
## CTFS block. Block-backed storage is deferred to M9.
##
## The tree supports sorted bulk-loading (common case) as well as
## arbitrary-order insertion.

import std/deques
import results

export results

const
  BlockSize = 4096
  NodeHeaderSize = 8  ## overhead per node (e.g. node type tag + count)

type
  BTreeEntry* = object
    key*: uint64
    descriptor*: seq[byte]

  BTreeNode = ref object
    isLeaf: bool
    keys: seq[uint64]
    # Leaf-only: descriptors parallel to keys
    descriptors: seq[seq[byte]]
    # Internal-only: children (len = keys.len + 1)
    children: seq[BTreeNode]

  BTree* = object
    descriptorSize*: int  ## 8 or 16
    order*: int           ## max entries per node (leaf order)
    root: BTreeNode
    count: uint64

# -----------------------------------------------------------------------------
# Construction
# -----------------------------------------------------------------------------

proc newLeaf(): BTreeNode =
  BTreeNode(isLeaf: true)

proc newInternal(): BTreeNode =
  BTreeNode(isLeaf: false)

proc initBTree*(descriptorSize: int): BTree =
  ## Create a new empty B-tree. descriptorSize must be 8 (Type A) or 16
  ## (Type B).
  let entrySize = 8 + descriptorSize  # key + descriptor
  let order = (BlockSize - NodeHeaderSize) div entrySize
  BTree(
    descriptorSize: descriptorSize,
    order: order,
    root: newLeaf(),
    count: 0,
  )

proc count*(tree: BTree): uint64 {.inline.} =
  tree.count

# -----------------------------------------------------------------------------
# Lookup
# -----------------------------------------------------------------------------

proc lowerBound(keys: seq[uint64], key: uint64): int =
  ## Binary search: returns index of first element >= key.
  var lo = 0
  var hi = keys.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    if keys[mid] < key:
      lo = mid + 1
    else:
      hi = mid
  lo

proc lookup*(tree: BTree, key: uint64): Result[seq[byte], string] =
  ## Look up a key and return its descriptor, or an error if not found.
  var node = tree.root
  while true:
    let idx = lowerBound(node.keys, key)
    if node.isLeaf:
      if idx < node.keys.len and node.keys[idx] == key:
        return ok(node.descriptors[idx])
      else:
        return err("key not found")
    else:
      if idx < node.keys.len and node.keys[idx] == key:
        # exact match on internal key — descend right
        node = node.children[idx + 1]
      else:
        node = node.children[idx]

# -----------------------------------------------------------------------------
# Insertion (with split)
# -----------------------------------------------------------------------------

type
  SplitResult = object
    didSplit: bool
    wasUpdate: bool       ## true when the key already existed (no new entry)
    promotedKey: uint64
    newRight: BTreeNode

proc insertNonFull(
    node: BTreeNode, key: uint64, desc: seq[byte],
    order: int): SplitResult

proc splitLeaf(node: BTreeNode, order: int): SplitResult =
  let mid = node.keys.len div 2
  let right = newLeaf()
  right.keys = node.keys[mid ..< node.keys.len]
  right.descriptors = node.descriptors[mid ..< node.descriptors.len]
  let promotedKey = right.keys[0]
  node.keys.setLen(mid)
  node.descriptors.setLen(mid)
  SplitResult(didSplit: true, promotedKey: promotedKey, newRight: right)

proc splitInternal(node: BTreeNode, order: int): SplitResult =
  let mid = node.keys.len div 2
  let promotedKey = node.keys[mid]
  let right = newInternal()
  right.keys = node.keys[mid + 1 ..< node.keys.len]
  right.children = node.children[mid + 1 ..< node.children.len]
  node.keys.setLen(mid)
  node.children.setLen(mid + 1)
  SplitResult(didSplit: true, promotedKey: promotedKey, newRight: right)

proc insertNonFull(
    node: BTreeNode, key: uint64, desc: seq[byte],
    order: int): SplitResult =
  if node.isLeaf:
    let idx = lowerBound(node.keys, key)
    if idx < node.keys.len and node.keys[idx] == key:
      # Update existing key
      node.descriptors[idx] = desc
      return SplitResult(didSplit: false, wasUpdate: true)
    node.keys.insert(key, idx)
    node.descriptors.insert(desc, idx)
    if node.keys.len > order:
      return splitLeaf(node, order)
    return SplitResult(didSplit: false)
  else:
    var idx = lowerBound(node.keys, key)
    if idx < node.keys.len and node.keys[idx] == key:
      idx = idx + 1  # go right on exact match
    let sr = insertNonFull(node.children[idx], key, desc, order)
    if sr.didSplit:
      node.keys.insert(sr.promotedKey, idx)
      node.children.insert(sr.newRight, idx + 1)
      if node.keys.len > order:
        var split = splitInternal(node, order)
        split.wasUpdate = sr.wasUpdate
        return split
    return SplitResult(didSplit: false, wasUpdate: sr.wasUpdate)

proc insert*(tree: var BTree, key: uint64, descriptor: openArray[byte]) =
  ## Insert a key-descriptor pair. If the key already exists, its descriptor
  ## is updated.
  let desc = @descriptor
  let sr = insertNonFull(tree.root, key, desc, tree.order)
  if sr.didSplit:
    let newRoot = newInternal()
    newRoot.keys = @[sr.promotedKey]
    newRoot.children = @[tree.root, sr.newRight]
    tree.root = newRoot
  if not sr.wasUpdate:
    tree.count += 1

# -----------------------------------------------------------------------------
# Range scan
# -----------------------------------------------------------------------------

iterator rangeIter*(tree: BTree, lo, hi: uint64): BTreeEntry =
  ## Yield all entries with keys in [lo, hi] in ascending order.
  var stack: seq[(BTreeNode, int)]
  stack.add((tree.root, 0))
  while stack.len > 0:
    let (node, _) = stack[^1]
    stack.setLen(stack.len - 1)
    if node.isLeaf:
      let startIdx = lowerBound(node.keys, lo)
      for i in startIdx ..< node.keys.len:
        if node.keys[i] > hi:
          break
        yield BTreeEntry(key: node.keys[i], descriptor: node.descriptors[i])
    else:
      # Push children in reverse order so left-most is processed first.
      let startChild = lowerBound(node.keys, lo)
      # We need to visit children[startChild] through the child after the last
      # key <= hi.
      var endChild = lowerBound(node.keys, hi)
      if endChild < node.keys.len and node.keys[endChild] <= hi:
        endChild += 1
      # Push in reverse so left child is on top of stack.
      for ci in countdown(endChild, startChild):
        if ci < node.children.len:
          stack.add((node.children[ci], 0))

proc rangeScan*(tree: BTree, lo, hi: uint64,
                output: var openArray[BTreeEntry]): int =
  ## Collect entries with keys in [lo, hi] into output.
  ## Returns the number of entries written (capped by output.len).
  var count = 0
  for entry in tree.rangeIter(lo, hi):
    if count >= output.len:
      break
    output[count] = entry
    count += 1
  count

# -----------------------------------------------------------------------------
# Stats accessors (for space analyzer)
# -----------------------------------------------------------------------------

proc depth*(tree: BTree): int =
  ## Compute the depth of the B-tree (1 = root is leaf).
  var d = 1
  var node = tree.root
  while not node.isLeaf:
    d += 1
    if node.children.len > 0:
      node = node.children[0]
    else:
      break
  d

proc nodeCount*(tree: BTree): int =
  ## Count total number of nodes in the B-tree (BFS traversal).
  var count = 0
  var stack: seq[BTreeNode]
  stack.add(tree.root)
  while stack.len > 0:
    let node = stack[^1]
    stack.setLen(stack.len - 1)
    count += 1
    if not node.isLeaf:
      for child in node.children:
        stack.add(child)
  count

# -----------------------------------------------------------------------------
# Serialization to/from CTFS blocks
# -----------------------------------------------------------------------------
#
# Each node occupies exactly one BlockSize (4096-byte) region.
# Nodes are laid out in BFS order so that the root is always at index 0.
#
# Node on-disk format:
#   Byte  0:    isLeaf (0 = internal, 1 = leaf)
#   Byte  1:    reserved (0)
#   Bytes 2-3:  count (uint16 LE) — number of keys
#   Bytes 4-7:  reserved (0)
#   Bytes 8..:  payload
#
# Leaf payload:
#   [keys: count * 8 bytes (uint64 LE)]
#   [descriptors: count * descriptorSize bytes]
#
# Internal payload:
#   [keys: count * 8 bytes (uint64 LE)]
#   [childIndices: (count + 1) * 4 bytes (uint32 LE)]
#     — each child index is the BFS node index within the serialized block
#       sequence.
#
# The serialized output also has a 16-byte header prepended before the first
# node block:
#   Bytes 0-3:   magic "BTR\0"
#   Bytes 4-5:   descriptorSize (uint16 LE)
#   Bytes 6-7:   order (uint16 LE)
#   Bytes 8-15:  count (uint64 LE) — total number of key-descriptor pairs

const
  SerHeaderSize = 16
  NodeHeaderBytes = 8  # isLeaf(1) + reserved(1) + count(2) + reserved(4)

proc writeU16LE(buf: var seq[byte], offset: int, val: uint16) =
  buf[offset] = byte(val and 0xFF)
  buf[offset + 1] = byte((val shr 8) and 0xFF)

proc readU16LE(buf: openArray[byte], offset: int): uint16 =
  uint16(buf[offset]) or (uint16(buf[offset + 1]) shl 8)

proc writeU32LE(buf: var seq[byte], offset: int, val: uint32) =
  buf[offset] = byte(val and 0xFF)
  buf[offset + 1] = byte((val shr 8) and 0xFF)
  buf[offset + 2] = byte((val shr 16) and 0xFF)
  buf[offset + 3] = byte((val shr 24) and 0xFF)

proc readU32LE(buf: openArray[byte], offset: int): uint32 =
  uint32(buf[offset]) or
    (uint32(buf[offset + 1]) shl 8) or
    (uint32(buf[offset + 2]) shl 16) or
    (uint32(buf[offset + 3]) shl 24)

proc writeU64LE(buf: var seq[byte], offset: int, val: uint64) =
  for i in 0 ..< 8:
    buf[offset + i] = byte((val shr (i * 8)) and 0xFF)

proc readU64LE(buf: openArray[byte], offset: int): uint64 =
  for i in 0 ..< 8:
    result = result or (uint64(buf[offset + i]) shl (i * 8))

proc serialize*(tree: BTree): seq[byte] =
  ## Serialize the B-tree to a byte sequence.
  ## Each node occupies a fixed BlockSize (4096-byte) region laid out in BFS
  ## order. A 16-byte header precedes the node blocks.
  ##
  ## Format per node: [isLeaf: u8] [reserved: u8] [count: u16] [reserved: 4B]
  ##   [keys...] [children_indices_or_descriptors...]

  # BFS to assign indices and collect nodes in order.
  var nodes: seq[BTreeNode]
  var queue = initDeque[BTreeNode]()
  queue.addLast(tree.root)
  while queue.len > 0:
    let node = queue.popFirst()
    nodes.add(node)
    if not node.isLeaf:
      for child in node.children:
        queue.addLast(child)

  let totalNodes = nodes.len

  # Build a mapping from BTreeNode identity to BFS index.
  # We need this so internal nodes can record child indices.
  # Use a second BFS pass to compute child start indices.
  # For node at BFS index i that is internal with k keys,
  # its children are the next k+1 nodes in BFS order that are
  # children of this node. We track this with a counter.
  #
  # Actually, since BFS order means children of node i appear
  # contiguously at some offset, we can compute the child index
  # on the fly. We use a pointer-to-index table.
  type NodeIndexPair = tuple[node: BTreeNode, idx: int]
  # Build identity map using a seq and linear scan (BTreeNode is ref so
  # we can compare by identity).
  var nodeIndex: seq[NodeIndexPair]
  for i, n in nodes:
    nodeIndex.add((n, i))

  proc findIndex(ni: seq[NodeIndexPair], node: BTreeNode): int =
    for pair in ni:
      if pair.node == node:
        return pair.idx
    return -1  # should never happen

  # Allocate output: header + totalNodes * BlockSize
  let totalSize = SerHeaderSize + totalNodes * int(BlockSize)
  result = newSeq[byte](totalSize)

  # Write header.
  result[0] = byte('B')
  result[1] = byte('T')
  result[2] = byte('R')
  result[3] = 0
  writeU16LE(result, 4, uint16(tree.descriptorSize))
  writeU16LE(result, 6, uint16(tree.order))
  writeU64LE(result, 8, tree.count)

  # Write each node.
  for i, node in nodes:
    let base = SerHeaderSize + i * int(BlockSize)
    result[base] = if node.isLeaf: 1'u8 else: 0'u8
    result[base + 1] = 0  # reserved
    writeU16LE(result, base + 2, uint16(node.keys.len))
    # bytes 4-7 reserved, already zero

    var offset = base + NodeHeaderBytes

    # Write keys.
    for k in node.keys:
      writeU64LE(result, offset, k)
      offset += 8

    if node.isLeaf:
      # Write descriptors.
      for desc in node.descriptors:
        for b in 0 ..< tree.descriptorSize:
          if b < desc.len:
            result[offset + b] = desc[b]
          # else: already zero
        offset += tree.descriptorSize
    else:
      # Write child indices as uint32 LE.
      for child in node.children:
        let childIdx = findIndex(nodeIndex, child)
        writeU32LE(result, offset, uint32(childIdx))
        offset += 4

proc deserialize*(data: openArray[byte],
                  descriptorSize: int): Result[BTree, string] =
  ## Deserialize a B-tree from bytes produced by serialize().
  if data.len < SerHeaderSize:
    return err("data too short for header")

  # Validate header magic.
  if data[0] != byte('B') or data[1] != byte('T') or
      data[2] != byte('R') or data[3] != 0:
    return err("invalid B-tree magic")

  let storedDescSize = int(readU16LE(data, 4))
  let storedOrder = int(readU16LE(data, 6))
  let storedCount = readU64LE(data, 8)

  if storedDescSize != descriptorSize:
    return err("descriptor size mismatch: stored=" & $storedDescSize &
                " expected=" & $descriptorSize)

  let payloadLen = data.len - SerHeaderSize
  if payloadLen <= 0 or payloadLen mod int(BlockSize) != 0:
    return err("payload not aligned to block size")

  let totalNodes = payloadLen div int(BlockSize)

  # First pass: create all nodes (without linking children).
  var nodes = newSeq[BTreeNode](totalNodes)
  for i in 0 ..< totalNodes:
    let base = SerHeaderSize + i * int(BlockSize)
    let isLeaf = data[base] == 1
    let count = int(readU16LE(data, base + 2))

    if isLeaf:
      let n = newLeaf()
      n.keys = newSeq[uint64](count)
      n.descriptors = newSeq[seq[byte]](count)
      var offset = base + NodeHeaderBytes
      for k in 0 ..< count:
        n.keys[k] = readU64LE(data, offset)
        offset += 8
      for k in 0 ..< count:
        n.descriptors[k] = newSeq[byte](descriptorSize)
        for b in 0 ..< descriptorSize:
          n.descriptors[k][b] = data[offset + b]
        offset += descriptorSize
      nodes[i] = n
    else:
      let n = newInternal()
      n.keys = newSeq[uint64](count)
      n.children = newSeq[BTreeNode](count + 1)
      var offset = base + NodeHeaderBytes
      for k in 0 ..< count:
        n.keys[k] = readU64LE(data, offset)
        offset += 8
      # Children will be linked in second pass; store indices temporarily.
      # We read child indices now and link after all nodes are created.
      nodes[i] = n
    # else already set above

  # Second pass: link children for internal nodes.
  for i in 0 ..< totalNodes:
    let base = SerHeaderSize + i * int(BlockSize)
    let isLeaf = data[base] == 1
    if not isLeaf:
      let count = int(readU16LE(data, base + 2))
      var offset = base + NodeHeaderBytes + count * 8  # skip keys
      for c in 0 .. count:
        let childIdx = int(readU32LE(data, offset))
        if childIdx < 0 or childIdx >= totalNodes:
          return err("child index out of range: " & $childIdx)
        nodes[i].children[c] = nodes[childIdx]
        offset += 4

  var tree = BTree(
    descriptorSize: descriptorSize,
    order: storedOrder,
    root: nodes[0],
    count: storedCount,
  )
  ok(tree)
