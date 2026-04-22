{.push raises: [].}

## In-memory B-tree for namespace key lookup.
##
## Maps uint64 keys to fixed-size descriptor values (8 bytes for Type A,
## 16 bytes for Type B). Each node conceptually represents one 4096-byte
## CTFS block. Block-backed storage is deferred to M9.
##
## The tree supports sorted bulk-loading (common case) as well as
## arbitrary-order insertion.

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
