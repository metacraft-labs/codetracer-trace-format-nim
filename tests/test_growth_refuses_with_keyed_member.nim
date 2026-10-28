## Root growth refuses a container that holds a keyed member, naming it
## (`CTFS-Keyed-Families` CKF-4N, "The growth guard"). Growth relocates blocks
## and rewrites only the root's `MapBlock`s and the mapping blocks it knows, so
## a keyed member's stored block numbers would be left naming moved blocks.
##
## Null prediction: on the base, the 32nd `addFile` of a 31-entry container
## grows the root and succeeds; here it must be refused, naming `cppages.hdr`,
## and the container must be byte-identical to what it was before the attempt.
## A control without a keyed member still grows (growth is retired by CKF-8,
## not here), so the refusal is the guard's and not a broken growth.

import std/strutils
import results
import codetracer_ctfs/[types, container, page_store]

block withKeyed:
  var c = createCtfs(maxRootEntries = 31)
  discard c.createPageStore().get()   # five keyed members
  for i in 0 ..< 26:
    discard c.addFile("m" & $i & ".dat").get()
  let before = c.data
  let r = c.addFile("one.more")
  doAssert r.isErr, "a container with a keyed member grew its root"
  doAssert "cppages.hdr" in r.error, "the refusal does not name the keyed member: " & r.error
  doAssert c.data == before, "the refused growth changed the container"
  doAssert c.maxRootEntries == 31

block control:
  var c = createCtfs(maxRootEntries = 31)
  for i in 0 ..< 31:
    discard c.addFile("m" & $i & ".dat").get()
  let r = c.addFile("one.more")
  doAssert r.isOk, "the control (no keyed member) did not grow: " & r.error
  doAssert c.maxRootEntries > 31

echo "PASS test_growth_refuses_with_keyed_member"
