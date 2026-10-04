## A CTFS member name is 1 to 12 characters from `0-9 a-z . / -`
## (`ctfs-container.md` §3). The writer refuses any other name when the member
## is created, naming it, instead of storing the base40 packing of a different
## name; a reader refuses to look one up instead of answering with whatever
## member its packing happens to match.
##
## `Refused` is the set of names the Rust writer refuses too
## (`codetracer-trace-format`'s `member_names_are_refused_alike.rs` drives
## both writers over it). No mocks: the real CTFS writer and readers.

import std/strutils
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/member_view

const
  Refused* = ["", "event_log.dat", "Meta.dat", "abcdefghijklm", "a b",
    "steps.dat\0", "naïve.dat", "a_b", "tab\tname"]
    ## Each breaks §3 one way: empty, an underscore and 13 characters,
    ## a capital, 13 characters, a space, a NUL, a non-ASCII byte, an
    ## underscore alone, a control character.
  Accepted* = ["steps.dat", "a", "abcdefghijkl", "a/b-c.12", "0", "step-map.ns"]

block the_writer_refuses_an_unencodable_name_by_name:
  for name in Refused:
    var c = createCtfs()
    let r = c.addFile(name)
    doAssert r.isErr, "addFile accepted " & escape(name)
    doAssert escape(name) in r.error or name in r.error,
      "the refusal does not name " & escape(name) & ": " & r.error
  for name in Accepted:
    var c = createCtfs()
    doAssert c.addFile(name).isOk, name
  echo "PASS the_writer_refuses_an_unencodable_name_by_name"

block a_reader_does_not_answer_for_a_mangled_name:
  # "abcdefghijklm" packs as "abcdefghijkl" — its first 12 characters.
  var c = createCtfs()
  var f = c.addFile("abcdefghijkl").get()
  doAssert c.writeToFile(f, [1'u8, 2, 3]).isOk
  let image = c.toBytes()
  doAssert readInternalFile(image, "abcdefghijkl").get() == @[1'u8, 2, 3]
  let r = readInternalFile(image, "abcdefghijklm")
  doAssert r.isErr and "abcdefghijklm" in r.error,
    "a lookup of a 13-character name answered: " & $r
  doAssert not hasInternalFile(image, "abcdefghijklm")
  doAssert viewMember(newContainerImage(image), "abcdefghijklm").isErr
  echo "PASS a_reader_does_not_answer_for_a_mangled_name"
