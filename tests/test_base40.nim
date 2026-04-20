{.push raises: [].}

## Base40 encoding/decoding roundtrip tests.

import codetracer_ctfs/base40

proc test_roundtrip_basic() {.raises: [].} =
  ## Test roundtrip for common filenames.
  let names = ["events.log", "meta.json", "a", "big.dat", "stream.dat", "test.dat"]
  for name in names:
    let encoded = base40Encode(name)
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "roundtrip failed for '" & name & "': got '" & decoded & "'"
  echo "PASS: test_roundtrip_basic"

proc test_roundtrip_special_chars() {.raises: [].} =
  ## Test filenames with special characters: . / -
  let names = ["a.b", "a/b", "a-b", "foo.bar", "x/y/z", "a-b-c"]
  for name in names:
    let encoded = base40Encode(name)
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "roundtrip failed for '" & name & "': got '" & decoded & "'"
  echo "PASS: test_roundtrip_special_chars"

proc test_roundtrip_max_length() {.raises: [].} =
  ## Test 12-character name (maximum for base40 in uint64).
  let name = "abcdefghijkl"
  let encoded = base40Encode(name)
  let decoded = base40Decode(encoded)
  doAssert decoded == name,
    "roundtrip failed for max-length name: got '" & decoded & "'"
  echo "PASS: test_roundtrip_max_length"

proc test_roundtrip_digits() {.raises: [].} =
  ## Test filenames with digits.
  let names = ["file01", "123", "0", "9a8b7c"]
  for name in names:
    let encoded = base40Encode(name)
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "roundtrip failed for '" & name & "': got '" & decoded & "'"
  echo "PASS: test_roundtrip_digits"

proc test_empty_name() {.raises: [].} =
  ## Test that encoding an empty string produces 0 and decoding 0 produces "".
  let encoded = base40Encode("")
  doAssert encoded == 0, "empty name should encode to 0"
  let decoded = base40Decode(0)
  doAssert decoded == "", "decoding 0 should produce empty string, got '" & decoded & "'"
  echo "PASS: test_empty_name"

proc test_single_chars() {.raises: [].} =
  ## Test each character in the base40 alphabet individually.
  let chars = "0123456789abcdefghijklmnopqrstuvwxyz./-"
  for c in chars:
    let name = $c
    let encoded = base40Encode(name)
    let decoded = base40Decode(encoded)
    doAssert decoded == name,
      "roundtrip failed for single char '" & name & "': got '" & decoded & "'"
  echo "PASS: test_single_chars"

# Run all tests
test_roundtrip_basic()
test_roundtrip_special_chars()
test_roundtrip_max_length()
test_roundtrip_digits()
test_empty_name()
test_single_chars()
