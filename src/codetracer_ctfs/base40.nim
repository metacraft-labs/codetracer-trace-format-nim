{.push raises: [].}

## Base40 filename encoding/decoding.
##
## Encodes filenames up to 12 characters into a single uint64 using a
## 40-character alphabet: \0, 0-9, a-z, ., /, -

# Base40 alphabet: \0, 0-9, a-z, ., /, -
const Base40Chars* = "\x000123456789abcdefghijklmnopqrstuvwxyz./-"

proc base40Encode*(name: string): uint64 =
  ## Encode a filename (up to 12 chars) into a single uint64 using base40.
  var val: uint64 = 0
  var multiplier: uint64 = 1
  for i in 0 ..< 12:
    var charIdx: uint64 = 0
    if i < name.len:
      let c = name[i]
      if c >= '0' and c <= '9':
        charIdx = uint64(ord(c) - ord('0') + 1)
      elif c >= 'a' and c <= 'z':
        charIdx = uint64(ord(c) - ord('a') + 11)
      elif c == '.':
        charIdx = 37
      elif c == '/':
        charIdx = 38
      elif c == '-':
        charIdx = 39
    val = val + charIdx * multiplier
    multiplier = multiplier * 40
  val

proc base40Decode*(val: uint64): string =
  ## Decode a base40-encoded uint64 back to a filename string.
  var remaining = val
  var chars: array[12, char]
  var lastNonZero = -1
  for i in 0 ..< 12:
    let idx = remaining mod 40
    remaining = remaining div 40
    if idx == 0:
      chars[i] = '\0'
    else:
      chars[i] = Base40Chars[idx]
      lastNonZero = i
  result = ""
  for i in 0 .. lastNonZero:
    result.add(chars[i])
