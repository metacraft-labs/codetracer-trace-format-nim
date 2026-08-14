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

proc base40Encodable*(name: string): bool =
  ## Report whether `base40Encode` is lossless for `name`.
  ##
  ## `base40Encode` maps anything outside the alphabet to the padding index 0,
  ## so `"snap!pages"` and `"snap"` encode to the *same* u64 — silently, and
  ## with the container then carrying a stream under a name nobody asked for.
  ## Any caller that takes a filename from outside the library (an FFI
  ## consumer naming a derived stream, say) must reject it here first rather
  ## than discover the collision later.
  if name.len == 0 or name.len > 12:
    return false
  for c in name:
    if not ((c >= '0' and c <= '9') or (c >= 'a' and c <= 'z') or
            c == '.' or c == '/' or c == '-'):
      return false
  true

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
