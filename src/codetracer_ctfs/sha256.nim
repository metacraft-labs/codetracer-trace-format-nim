when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## SHA-256 (FIPS 180-4), for the name of a container's live coordination page
## (`ctfs-container.md` §6, "The page: naming and discovery": the first 24
## hexadecimal digits of SHA-256 over the file's identity). Small inputs only;
## nothing here is performance-sensitive.

const K: array[64, uint32] = [
  0x428a2f98'u32, 0x71374491'u32, 0xb5c0fbcf'u32, 0xe9b5dba5'u32,
  0x3956c25b'u32, 0x59f111f1'u32, 0x923f82a4'u32, 0xab1c5ed5'u32,
  0xd807aa98'u32, 0x12835b01'u32, 0x243185be'u32, 0x550c7dc3'u32,
  0x72be5d74'u32, 0x80deb1fe'u32, 0x9bdc06a7'u32, 0xc19bf174'u32,
  0xe49b69c1'u32, 0xefbe4786'u32, 0x0fc19dc6'u32, 0x240ca1cc'u32,
  0x2de92c6f'u32, 0x4a7484aa'u32, 0x5cb0a9dc'u32, 0x76f988da'u32,
  0x983e5152'u32, 0xa831c66d'u32, 0xb00327c8'u32, 0xbf597fc7'u32,
  0xc6e00bf3'u32, 0xd5a79147'u32, 0x06ca6351'u32, 0x14292967'u32,
  0x27b70a85'u32, 0x2e1b2138'u32, 0x4d2c6dfc'u32, 0x53380d13'u32,
  0x650a7354'u32, 0x766a0abb'u32, 0x81c2c92e'u32, 0x92722c85'u32,
  0xa2bfe8a1'u32, 0xa81a664b'u32, 0xc24b8b70'u32, 0xc76c51a3'u32,
  0xd192e819'u32, 0xd6990624'u32, 0xf40e3585'u32, 0x106aa070'u32,
  0x19a4c116'u32, 0x1e376c08'u32, 0x2748774c'u32, 0x34b0bcb5'u32,
  0x391c0cb3'u32, 0x4ed8aa4a'u32, 0x5b9cca4f'u32, 0x682e6ff3'u32,
  0x748f82ee'u32, 0x78a5636f'u32, 0x84c87814'u32, 0x8cc70208'u32,
  0x90befffa'u32, 0xa4506ceb'u32, 0xbef9a3f7'u32, 0xc67178f2'u32]

proc rotr(x: uint32, n: int): uint32 {.inline.} = (x shr n) or (x shl (32 - n))

proc sha256*(data: openArray[byte]): array[32, byte] =
  var h: array[8, uint32] = [0x6a09e667'u32, 0xbb67ae85'u32, 0x3c6ef372'u32,
    0xa54ff53a'u32, 0x510e527f'u32, 0x9b05688c'u32, 0x1f83d9ab'u32,
    0x5be0cd19'u32]
  var msg = newSeq[byte](data.len)
  for i in 0 ..< data.len: msg[i] = data[i]
  let bitLen = uint64(data.len) * 8
  msg.add 0x80'u8
  while msg.len mod 64 != 56: msg.add 0'u8
  for i in countdown(7, 0): msg.add byte((bitLen shr (8 * i)) and 0xff)
  var w: array[64, uint32]
  var chunk = 0
  while chunk < msg.len:
    for i in 0 ..< 16:
      let o = chunk + 4 * i
      w[i] = (uint32(msg[o]) shl 24) or (uint32(msg[o + 1]) shl 16) or
        (uint32(msg[o + 2]) shl 8) or uint32(msg[o + 3])
    for i in 16 ..< 64:
      let s0 = rotr(w[i - 15], 7) xor rotr(w[i - 15], 18) xor (w[i - 15] shr 3)
      let s1 = rotr(w[i - 2], 17) xor rotr(w[i - 2], 19) xor (w[i - 2] shr 10)
      w[i] = w[i - 16] + s0 + w[i - 7] + s1
    var a = h[0]
    var b = h[1]
    var c = h[2]
    var d = h[3]
    var e = h[4]
    var f = h[5]
    var g = h[6]
    var hh = h[7]
    for i in 0 ..< 64:
      let s1 = rotr(e, 6) xor rotr(e, 11) xor rotr(e, 25)
      let ch = (e and f) xor ((not e) and g)
      let t1 = hh + s1 + ch + K[i] + w[i]
      let s0 = rotr(a, 2) xor rotr(a, 13) xor rotr(a, 22)
      let maj = (a and b) xor (a and c) xor (b and c)
      let t2 = s0 + maj
      hh = g
      g = f
      f = e
      e = d + t1
      d = c
      c = b
      b = a
      a = t1 + t2
    h[0] += a
    h[1] += b
    h[2] += c
    h[3] += d
    h[4] += e
    h[5] += f
    h[6] += g
    h[7] += hh
    chunk += 64
  for i in 0 ..< 8:
    result[4 * i] = byte(h[i] shr 24)
    result[4 * i + 1] = byte((h[i] shr 16) and 0xff)
    result[4 * i + 2] = byte((h[i] shr 8) and 0xff)
    result[4 * i + 3] = byte(h[i] and 0xff)

proc toHexLower*(b: openArray[byte]): string =
  const digits = "0123456789abcdef"
  result = newStringOfCap(b.len * 2)
  for x in b:
    result.add digits[int(x shr 4)]
    result.add digits[int(x and 0xf)]
