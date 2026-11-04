param(
  [Parameter(Mandatory = $true)]
  [string]$Destination
)

$ErrorActionPreference = "Stop"
# Immutable compiler pin inherited from official Repro source 064daf5b:
# windows/toolchain-versions.env and ensure-gcc.ps1 (ZIP, POSIX, UCRT).
$version = "16.1.0"
$release = "16.1.0posix-14.0.0-ucrt-r2"
$pin = @{
  asset = "winlibs-x86_64-posix-seh-gcc-16.1.0-mingw-w64ucrt-14.0.0-r2.zip"
  sha256 = "78eff1e2e804b6a6320c713f084b8f820c662104a24cea6a3bfcab82032bdd60"
  machine = 0x8664
}
# Both original Windows jobs target AMD64; the ARM job explicitly uses x64
# emulation. This is not a native ARM compiler/backend claim.
if (-not $IsWindows -or [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() -notin @("X64", "Arm64")) {
  throw "Declared AMD64 compiler requires a supported Windows host."
}
$parent = [System.IO.Path]::GetFullPath($env:RUNNER_TEMP)
$root = [System.IO.Path]::GetFullPath($Destination)
if ([System.IO.Path]::GetDirectoryName($root) -ne $parent) {
  throw "GCC extraction must be a direct child of the declared runner temp directory."
}
$parentItem = Get-Item -LiteralPath $parent -Force
if ($parentItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "GCC extraction parent is a reparse point."
}
if ($null -ne (Get-Item -LiteralPath $root -Force -ErrorAction SilentlyContinue)) {
  throw "Occupied GCC extraction destination."
}
New-Item -ItemType Directory -Path $root -ErrorAction Stop | Out-Null
$rootItem = Get-Item -LiteralPath $root -Force
$rootCreation = $rootItem.CreationTimeUtc
$archive = Join-Path $root "source.zip"
$url = "https://github.com/brechtsanders/winlibs_mingw/releases/download/$release/$($pin.asset)"
Invoke-WebRequest -Uri $url -OutFile $archive
if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $pin.sha256) {
  throw "Pinned GCC archive checksum differs."
}
if ((Get-Item -LiteralPath $archive).Length -gt 1GB) {
  throw "Pinned GCC archive exceeds its finite download allowance."
}
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead($archive)
$packageName = "mingw64"
$rootPrefix = $root + [System.IO.Path]::DirectorySeparatorChar
$seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$expandedBytes = 0L
try {
  foreach ($entry in $zip.Entries) {
    $name = $entry.FullName.Replace("\", "/")
    if (-not $name.StartsWith("$packageName/", [System.StringComparison]::Ordinal) -or $name.Split("/") -contains ".." -or $name -match "[:\x00]") {
      throw "Foreign GCC archive member path."
    }
    $target = [System.IO.Path]::GetFullPath((Join-Path $root $name))
    if (-not $target.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase) -or -not $seen.Add($target.TrimEnd([System.IO.Path]::DirectorySeparatorChar))) {
      throw "Aliased GCC archive member path."
    }
    $kind = ($entry.ExternalAttributes -shr 16) -band 0xf000
    if ($kind -notin @(0, 0x8000, 0x4000)) {
      throw "Nonregular GCC archive member."
    }
    $expandedBytes += $entry.Length
    if ($expandedBytes -gt 4GB) {
      throw "GCC archive exceeds its finite extraction allowance."
    }
  }
} finally {
  $zip.Dispose()
}
Expand-Archive -LiteralPath $archive -DestinationPath $root
$after = Get-Item -LiteralPath $root -Force
if ($after.CreationTimeUtc -ne $rootCreation -or $after.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "Owned GCC extraction directory changed."
}
$executable = Join-Path $root "$packageName\bin\gcc.exe"
$item = Get-Item -LiteralPath $executable -Force
if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
  throw "Pinned GCC executable is a reparse point."
}
$bytes = [System.IO.File]::ReadAllBytes($executable)
if ($bytes.Length -lt 64 -or $bytes[0] -ne 0x4d -or $bytes[1] -ne 0x5a) {
  throw "Pinned GCC executable has no DOS header."
}
$offset = [System.BitConverter]::ToUInt32($bytes, 0x3c)
if ($offset -gt $bytes.Length - 24 -or [System.BitConverter]::ToUInt32($bytes, $offset) -ne 0x00004550 -or [System.BitConverter]::ToUInt16($bytes, $offset + 4) -ne $pin.machine) {
  throw "Pinned GCC executable machine differs."
}
$output = @(& $executable -dumpfullversion)
if ($LASTEXITCODE -ne 0 -or $output.Count -ne 1 -or $output[0] -ne $version) {
  throw "Pinned GCC executable version differs."
}
$target = @(& $executable -dumpmachine)
if ($LASTEXITCODE -ne 0 -or $target.Count -ne 1 -or $target[0] -ne "x86_64-w64-mingw32") {
  throw "Pinned GCC target ABI differs."
}
$probe = Join-Path $root "compiler-probe.c"
$object = Join-Path $root "compiler-probe.o"
[System.IO.File]::WriteAllText($probe, "#include <string.h>`n#include <stdint.h>`nint main(void) { return 0; }`n", [System.Text.UTF8Encoding]::new($false))
& $executable -c -o $object $probe
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $object -PathType Leaf)) {
  throw "Pinned GCC cannot compile its standard headers."
}
$objectBytes = [System.IO.File]::ReadAllBytes($object)
if ($objectBytes.Length -lt 20 -or [System.BitConverter]::ToUInt16($objectBytes, 0) -ne 0x8664) {
  throw "Pinned GCC emitted a different COFF target."
}
$gccHash = (Get-FileHash -LiteralPath $executable -Algorithm SHA256).Hash
$bin = [System.IO.Path]::GetDirectoryName($executable)
Add-Content -LiteralPath $env:GITHUB_PATH -Value $bin
Add-Content -LiteralPath $env:GITHUB_ENV -Value "TRACE_NIM_GCC_IMAGE=$executable"
Add-Content -LiteralPath $env:GITHUB_ENV -Value "TRACE_NIM_GCC_SHA256=$gccHash"
Write-Host "Pinned AMD64 GCC $version selected; archive SHA256 $($pin.sha256)."
