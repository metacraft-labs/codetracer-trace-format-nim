[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
# Resolve physical payload files, rather than publishing a mutable SDK profile alias.
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
public static class TraceZstdPhysicalPath {
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  private static extern uint GetFinalPathNameByHandle(SafeFileHandle h, StringBuilder b, uint n, uint flags);
  public static string Of(SafeFileHandle h) {
    var b = new StringBuilder(32768);
    uint n = GetFinalPathNameByHandle(h, b, (uint)b.Capacity, 0);
    if (n == 0 || n >= b.Capacity) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
    string p = b.ToString();
    if (p.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("remote Zstd payload refused");
    if (!p.StartsWith(@"\\?\", StringComparison.Ordinal)) throw new InvalidOperationException("unexpected physical Zstd path");
    return p.Substring(4);
  }
}
'@
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'windows-zstd-artifact-manifest.json') -Raw | ConvertFrom-Json
if ($manifest.sdkRevision -ne '064daf5b154ea6a0758ddb2dcbcbc07685216e44' -or
    $manifest.archiveSha256 -ne '7b4eff6719990e38aca93a4844c2e86a1935090625c4611f7e89675e999c56cc' -or
    $manifest.files.Count -ne 3) { throw 'unexpected declared Zstd manifest' }
# Exact authenticated member set: count plus membership alone permits duplicates.
$expectedMembers = [System.Collections.Generic.Dictionary[string,string]]::new([System.StringComparer]::Ordinal)
$expectedMembers.Add('zstd.exe', '6b5c50dde7062909b69b618fae228c72090596dc254efe498fb426f5f430a1f9')
$expectedMembers.Add('include/zstd.h', '4fab9cf39160cd15fbd9fbc2fd637ee300002fa70e307f85e8a7e45c1e057906')
$expectedMembers.Add('static/libzstd_static.lib', '9b5247fe7a6fe0177cb731ebe5f9d6dd90e5ec6a26c1c72f796471ef7feb4295')
$seenMembers = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($entry in $manifest.files) {
  if (-not $expectedMembers.ContainsKey([string]$entry.path) -or
      -not $seenMembers.Add([string]$entry.path) -or
      [string]$entry.sha256 -cne $expectedMembers[[string]$entry.path]) {
    throw 'Zstd manifest member set or authenticated hash mismatch'
  }
}
if ($seenMembers.Count -ne $expectedMembers.Count) { throw 'incomplete authenticated Zstd manifest' }
$selected = @(Get-Command zstd -CommandType Application -ErrorAction Stop)[0].Source
$aliasRoot = Split-Path -Parent $selected
$held = [System.Collections.Generic.List[System.IO.FileStream]]::new()
$physicalRoot = $null
try {
  foreach ($entry in $manifest.files) {
    if ($entry.path -notin @('zstd.exe', 'include/zstd.h', 'static/libzstd_static.lib')) { throw 'unexpected Zstd member' }
    $inputPath = Join-Path $aliasRoot $entry.path
    $stream = [System.IO.File]::Open($inputPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
    $held.Add($stream)
    $physical = [TraceZstdPhysicalPath]::Of($stream.SafeFileHandle)
    $suffix = $entry.path.Replace('/', [System.IO.Path]::DirectorySeparatorChar)
    if (-not $physical.EndsWith([System.IO.Path]::DirectorySeparatorChar + $suffix, [System.StringComparison]::OrdinalIgnoreCase)) { throw 'unexpected Zstd payload member layout' }
    $memberRoot = $physical.Substring(0, $physical.Length - $suffix.Length - 1)
    if ($null -eq $physicalRoot) { $physicalRoot = $memberRoot }
    if (-not [string]::Equals($physicalRoot, $memberRoot, [System.StringComparison]::OrdinalIgnoreCase)) { throw 'mixed physical Zstd package roots' }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { $actual = [Convert]::ToHexString($sha.ComputeHash($stream)).ToLowerInvariant() } finally { $sha.Dispose() }
    if ($actual -cne $entry.sha256) { throw "declared Zstd member identity mismatch: $($entry.path)" }
  }
  if ($physicalRoot -match "[\r\n]") { throw 'invalid Zstd environment path' }
  if (-not $env:GITHUB_ENV -or -not $env:GITHUB_PATH) { throw 'missing workflow environment destinations' }
  Add-Content -LiteralPath $env:GITHUB_ENV -Value "CT_ZSTD_INCLUDE_DIR=$physicalRoot\include"
  Add-Content -LiteralPath $env:GITHUB_ENV -Value "CT_ZSTD_LIBRARY_FILE=$physicalRoot\static\libzstd_static.lib"
  Add-Content -LiteralPath $env:GITHUB_PATH -Value $physicalRoot
} finally {
  foreach ($stream in $held) { $stream.Dispose() }
}
