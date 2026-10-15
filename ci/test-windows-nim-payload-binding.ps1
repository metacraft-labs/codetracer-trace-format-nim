# Native filesystem controls for the production binding, followed separately by
# the unchanged real Nim/GCC header probes in the original workflow.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (!$IsWindows -or ![IO.Path]::IsPathFullyQualified($env:RUNNER_TEMP)) {
  throw 'native Windows controls require the owning runtime root'
}
Add-Type -Path (Join-Path $PSScriptRoot 'NimPayloadBinding.cs')
$root = Join-Path $env:RUNNER_TEMP ('nim-payload-controls-' + [guid]::NewGuid().ToString('N'))
if (Test-Path -LiteralPath $root) { throw 'occupied native control directory' }
[void][IO.Directory]::CreateDirectory($root)
$junctions = [Collections.Generic.List[string]]::new()
$results = [Collections.Generic.List[object]]::new()
function New-ControlJunction([string]$Path, [string]$Target) {
  if (Test-Path -LiteralPath $Path) { throw 'occupied control junction' }
  New-Item -ItemType Junction -Path $Path -Target $Target -ErrorAction Stop | Out-Null
  $junctions.Add($Path)
}
function Assert-BindingRefusal([string]$Name, [scriptblock]$Body, [string]$Reason) {
  $refused = $false
  try { & $Body } catch {
    $errorObject = $_.Exception
    while ($null -ne $errorObject.InnerException) { $errorObject = $errorObject.InnerException }
    if ($errorObject -isnot [IO.InvalidDataException] -or $errorObject.Message -ne $Reason) { throw }
    $refused = $true
  }
  if (!$refused) { throw 'native binding negative unexpectedly accepted' }
  $results.Add([ordered]@{control=$Name;passed=$true})
}
function Assert-SharingRefusal([string]$Name, [scriptblock]$Body) {
  $refused = $false
  try { & $Body } catch {
    $errorObject = $_.Exception
    while ($null -ne $errorObject.InnerException) { $errorObject = $errorObject.InnerException }
    # ERROR_SHARING_VIOLATION, not an unrelated permission or fixture failure.
    if ($errorObject -isnot [IO.IOException] -or ($errorObject.HResult -band 0xffff) -ne 32) { throw }
    $refused = $true
  }
  if (!$refused) { throw 'held native mapping unexpectedly mutable' }
  $results.Add([ordered]@{control=$Name;passed=$true})
}
try {
  $nim = @(Get-Command nim -CommandType Application)[0].Source
  $probe = Join-Path $root 'payload_query.nim'
  [IO.File]::WriteAllText($probe, "discard`n", [Text.UTF8Encoding]::new($false))
  $dump = @(& $nim dump --dump.format:json $probe)
  if ($LASTEXITCODE -ne 0) { throw 'actual selected Nim control metadata query failed' }
  $metadata = ($dump -join "`n") | ConvertFrom-Json
  $actual = [NimPayloadBinding]::Bind($metadata.prefixdir,$env:REPRO_PREFIX_NIM,$nim,$metadata.libpath)
  try {
    $actual.Verify()
    $actualImage = $actual.ImagePath
    $actualHeader = Join-Path $actual.IncludePath 'nimbase.h'
    $imageHash = $actual.ImageSha256
    $headerHash = $actual.HeaderSha256
    $store = Join-Path $root 'store'
    $prefix = Join-Path $store 'prefixes/nim/real-payload'
    $foreign = Join-Path $store 'prefixes/nim/foreign-payload'
    foreach ($directory in @($prefix,$foreign)) {
      [void][IO.Directory]::CreateDirectory((Join-Path $directory 'bin'))
      [void][IO.Directory]::CreateDirectory((Join-Path $directory 'lib'))
      [IO.File]::Copy($actualImage,(Join-Path $directory 'bin/nim.exe'))
      [IO.File]::Copy($actualHeader,(Join-Path $directory 'lib/nimbase.h'))
    }
    if ((Get-FileHash (Join-Path $prefix 'bin/nim.exe')).Hash -ne $imageHash -or
        (Get-FileHash (Join-Path $prefix 'lib/nimbase.h')).Hash -ne $headerHash) {
      throw 'native fixture differs from the actual selected payload'
    }
    $results.Add([ordered]@{control='actual-selected-SDK-payload';passed=$true})
  } finally { $actual.Dispose() }
  $alias = Join-Path $store 'p/1234abcd'
  [void][IO.Directory]::CreateDirectory((Split-Path $alias))
  New-ControlJunction $alias $prefix
  $image = Join-Path $alias 'bin/nim.exe'
  $include = Join-Path $alias 'lib'
  $binding = [NimPayloadBinding]::Bind($alias,$prefix,$image,$include)
  try {
    $binding.Verify()
    $results.Add([ordered]@{control='genuine-root-junction';passed=$true})
    Assert-SharingRefusal 'held-root-junction-replacement' {
      [IO.Directory]::Move($alias,($alias + '-replaced'))
    }
    Assert-SharingRefusal 'held-short-farm-replacement' {
      [IO.Directory]::Move((Join-Path $store 'p'),(Join-Path $store 'p-replaced'))
    }
    Assert-SharingRefusal 'held-package-parent-replacement' {
      [IO.Directory]::Move((Join-Path $store 'prefixes/nim'),(Join-Path $store 'prefixes/nim-replaced'))
    }
    Assert-SharingRefusal 'held-physical-prefix-replacement' {
      [IO.Directory]::Move($prefix,($prefix + '-replaced'))
    }
    Assert-SharingRefusal 'held-image-write' {
      $write = [IO.File]::Open((Join-Path $prefix 'bin/nim.exe'),[IO.FileMode]::Open,[IO.FileAccess]::Write,[IO.FileShare]::ReadWrite)
      $write.Dispose()
    }
    Assert-SharingRefusal 'held-header-write' {
      $write = [IO.File]::Open((Join-Path $prefix 'lib/nimbase.h'),[IO.FileMode]::Open,[IO.FileAccess]::Write,[IO.FileShare]::ReadWrite)
      $write.Dispose()
    }
    $binding.Verify()
  } finally { $binding.Dispose() }
  $direct = [NimPayloadBinding]::Bind($prefix,$prefix,(Join-Path $prefix 'bin/nim.exe'),(Join-Path $prefix 'lib'))
  try { $direct.Verify(); $results.Add([ordered]@{control='normal-direct-payload';passed=$true}) }
  finally { $direct.Dispose() }
  Assert-BindingRefusal 'foreign-declared-prefix' {
    $unexpected = [NimPayloadBinding]::Bind($alias,$foreign,$image,$include); $unexpected.Dispose()
  } 'foreign Nim junction target'
  Assert-BindingRefusal 'missing-declared-prefix' {
    $unexpected = [NimPayloadBinding]::Bind($alias,'',$image,$include); $unexpected.Dispose()
  } 'missing absolute Nim payload role'
  [IO.Directory]::Delete($alias)
  New-ControlJunction $alias $foreign
  Assert-BindingRefusal 'replaced-root-mapping' {
    $unexpected = [NimPayloadBinding]::Bind($alias,$prefix,$image,$include); $unexpected.Dispose()
  } 'foreign Nim junction target'
  [IO.Directory]::Delete($alias)
  New-ControlJunction $alias $prefix
  $lib = Join-Path $prefix 'lib'
  $saved = Join-Path $prefix 'lib-owned'
  [IO.Directory]::Move($lib,$saved)
  New-ControlJunction $lib $saved
  Assert-BindingRefusal 'intermediate-library-junction' {
    $unexpected = [NimPayloadBinding]::Bind($alias,$prefix,$image,$include); $unexpected.Dispose()
  } 'Nim payload object kind differs'
  [IO.Directory]::Delete($lib)
  [void]$junctions.Remove($lib)
  [IO.Directory]::Move($saved,$lib)
  $results | ConvertTo-Json -Compress
} finally {
  # Delete only created junction objects before recursive cleanup of this own
  # exclusive fixture. Never recurse through a reparse point or foreign target.
  foreach ($junction in @($junctions | Select-Object -Unique)) {
    if (Test-Path -LiteralPath $junction) {
      $item = Get-Item -Force -LiteralPath $junction
      if (!($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
          !$item.ResolveLinkTarget($false).FullName.StartsWith($root + [IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)) {
        throw 'native fixture cleanup identity differs'
      }
      [IO.Directory]::Delete($junction)
    }
  }
  [IO.Directory]::Delete($root,$true)
}
