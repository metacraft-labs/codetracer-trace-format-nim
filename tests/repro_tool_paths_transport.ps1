# No mocks: exercise the shared prerequisite binder against real JSON files.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot '../ci/repro-tool-paths.ps1')
$keys = @('RUNNER_TEMP', 'GITHUB_ENV', 'REPRO_VARIANTS')
$saved = @{}
foreach ($key in $keys) { $saved[$key] = [Environment]::GetEnvironmentVariable($key) }
$root = Join-Path ([IO.Path]::GetTempPath()) ('ct-tool-path-transport-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory $root | Out-Null
try {
  $env:RUNNER_TEMP = $root
  $env:GITHUB_ENV = Join-Path $root 'exports'
  $env:REPRO_VARIANTS = 'other=keep,toolPathsFile=/stale,profile=debug'
  $source = Join-Path $root 'source with spaces,commas-café'
  New-Item -ItemType Directory $source | Out-Null
  Set-TraceReproToolPaths -Bindings @{
    CT_NIM_STEW_SRC=$source; CT_NIM_RESULTS_SRC=$source; CT_NIM_UNITTEST2_SRC=$source
  }
  # The next prerequisite step must retain source bindings and refresh selection.
  $env:REPRO_VARIANTS = 'other=keep,toolPathsFile=/other-stale,profile=debug'
  Set-TraceReproToolPaths -Bindings @{
    CT_ZSTD_INCLUDE_DIR=(Join-Path $root 'include'); CT_ZSTD_LIBRARY_FILE=(Join-Path $root 'lib zstd,static.lib')
  }
  $manifest = Join-Path $root 'trace-repro-tool-paths.json'
  $document = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json -AsHashtable
  $bytes = [IO.File]::ReadAllBytes($manifest)
  if ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
    throw 'manifest unexpectedly has a UTF8 BOM'
  }
  $null = [Text.UTF8Encoding]::new($false, $true).GetString($bytes)
  if ($document.schema -ne 1 -or $document.paths.Count -ne 5 -or
      $document.paths.CT_NIM_STEW_SRC -cne $source -or
      $document.paths.CT_NIM_RESULTS_SRC -cne $source -or
      $document.paths.CT_NIM_UNITTEST2_SRC -cne $source -or
      $document.paths.CT_ZSTD_LIBRARY_FILE -cne (Join-Path $root 'lib zstd,static.lib')) {
    throw 'source/Zstd accumulation or JSON path round trip failed'
  }
  $expected = 'other=keep,profile=debug,toolPathsFile=' + $manifest
  if ($env:REPRO_VARIANTS -cne $expected) { throw 'owning selection refresh discarded unrelated variants' }
  if (@(Get-Content -LiteralPath $env:GITHUB_ENV)[-1] -cne ('REPRO_VARIANTS=' + $expected)) {
    throw 'workflow environment handoff differs from selected variants'
  }
  $optionalPcre = Join-Path $root 'optional pcre,library'
  Set-TraceReproToolPaths -Bindings @{ CT_PCRE_LIB_DIR=$optionalPcre }
  $withPcre = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json -AsHashtable
  if ($withPcre.paths.Count -ne 6 -or $withPcre.paths.CT_PCRE_LIB_DIR -cne $optionalPcre -or
      $withPcre.paths.CT_NIM_STEW_SRC -cne $source -or $env:REPRO_VARIANTS -cne $expected) {
    throw 'optional Windows PCRE binding discarded earlier inputs or variants'
  }
  $env:RUNNER_TEMP = Join-Path $root 'comma,path'
  $refused = $false
  try { Set-TraceReproToolPaths -Bindings @{ CT_NIM_STEW_SRC=$source } }
  catch { if ($_ -notmatch 'without commas or newlines') { throw }; $refused=$true }
  if (-not $refused) { throw 'comma-delimited manifest selection was silently truncated' }
  $env:RUNNER_TEMP = $root
  foreach ($malformed in @('[]', '{}', '{"schema":"1","paths":{}}', '{"schema":1,"paths":[]}')) {
    $malformed | Set-Content -LiteralPath $manifest
    $refused = $false
    try { Set-TraceReproToolPaths -Bindings @{ CT_NIM_STEW_SRC=$source } }
    catch { if ($_ -notmatch 'invalid owning tool-path manifest schema') { throw }; $refused=$true }
    if (-not $refused) { throw 'malformed manifest accepted' }
  }
  Write-Output 'PASS: prerequisite accumulation, JSON paths, selection freshness and CI handoff'
} finally {
  foreach ($key in $keys) { [Environment]::SetEnvironmentVariable($key, $saved[$key]) }
  Remove-Item -LiteralPath $root -Recurse -Force
}
