# Shared transport for the two prerequisite binders. JSON keeps arbitrary
# tool-path characters out of REPRO_VARIANTS' comma-delimited syntax.
function Set-TraceReproToolPaths {
  [CmdletBinding()]
  param([Parameter(Mandatory)][hashtable]$Bindings)
  $ErrorActionPreference = 'Stop'
  $manifest = Join-Path $env:RUNNER_TEMP 'trace-repro-tool-paths.json'
  if (-not [IO.Path]::IsPathFullyQualified($manifest) -or $manifest -match '[,\r\n]') {
    throw 'tool-path manifest requires an absolute path without commas or newlines'
  }
  if ($env:REPRO_VARIANTS -match '[\r\n]') { throw 'invalid multiline REPRO_VARIANTS' }
  $document = @{ schema = 1; paths = @{} }
  if (Test-Path -LiteralPath $manifest) {
    $document = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json -AsHashtable
    if ($document -isnot [System.Collections.IDictionary] -or
        -not $document.Contains('schema') -or -not $document.Contains('paths') -or
        ($document.schema -isnot [int] -and $document.schema -isnot [long]) -or
        $document.schema -ne 1 -or $document.paths -isnot [System.Collections.IDictionary]) {
      throw 'invalid owning tool-path manifest schema'
    }
  }
  foreach ($key in $Bindings.Keys) {
    if ($key -cnotin @('CT_NIM_STEW_SRC','CT_NIM_RESULTS_SRC','CT_NIM_UNITTEST2_SRC',
                       'CT_ZSTD_INCLUDE_DIR','CT_ZSTD_LIBRARY_FILE','CT_PCRE_LIB_DIR')) {
      throw "unknown owning tool-path field: $key"
    }
    $value = $Bindings[$key]
    if ($value -isnot [string] -or -not [IO.Path]::IsPathFullyQualified($value) -or $value -match '[\r\n]') {
      throw "invalid owning tool path: $key"
    }
    $document.paths[$key] = $value
  }
  $encoded = $document | ConvertTo-Json -Depth 4
  $temporary = $manifest + '.' + [Guid]::NewGuid().ToString('N') + '.tmp'
  try {
    [IO.File]::WriteAllText($temporary, $encoded, [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $manifest -Force
  } finally {
    if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary }
  }
  $unrelated = @($env:REPRO_VARIANTS -split ',' | Where-Object {
    $_ -and $_ -cnotmatch '^\s*toolPathsFile\s*='
  })
  $env:REPRO_VARIANTS = (@($unrelated) + @('toolPathsFile=' + $manifest)) -join ','
  if (-not $env:GITHUB_ENV) { throw 'missing workflow environment destination' }
  Add-Content -LiteralPath $env:GITHUB_ENV -Value ('REPRO_VARIANTS=' + $env:REPRO_VARIANTS)
}
