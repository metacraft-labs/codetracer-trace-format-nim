# Read-only native Windows SDK/compiler boundary diagnostic; no original test acceptance.
# No mocks. Real files/compiler, exclusively owned RUNNER_TEMP output only.
param([Parameter(Mandatory=$true)][string]$NimInclude,
      [Parameter(Mandatory=$true)][string]$ProjectInclude,
      [Parameter(Mandatory=$true)][string]$ProjectHeader)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (![IO.Path]::IsPathFullyQualified($env:RUNNER_TEMP)) { throw 'missing absolute private runtime root' }
$work = Join-Path $env:RUNNER_TEMP ('gcc-header-boundary-' + [guid]::NewGuid().ToString('N'))
if (Test-Path -LiteralPath $work) { throw 'foreign diagnostic directory' }
New-Item -ItemType Directory -Path $work | Out-Null
$gcc = @(Get-Command gcc -CommandType Application)[0].Source
$nim = @(Get-Command nim -CommandType Application)[0].Source
$before = (Get-FileHash -Algorithm SHA256 -LiteralPath $gcc).Hash
$observedSources = @{}
if (![IO.Path]::IsPathFullyQualified($NimInclude)) { throw 'relative original Nim include prefix' }
$nimHeader = Join-Path $NimInclude 'nimbase.h'
foreach ($path in @($gcc,$nim,$nimHeader,(Join-Path $ProjectInclude $ProjectHeader))) {
  if (![IO.Path]::IsPathFullyQualified($path)) { throw 'relative diagnostic authority' }
  $file = Get-Item -Force -LiteralPath $path
  if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'reparse diagnostic authority' }
  $observedSources[$file.FullName] = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash
  [ordered]@{role='observed SDK file';path=$file.FullName;length=$file.Length;sha256=$observedSources[$file.FullName]} | ConvertTo-Json -Compress
}
& $gcc --version
if ($LASTEXITCODE -ne 0) { throw 'compiler version diagnostic failed' }
$results = @()
foreach ($probe in @(@{name='project-and-string';include=$ProjectInclude;header=$ProjectHeader},@{name='nimbase';include=$NimInclude;header='nimbase.h'})) {
  $inputFile = Join-Path $work ($probe.name + '.c')
  $outputFile = Join-Path $work ($probe.name + '.o')
  [IO.File]::WriteAllText($inputFile, '#include <string.h>' + "`n" + '#include "' + $probe.header + '"' + "`n" + 'int main(void) { return 0; }' + "`n", [Text.UTF8Encoding]::new($false))
  $arguments = @('-c',('-I'+$probe.include),'-o',$outputFile,$inputFile)
  [ordered]@{scope='actual declared SDK command selected inside original dev-exec wrapper; typed action profile identity remains separate';probe=$probe.name;compiler=$gcc;argv=$arguments} | ConvertTo-Json -Compress
  & $gcc @arguments
  $result = $LASTEXITCODE
  $results += [ordered]@{probe=$probe.name;exit=$result}
  if ($result -eq 0) { Get-FileHash -Algorithm SHA256 -LiteralPath $outputFile | Select-Object Path,Hash | ConvertTo-Json -Compress }
}
foreach ($path in $observedSources.Keys) {
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash -ne $observedSources[$path]) { throw 'observed compiler/header source changed' }
}
$results | ConvertTo-Json -Compress
if (@($results | Where-Object {$_.exit -ne 0}).Count) { exit 1 }
exit 0
