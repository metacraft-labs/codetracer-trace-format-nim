# Read-only native Windows SDK/compiler boundary diagnostic; no original test acceptance.
# No mocks. Real files/compiler, exclusively owned RUNNER_TEMP output only.
param([Parameter(Mandatory=$true)][string]$ProjectInclude,
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
foreach ($tool in @($gcc, $nim)) {
  if (![IO.Path]::IsPathFullyQualified($tool)) { throw 'relative selected compiler image' }
  $toolFile = Get-Item -Force -LiteralPath $tool
  if ($toolFile -isnot [IO.FileInfo] -or ($toolFile.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
    throw 'nonregular selected compiler image'
  }
}
$nimImageBefore = (Get-FileHash -Algorithm SHA256 -LiteralPath $nim).Hash
$nimInput = Join-Path $work 'nimbase_probe.nim'
[IO.File]::WriteAllText($nimInput, "discard`n", [Text.UTF8Encoding]::new($false))
$dumpText = @(& $nim dump --dump.format:json $nimInput)
if ($LASTEXITCODE -ne 0) { throw 'selected Nim library metadata query failed' }
$metadata = ($dumpText -join "`n") | ConvertFrom-Json
if ($metadata -isnot [System.Management.Automation.PSCustomObject] -or
    $metadata.nimExe -isnot [string] -or $metadata.prefixdir -isnot [string] -or
    $metadata.libpath -isnot [string] -or
    ![IO.Path]::IsPathFullyQualified($metadata.nimExe) -or
    ![IO.Path]::IsPathFullyQualified($metadata.prefixdir) -or
    ![IO.Path]::IsPathFullyQualified($metadata.libpath)) {
  throw 'selected Nim library metadata schema differs'
}
$nimPrefix = [IO.Path]::GetDirectoryName([IO.Path]::GetDirectoryName($nim))
if ([IO.Path]::GetFullPath($metadata.nimExe) -ne [IO.Path]::GetFullPath($nim) -or
    [IO.Path]::GetFullPath($metadata.prefixdir) -ne [IO.Path]::GetFullPath($nimPrefix)) {
  throw 'selected Nim library metadata image differs'
}
$NimInclude = [IO.Path]::GetFullPath($metadata.libpath)
$prefix = [IO.Path]::GetFullPath($nimPrefix) + [IO.Path]::DirectorySeparatorChar
if (!$NimInclude.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase) -or
    (Get-FileHash -Algorithm SHA256 -LiteralPath $nim).Hash -ne $nimImageBefore) {
  throw 'selected Nim library payload differs'
}
$directory = $NimInclude
while ($true) {
  $directoryItem = Get-Item -Force -LiteralPath $directory
  if ($directoryItem -isnot [IO.DirectoryInfo] -or ($directoryItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
    throw 'reparse selected Nim library payload'
  }
  if ($directory -eq [IO.Path]::GetFullPath($nimPrefix)) { break }
  $directory = [IO.Path]::GetDirectoryName($directory)
}
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
  if ($probe.name -eq 'nimbase') {
    # Nim supplies NIM_INTBITS and the other generated-C header contracts.
    # A standalone hand-written include is not a genuine Nim translation unit.
    $nimCache = Join-Path $work 'nimbase-cache'
    New-Item -ItemType Directory -Path $nimCache -ErrorAction Stop | Out-Null
    & $nim c --compileOnly ("--lib:" + $NimInclude) ("--nimcache:" + $nimCache) ("--out:" + (Join-Path $work 'nimbase_probe.exe')) $nimInput
    if ($LASTEXITCODE -ne 0) { throw 'selected Nim header-context generation failed' }
    $mainPath = Join-Path $nimCache '@mnimbase_probe.nim.c'
    $mainFiles = @(Get-ChildItem -LiteralPath $nimCache -Filter '@mnimbase_probe.nim.c' -File)
    $compileInventory = Get-Content -LiteralPath (Join-Path $nimCache 'nimbase_probe.json') -Raw | ConvertFrom-Json
    $mainRows = @($compileInventory.compile | Where-Object {
      $_.Count -eq 2 -and $_[0] -is [string] -and $_[0] -eq $mainPath
    })
    if ($mainFiles.Count -ne 1 -or $mainRows.Count -ne 1 -or ($mainFiles[0].Attributes -band [IO.FileAttributes]::ReparsePoint)) {
      throw 'selected Nim generated main translation unit differs'
    }
    $inputFile = $mainFiles[0].FullName
  } else {
    [IO.File]::WriteAllText($inputFile, '#include <string.h>' + "`n" + '#include "' + $probe.header + '"' + "`n" + 'int main(void) { return 0; }' + "`n", [Text.UTF8Encoding]::new($false))
  }
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
