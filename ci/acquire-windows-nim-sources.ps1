# PRIVATE proposal: whole locked public source acquisition, never native runtime proof.
param([Parameter(Mandatory=$true)][string]$Destination,
      [Parameter(Mandatory=$true)][string]$Manifest,
      [Parameter(Mandatory=$true)][string]$Lock,
      [Parameter(Mandatory=$true)][string]$Receipt)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (!(Test-Path -LiteralPath $Manifest -PathType Leaf)) { throw 'missing complete source manifest' }
if (!( [IO.Path]::IsPathFullyQualified($Destination))) { throw 'source destination must be absolute' }
if ((Test-Path -LiteralPath $Destination) -or (Test-Path -LiteralPath $Receipt)) { throw 'foreign source destination/receipt refused' }
$parent = Get-Item -Force -LiteralPath (Split-Path -Parent $Destination)
while ($null -ne $parent) {
  if ($parent.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'reparse source ancestor refused' }
  $parent = $parent.Parent
}
foreach ($key in @('GIT_DIR','GIT_WORK_TREE','GIT_COMMON_DIR','GIT_TEMPLATE_DIR','GIT_CONFIG_COUNT','GIT_CONFIG_PARAMETERS','GIT_INDEX_FILE','GIT_OBJECT_DIRECTORY','GIT_ALTERNATE_OBJECT_DIRECTORIES')) {
  if ([Environment]::GetEnvironmentVariable($key)) { throw ('inherited source Git authority refused: '+$key) }
}
function Assert-PlainPath([string]$Path) {
  if (![IO.Path]::IsPathFullyQualified($Path)) { throw 'relative authority path refused' }
  $member = Get-Item -Force -LiteralPath $Path
  while ($null -ne $member) {
    if ($member.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'reparse authority path refused' }
    $member = if ($member -is [IO.DirectoryInfo]) { $member.Parent } else { $member.Directory }
  }
}
Assert-PlainPath $Manifest
Assert-PlainPath $Lock
if (!(Test-Path -LiteralPath $Lock -PathType Leaf)) { throw 'missing existing owning lock' }
Assert-PlainPath $env:RUNNER_TEMP
Assert-PlainPath $env:GITHUB_ENV
if (!(Test-Path -LiteralPath $env:GITHUB_ENV -PathType Leaf)) { throw 'missing ordinary CI export file' }
$runnerRoot = [IO.Path]::GetFullPath($env:RUNNER_TEMP).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
foreach ($path in @($Destination,$Receipt,$env:GITHUB_ENV)) {
  if (![IO.Path]::IsPathFullyQualified($path) -or ![IO.Path]::GetFullPath($path).StartsWith($runnerRoot,[StringComparison]::Ordinal)) { throw 'foreign runtime destination refused' }
}
if (@($Manifest,$Lock,$env:GITHUB_ENV) | Select-Object -Unique | Measure-Object | ForEach-Object Count | Where-Object { $_ -ne 3 }) { throw 'aliased input/export authority refused' }
$manifestBefore = (Get-FileHash -Algorithm SHA256 -LiteralPath $Manifest).Hash
$lockBefore = (Get-FileHash -Algorithm SHA256 -LiteralPath $Lock).Hash
$exportBefore = (Get-FileHash -Algorithm SHA256 -LiteralPath $env:GITHUB_ENV).Hash
$authority = Get-Content -Raw -LiteralPath $Manifest | ConvertFrom-Json
$locked = Get-Content -Raw -LiteralPath $Lock | ConvertFrom-Json
foreach ($name in @('nim-stew','nim-results','nim-unittest2')) {
  foreach ($field in @('owner','repo','rev','narHash','type')) {
    if ($authority.$name.locked.$field -ne $locked.nodes.$name.locked.$field) { throw 'source manifest differs from existing owning lock' }
  }
}
$gitImage = @(Get-Command git -CommandType Application)[0].Source
$gitHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $gitImage).Hash.ToLowerInvariant()
New-Item -ItemType Directory -Path $Destination | Out-Null
$template = Join-Path $Destination '.empty-template'
New-Item -ItemType Directory -Path $template | Out-Null
$global = Join-Path $Destination '.empty-gitconfig'
[IO.File]::WriteAllText($global, '')
$priorGlobal = $env:GIT_CONFIG_GLOBAL
$priorNoSystem = $env:GIT_CONFIG_NOSYSTEM
$env:GIT_CONFIG_GLOBAL = $global
$env:GIT_CONFIG_NOSYSTEM = '1'
function Run-Git([string[]]$Arguments) {
  $result = & $gitImage @Arguments
  if ($LASTEXITCODE -ne 0) { throw "source Git failed ($LASTEXITCODE)" }
  return $result
}
$proof = [ordered]@{ scope='Complete committed Git source/body/index-mode qualification only; no Windows NAR or compiler/runtime acceptance'; git_image=$gitImage; git_sha256=$gitHash; sources=@{} }
try {
  foreach ($name in @('nim-stew','nim-results','nim-unittest2')) {
    $node = $authority.$name
    if ($node.locked.rev -notmatch '^[0-9a-f]{40}$') { throw 'malformed locked revision' }
    $root = Join-Path $Destination $name
    $url = 'https://github.com/' + $node.locked.owner + '/' + $node.locked.repo + '.git'
    Run-Git @('clone','--no-checkout',('--template='+$template),$url,$root) | Out-Null
    Run-Git @('-C',$root,'-c','core.autocrlf=false','-c','core.symlinks=true','checkout','--detach',$node.locked.rev) | Out-Null
    if ((Run-Git @('-C',$root,'rev-parse','HEAD')) -ne $node.locked.rev) { throw 'wrong source revision' }
    $rows = @(Run-Git @('-C',$root,'ls-files','--stage'))
    if ($rows.Count -ne @($node.members.PSObject.Properties).Count) { throw 'incomplete/unexpected source inventory' }
    $observed = @{}
    foreach ($row in $rows) {
      if ($row -notmatch '^100644 [0-9a-f]{40} 0\t(.+)$') { throw 'unexpected source mode/kind/stage' }
      $relative = $Matches[1]
      if ($relative -match '[\r\n]' -or [IO.Path]::IsPathFullyQualified($relative) -or ($relative.Split('/') -contains '..')) { throw 'unsafe source member' }
      $expected = $node.members.PSObject.Properties[$relative]
      if ($null -eq $expected -or $expected.Value.kind -ne 'file') { throw 'unknown source member' }
      $file = Get-Item -Force -LiteralPath (Join-Path $root $relative)
      if ($file -is [IO.DirectoryInfo] -or ($file.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'wrong/reparse source member' }
      $body = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
      if ($body -ne $expected.Value.body) { throw ('source body mismatch: '+$relative) }
      $observed[$relative] = @{ sha256=$body; git_mode='100644' }
    }
    if (@(Run-Git @('-C',$root,'--no-optional-locks','status','--porcelain=v1')).Count -ne 0) { throw 'source tree not clean' }
    $proof.sources[$name] = @{ revision=$node.locked.rev; path=$root; members=$observed }
  }
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $gitImage).Hash.ToLowerInvariant() -ne $gitHash) { throw 'Git image changed during acquisition' }
  Assert-PlainPath $Manifest
  Assert-PlainPath $Lock
  Assert-PlainPath $env:GITHUB_ENV
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $Manifest).Hash -ne $manifestBefore -or
      (Get-FileHash -Algorithm SHA256 -LiteralPath $Lock).Hash -ne $lockBefore -or
      (Get-FileHash -Algorithm SHA256 -LiteralPath $env:GITHUB_ENV).Hash -ne $exportBefore -or
      (Test-Path -LiteralPath $Receipt)) { throw 'source/export authority changed during acquisition' }
  $proof | ConvertTo-Json -Depth 20 | Set-Content -Encoding utf8 -LiteralPath $Receipt
  # Export only after every complete tree qualifies; original Unix selection unchanged.
  if (!$env:GITHUB_ENV) { throw 'missing CI export destination' }
  foreach ($pair in @(@('CT_NIM_STEW_SRC','nim-stew'),@('CT_NIM_RESULTS_SRC','nim-results'),@('CT_NIM_UNITTEST2_SRC','nim-unittest2'))) {
    $value = Join-Path $Destination $pair[1]
    if ($value -match '[\r\n]') { throw 'invalid source export path' }
    ($pair[0]+'='+$value) | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
  }
} finally {
  $env:GIT_CONFIG_GLOBAL = $priorGlobal
  $env:GIT_CONFIG_NOSYSTEM = $priorNoSystem
}
