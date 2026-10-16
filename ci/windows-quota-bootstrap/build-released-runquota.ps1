# Proposal: compile the actual released-client supplier, without modifying the SDK.
param(
    [Parameter(Mandatory)][string]$ReleasedReproImage,
    [Parameter(Mandatory)][object]$BootstrapInputs,
    [Parameter(Mandatory)][string]$DeclaredGccImage,
    [Parameter(Mandatory)][string]$DeclaredGccSHA256,
    [Parameter(Mandatory)][string]$EvidenceRoot
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'validate-source-tree.ps1')
if (-not ('QuotaBootstrapInputs' -as [type])) { Add-Type -Path (Join-Path $PSScriptRoot 'QuotaBootstrapInputs.cs') }
$BootstrapNimImage = Join-Path $BootstrapInputs.nimRoot 'bin/nim.exe'
$BootstrapNimLibrary = Join-Path $BootstrapInputs.nimRoot 'lib'
$LeaseSourceRoot = $BootstrapInputs.leaseRoot
Assert-DeclaredSourceTree $BootstrapInputs.nimRoot $BootstrapInputs.nimMembers 5249
function Assert-Image([string]$Path, [string]$SHA256) {
    if ($SHA256 -cnotmatch '^[0-9a-fA-F]{64}$') { throw 'quota-tool/hash-format' }
    $SHA256 = $SHA256.ToLowerInvariant()
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw 'quota-tool/absolute-image' }
    $item = Get-Item -LiteralPath $Path -Force
    if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
        (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $SHA256) {
        throw 'quota-tool/image-bytes'
    }
}
Assert-Image $ReleasedReproImage '9c92b68adcbdc87caf3b127ea6d2cdbaaec670e1c91c31f8e95e1c174c53c9e1'
Assert-Image $BootstrapNimImage 'ab1bcd5a479e81d2f3c58839b0654a97599aa8c2b015f6b021d6943d5f5b55a2'
Assert-Image (Join-Path $BootstrapNimLibrary 'nimbase.h') '28491d05916eab446de054370808030b33b63fd5623dcd454212adec27ee934d'
Assert-Image $DeclaredGccImage $DeclaredGccSHA256
$releaseRoot = Split-Path -Parent (Split-Path -Parent $ReleasedReproImage)
Assert-Image (Join-Path $releaseRoot 'bin/reprobuild.exe') 'e2f1eeb59e97220f4e7dbd15811c51bcff1e223dcb6dafdba6443b9b1b00ad6a'
$releasedSource = Join-Path $releaseRoot 'share/repro/src/runquota'
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'declared-released-runquota-source-manifest.json') -Raw | ConvertFrom-Json
Assert-DeclaredSourceTree $releasedSource $manifest.members 127
$plan = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'staged-major2-bootstrap-source-plan.json') -Raw | ConvertFrom-Json
Assert-DeclaredSourceTree $LeaseSourceRoot $plan.leaseCandidate.fullTrackedManifest 136
if (Test-Path -LiteralPath $EvidenceRoot) { throw 'quota-output/occupied' }
$null = New-Item -ItemType Directory -Path $EvidenceRoot
$source = Join-Path $EvidenceRoot 'source'
$null = New-Item -ItemType Directory -Path $source
foreach ($member in $manifest.members) {
    $destination = Join-Path $source $member.path
    $null = New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination)
    [IO.File]::Copy((Join-Path $releasedSource $member.path), $destination, $false)
}
Assert-DeclaredSourceTree $source $manifest.members 127
$hold = [QuotaBootstrapInputs]::new()
$beforePath = $env:PATH
$beforeLease = $env:SHM_LEASE_SRC
$completed = $false
try {
    $hold.HoldFile($releaseRoot, 'bin/repro.exe', '9c92b68adcbdc87caf3b127ea6d2cdbaaec670e1c91c31f8e95e1c174c53c9e1')
    $hold.HoldFile($releaseRoot, 'bin/reprobuild.exe', 'e2f1eeb59e97220f4e7dbd15811c51bcff1e223dcb6dafdba6443b9b1b00ad6a')
    foreach ($member in $BootstrapInputs.nimMembers) { $hold.HoldFile($BootstrapInputs.nimRoot, $member.path, $member.sha256) }
    foreach ($member in $manifest.members) {
        $hold.HoldFile($releasedSource, $member.path, $member.sha256)
        $hold.HoldFile($source, $member.path, $member.sha256)
    }
    foreach ($member in $plan.leaseCandidate.fullTrackedManifest) { $hold.HoldFile($LeaseSourceRoot, $member.path, $member.sha256) }
    # Authenticate the existing declared WinLibs archive, then hold every actual
    # package file, including compiler executables, CRT libraries and headers.
    $gccPackage = Split-Path -Parent (Split-Path -Parent $DeclaredGccImage)
    $gccAcquisition = Split-Path -Parent $gccPackage
    $gccArchive = Join-Path $gccAcquisition 'source.zip'
    $hold.HoldFile($gccAcquisition, 'source.zip', '78eff1e2e804b6a6320c713f084b8f820c662104a24cea6a3bfcab82032bdd60')
    $gccZip = [IO.Compression.ZipFile]::OpenRead($gccArchive)
    $gccFiles = 0
    $gccSeen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    try {
        foreach ($entry in $gccZip.Entries) {
            if ($entry.FullName.EndsWith('/')) { continue }
            if (-not $entry.FullName.StartsWith('mingw64/', [StringComparison]::Ordinal)) { throw 'quota-gcc/archive-prefix' }
            $relative = $entry.FullName.Substring(8)
            if (-not $gccSeen.Add($relative)) { throw 'quota-gcc/duplicate' }
            $stream = $entry.Open(); $hash = [Security.Cryptography.SHA256]::Create()
            try { $expected = [BitConverter]::ToString($hash.ComputeHash($stream)).Replace('-', '').ToLowerInvariant() }
            finally { $hash.Dispose(); $stream.Dispose() }
            $hold.HoldFile($gccPackage, $relative, $expected)
            $gccFiles++
        }
        if ($gccFiles -eq 0 -or -not $gccSeen.Contains('bin/gcc.exe') -or @($gccSeen | Where-Object { $_ -cmatch '(^|/)include/windows[.]h$' }).Count -eq 0) { throw 'quota-gcc/incomplete-package' }
        foreach ($file in Get-ChildItem -LiteralPath $gccPackage -Recurse -Force) {
            if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'quota-gcc/reparse' }
            if (-not $file.PSIsContainer -and -not $gccSeen.Contains([IO.Path]::GetRelativePath($gccPackage, $file.FullName).Replace([char]92, [char]47))) { throw 'quota-gcc/foreign-file' }
        }
    } finally { $gccZip.Dispose() }
    $hold.Verify()
    # This PATH selects the declared C backend; a source-bound native observer
    # must additionally authenticate the compiler process/headers actually read.
    $env:PATH = (Split-Path -Parent $DeclaredGccImage) + [IO.Path]::PathSeparator + $beforePath
    $actualGcc = (Get-Command gcc -CommandType Application).Source
    if (-not [StringComparer]::OrdinalIgnoreCase.Equals($actualGcc, $DeclaredGccImage)) { throw 'quota-tool/first-gcc' }
    $env:SHM_LEASE_SRC = Join-Path $LeaseSourceRoot 'src'
    Push-Location $source
    try {
        foreach ($app in @('runquotad', 'runquota')) {
            $output = Join-Path $EvidenceRoot ($app + '.exe')
            $cache = Join-Path $EvidenceRoot ('nimcache-' + $app)
            # Published cbf bootstrap recipe, same options, real shipping entrypoints.
            & $BootstrapNimImage c --threads:on -d:release "--lib:$BootstrapNimLibrary" "--nimcache:$cache" "--out:$output" "apps/$app/$app.nim" 1> (Join-Path $EvidenceRoot "$app.compile.stdout") 2> (Join-Path $EvidenceRoot "$app.compile.stderr")
            if ($LASTEXITCODE -ne 0) { throw "quota-build/$app" }
            & $output --version 1> (Join-Path $EvidenceRoot "$app.version.stdout") 2> (Join-Path $EvidenceRoot "$app.version.stderr")
            if ($LASTEXITCODE -ne 0) { throw "quota-version/$app" }
        }
    } finally { Pop-Location }
    $hold.Verify()
    Assert-DeclaredSourceTree $source $manifest.members 127
    Assert-DeclaredSourceTree $releasedSource $manifest.members 127
    Assert-DeclaredSourceTree $LeaseSourceRoot $plan.leaseCandidate.fullTrackedManifest 136
    $hold.Verify()
    $completed = $true
} finally {
    if (-not $completed) { $hold.Dispose() }
    $env:PATH = $beforePath
    $env:SHM_LEASE_SRC = $beforeLease
}
# The caller owns successful holds through the final actual SDK handshake.
# On every constructor failure the finally block disposes partial holds.

Write-Output -NoEnumerate ([pscustomobject]@{inputHold=$hold;daemon=(Join-Path $EvidenceRoot 'runquotad.exe');cli=(Join-Path $EvidenceRoot 'runquota.exe');source=$source;bootstrapNim=$BootstrapNimImage;bootstrapLibrary=$BootstrapNimLibrary;leaseSource=(Join-Path $LeaseSourceRoot 'src')})
