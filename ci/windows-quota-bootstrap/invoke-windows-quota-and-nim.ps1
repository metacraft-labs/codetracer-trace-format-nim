param([Parameter(Mandatory)][string]$OutputRoot)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'quota-bootstrap/windows-only' }
if (-not [IO.Path]::IsPathFullyQualified($OutputRoot) -or (Test-Path -LiteralPath $OutputRoot)) { throw 'quota-bootstrap/exclusive-output' }
$parent = Split-Path -Parent $OutputRoot
if (-not [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($parent), [IO.Path]::GetFullPath($env:RUNNER_TEMP))) { throw 'quota-bootstrap/output-parent' }
$parentItem = Get-Item -LiteralPath $parent -Force
if ($parentItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'quota-bootstrap/output-parent-reparse' }
$null = New-Item -ItemType Directory -Path $OutputRoot
$releasedImage = (Get-Command repro -CommandType Application | Select-Object -First 1).Source
$bootstrap = $null
try {
    $inputs = & (Join-Path $PSScriptRoot 'acquire-quota-bootstrap-inputs.ps1') -OutputRoot (Join-Path $OutputRoot 'inputs')
    & (Join-Path $PSScriptRoot 'test-quota-bootstrap-inputs.ps1') -OutputRoot (Join-Path $OutputRoot 'native-hold-controls')
    $bootstrap = & (Join-Path $PSScriptRoot 'build-released-runquota.ps1') -ReleasedReproImage $releasedImage -BootstrapInputs $inputs -DeclaredGccImage $env:TRACE_NIM_GCC_IMAGE -DeclaredGccSHA256 $env:TRACE_NIM_GCC_SHA256 -EvidenceRoot (Join-Path $OutputRoot 'build')
    $daemonHash = (Get-FileHash -LiteralPath $bootstrap.daemon -Algorithm SHA256).Hash.ToLowerInvariant()
    $cliHash = (Get-FileHash -LiteralPath $bootstrap.cli -Algorithm SHA256).Hash.ToLowerInvariant()
    $bootstrap.inputHold.HoldFile((Split-Path -Parent $bootstrap.daemon), 'runquotad.exe', $daemonHash)
    $bootstrap.inputHold.HoldFile((Split-Path -Parent $bootstrap.cli), 'runquota.exe', $cliHash)
    $bootstrap.inputHold.Verify()
    # Normal SDK discovery/startup owns the daemon. Do not spawn an alternate
    # budget authority, alter services, set bypass flags, or kill an existing daemon.
    $env:RUNQUOTAD_BIN = $bootstrap.daemon
    Add-Content -LiteralPath $env:GITHUB_ENV -Value "RUNQUOTAD_BIN=$env:RUNQUOTAD_BIN"
    $powerShell = (Get-Command pwsh -CommandType Application | Select-Object -First 1).Source
    & $releasedImage develop ./ci/windows-nim-prerequisite --tool-provisioning=tarball -- $powerShell -NoProfile -File "$env:GITHUB_WORKSPACE/ci/bind-declared-windows-nim.ps1" -AuthenticatedArchive (Join-Path $OutputRoot 'inputs/nim.zip')
    if ($LASTEXITCODE -ne 0) { throw "declared Windows Nim prerequisite failed: $LASTEXITCODE" }
    $bootstrap.inputHold.Verify()

    # Additive protocol control uses only genuine staged source, independently
    # of project tests; original payload32 and all platform jobs remain unchanged.
    $controlSource = Join-Path $OutputRoot 'released_quota_native_control.nim'
    [IO.File]::Copy((Join-Path $PSScriptRoot 'released_quota_native_control.nim'), $controlSource, $false)
    $controlImage = Join-Path $OutputRoot 'released_quota_native_control.exe'
    $arguments = @('c', '--threads:on', '-d:release', '--skipParentCfg', "--lib:$($bootstrap.bootstrapLibrary)", "--nimcache:$(Join-Path $OutputRoot 'control-cache')", "--out:$controlImage")
    foreach ($library in Get-ChildItem -LiteralPath (Join-Path $bootstrap.source 'libs') -Directory) {
        $source = Join-Path $library.FullName 'src'
        if (Test-Path -LiteralPath $source -PathType Container) { $arguments += "--path:$source" }
    }
    $arguments += "--path:$($bootstrap.leaseSource)"
    $arguments += $controlSource
    $savedPath = $env:PATH
    try {
        $env:PATH = (Split-Path -Parent $env:TRACE_NIM_GCC_IMAGE) + [IO.Path]::PathSeparator + $savedPath
        & $bootstrap.bootstrapNim @arguments 1> (Join-Path $OutputRoot 'native-control.compile.stdout') 2> (Join-Path $OutputRoot 'native-control.compile.stderr')
        if ($LASTEXITCODE -ne 0) { throw 'quota-bootstrap/native-control-compile' }
    } finally { $env:PATH = $savedPath }
    $missingEndpoint = '\\.\pipe\trace-quota-missing-' + [Guid]::NewGuid().ToString('N')
    & $controlImage default $missingEndpoint $bootstrap.daemon 1> (Join-Path $OutputRoot 'native-control.stdout') 2> (Join-Path $OutputRoot 'native-control.stderr')
    if ($LASTEXITCODE -ne 0) { throw 'quota-bootstrap/native-control-runtime' }
    $bootstrap.inputHold.Verify()
    & $bootstrap.cli status --json 1> (Join-Path $OutputRoot 'actual-daemon-status.json')
    if ($LASTEXITCODE -ne 0) { throw 'quota-bootstrap/daemon-status' }
    & $bootstrap.cli leases --json 1> (Join-Path $OutputRoot 'actual-daemon-leases.json')
    if ($LASTEXITCODE -ne 0) { throw 'quota-bootstrap/daemon-leases' }
    [pscustomobject]@{daemon=$bootstrap.daemon;daemonSHA256=$daemonHash;cli=$bootstrap.cli;cliSHA256=$cliHash;releaseImage=$releasedImage;protocolControl='actual v2 positive/release plus precise invalid/incompatible/missing/over-budget refusals';nativeCredit='requires this real Windows execution'} |
        ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputRoot 'actual-quota-bootstrap.json')
} finally {
    if ($null -ne $bootstrap) { $bootstrap.inputHold.Dispose() }
}
