param([Parameter(Mandatory)][string]$OutputRoot)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not ('QuotaBootstrapInputs' -as [type])) { Add-Type -Path (Join-Path $PSScriptRoot 'QuotaBootstrapInputs.cs') }
if (Test-Path -LiteralPath $OutputRoot) { throw 'quota-control/occupied' }
$null = New-Item -ItemType Directory -Path $OutputRoot
$root = Join-Path $OutputRoot 'tree'
$nested = Join-Path $root 'nested'
$null = New-Item -ItemType Directory -Path $nested
$file = Join-Path $nested 'source.nim'
[IO.File]::WriteAllText($file, "discard`n")
$sha = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant()
$hold = [QuotaBootstrapInputs]::new()
try {
    $hold.HoldFile($root, 'nested/source.nim', $sha)
    $hold.Verify()
    foreach ($mutation in @('write-file', 'rename-file', 'rename-parent', 'rename-root')) {
        $refused = $false
        try {
            switch ($mutation) {
                'write-file' { [IO.File]::WriteAllText($file, 'foreign') }
                'rename-file' { [IO.File]::Move($file, (Join-Path $nested 'replaced.nim')) }
                'rename-parent' { [IO.Directory]::Move($nested, (Join-Path $root 'replaced')) }
                'rename-root' { [IO.Directory]::Move($root, (Join-Path $OutputRoot 'replaced')) }
            }
        } catch {
            $cause = $_.Exception.GetBaseException()
            if ($cause -isnot [IO.IOException]) { throw }
            # Sharing/lock violation is the tested refusal, not arbitrary failure.
            $code = $cause.HResult -band 0xffff
            if ($code -notin @(32, 33)) { throw }
            $refused = $true
        }
        if (-not $refused) { throw "quota-control/mutation-succeeded/$mutation" }
        $hold.Verify()
    }
} finally { $hold.Dispose() }
# Whole-tree source validation must additionally refuse an extra shadow module.
. (Join-Path $PSScriptRoot 'validate-source-tree.ps1')
$members = @([pscustomobject]@{path='nested/source.nim';size=(Get-Item -LiteralPath $file).Length;sha256=$sha})
Assert-DeclaredSourceTree $root $members 1
[IO.File]::WriteAllText((Join-Path $nested 'foreign_shadow.nim'), 'discard')
$refused = $false
try { Assert-DeclaredSourceTree $root $members 1 }
catch {
    if ($_.Exception.Message -cne 'quota-source/undeclared-member') { throw }
    $refused = $true
}
if (-not $refused) { throw 'quota-control/shadow-not-refused' }
# Preserve known fixture bytes for the evidence collector; no recursive cleanup
# of unexpected rename/reparse state can mask the primary control failure.
'actual native held-input file/parent/root mutation and shadow-source controls passed'
