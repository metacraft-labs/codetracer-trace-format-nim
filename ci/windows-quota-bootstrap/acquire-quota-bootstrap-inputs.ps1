param([Parameter(Mandatory)][string]$OutputRoot)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputRoot) { throw 'quota-intake/occupied' }
$null = New-Item -ItemType Directory -Path $OutputRoot
function Get-Bounded([string]$Url, [string]$Destination, [long]$Limit) {
    $client = [Net.Http.HttpClient]::new()
    $response = $null; $inputStream = $null; $outputStream = $null
    try {
        $response = $client.GetAsync($Url, [Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
        $null = $response.EnsureSuccessStatusCode()
        if ($response.Content.Headers.ContentLength -gt $Limit) { throw 'quota-intake/download-bound' }
        $inputStream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
        $outputStream = [IO.File]::Open($Destination, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $buffer = [byte[]]::new(65536); [long]$total = 0
        while (($count = $inputStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            $total += $count
            if ($total -gt $Limit) { throw 'quota-intake/download-bound' }
            $outputStream.Write($buffer, 0, $count)
        }
    } finally {
        if ($outputStream) { $outputStream.Dispose() }
        if ($inputStream) { $inputStream.Dispose() }
        if ($response) { $response.Dispose() }
        $client.Dispose()
    }
}
function Expand-DirectZip([string]$Archive, [string]$Root, [long]$ExpandedLimit) {
    $null = New-Item -ItemType Directory -Path $Root
    $zip = [IO.Compression.ZipFile]::OpenRead($Archive)
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $manifest = [Collections.Generic.List[object]]::new()
    try {
        [long]$total = 0; $top = $null
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName
            if ([string]::IsNullOrEmpty($name) -or $name.Contains([char]92) -or $name.Contains(':') -or $name.StartsWith('/')) { throw 'quota-intake/zip-path' }
            $parts = $name.TrimEnd('/').Split('/')
            if ($parts -contains '' -or $parts -contains '.' -or $parts -contains '..') { throw 'quota-intake/zip-path' }
            if (-not $seen.Add($name)) { throw 'quota-intake/duplicate' }
            if ($null -eq $top) { $top = $parts[0] }
            if ($parts[0] -cne $top) { throw 'quota-intake/multiple-roots' }
            $kind = ($entry.ExternalAttributes -shr 16) -band 0xf000
            if ($kind -notin @(0, 0x8000, 0x4000)) { throw 'quota-intake/zip-kind' }
            if ($parts.Length -eq 1) {
                if (-not $name.EndsWith('/')) { throw 'quota-intake/root-kind' }
                continue
            }
            $relative = [string]::Join('/', $parts[1..($parts.Length - 1)])
            $destination = Join-Path $Root $relative
            if ($name.EndsWith('/')) { $null = New-Item -ItemType Directory -Force -Path $destination; continue }
            $total += $entry.Length
            if ($total -gt $ExpandedLimit) { throw 'quota-intake/decoded-bound' }
            $null = New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination)
            $source = $entry.Open(); $output = $null
            $digest = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
            try {
                $output = [IO.File]::Open($destination, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
                $buffer = [byte[]]::new(65536); [long]$actual = 0
                while (($count = $source.Read($buffer, 0, $buffer.Length)) -gt 0) {
                    $actual += $count
                    if ($actual -gt $entry.Length) { throw 'quota-intake/member-size' }
                    $output.Write($buffer, 0, $count)
                    $digest.AppendData($buffer, 0, $count)
                }
                if ($actual -ne $entry.Length) { throw 'quota-intake/member-size' }
                $expectedSHA = [BitConverter]::ToString($digest.GetHashAndReset()).Replace('-', '').ToLowerInvariant()
            } finally { if ($output) { $output.Dispose() }; $source.Dispose(); $digest.Dispose() }
            if ((Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant() -cne $expectedSHA) { throw 'quota-intake/copied-member-bytes' }
            $manifest.Add([pscustomobject]@{path=$relative;size=$entry.Length;sha256=$expectedSHA})
        }
        if ($manifest.Count -eq 0) { throw 'quota-intake/empty' }
        return $manifest.ToArray()
    } finally { $zip.Dispose() }
}
$nimArchive = Join-Path $OutputRoot 'nim.zip'
Get-Bounded 'https://nim-lang.org/download/nim-2.2.10_x64.zip' $nimArchive 33554432
if ((Get-FileHash -LiteralPath $nimArchive -Algorithm SHA256).Hash.ToLowerInvariant() -cne 'fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f') { throw 'quota-intake/nim-archive' }
$nim = Join-Path $OutputRoot 'nim'
if (-not ('QuotaBootstrapInputs' -as [type])) { Add-Type -Path (Join-Path $PSScriptRoot 'QuotaBootstrapInputs.cs') }
$rawArchiveHold = [QuotaBootstrapInputs]::new()
try {
    $rawArchiveHold.HoldFile($OutputRoot, 'nim.zip', 'fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f')
    $nimMembers = @(Expand-DirectZip $nimArchive $nim 134217728)
    $rawArchiveHold.Verify()
} finally { $rawArchiveHold.Dispose() }
if ($nimMembers.Count -ne 5249) { throw 'quota-intake/nim-member-count' }
$nimMembers | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputRoot 'nim-members.json')
$leaseArchive = Join-Path $OutputRoot 'lease.zip'
Get-Bounded 'https://codeload.github.com/metacraft-labs/nim-shm-lease/zip/e3254b7ba1c501de6e64dbb41c96306be5ca6058' $leaseArchive 4194304
$lease = Join-Path $OutputRoot 'lease'
$leaseMembers = @(Expand-DirectZip $leaseArchive $lease 4194304)
$plan = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'staged-major2-bootstrap-source-plan.json') -Raw | ConvertFrom-Json
. (Join-Path $PSScriptRoot 'validate-source-tree.ps1')
Assert-DeclaredSourceTree $lease $plan.leaseCandidate.fullTrackedManifest 136
$result = [pscustomobject]@{nimRoot=$nim;nimMembers=$nimMembers;leaseRoot=$lease;leaseMemberCount=$leaseMembers.Count}
$result | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $OutputRoot 'actual-bootstrap-inputs.json')
Write-Output -NoEnumerate $result
