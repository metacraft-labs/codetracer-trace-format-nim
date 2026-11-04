# Validate declared raw supplier bytes before any bootstrap compiler runs.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
function Assert-DirectDirectory([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw "quota-source/direct-directory"
    }
}
function Assert-DeclaredSourceTree([string]$Root, [object[]]$Members, [int]$ExpectedCount) {
    if ($ExpectedCount -le 0 -or $Members.Count -ne $ExpectedCount) { throw 'quota-source/member-count' }
    if (-not [IO.Path]::IsPathFullyQualified($Root)) { throw 'quota-source/absolute-root' }
    Assert-DirectDirectory $Root
    $expected = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($member in $Members) {
        $relative = [string]$member.path
        if ([string]::IsNullOrEmpty($relative) -or $relative.Contains(':') -or [IO.Path]::IsPathRooted($relative) -or $relative.Contains('\') -or
            $relative.Split('/') -contains '' -or $relative.Split('/') -contains '..' -or $relative.Split('/') -contains '.' -or
            -not $expected.Add($relative)) { throw 'quota-source/member-path' }
        $cursor = $Root
        $parts = $relative.Split('/')
        for ($i = 0; $i -lt $parts.Length - 1; $i++) {
            $cursor = Join-Path $cursor $parts[$i]
            Assert-DirectDirectory $cursor
        }
        $file = Get-Item -LiteralPath (Join-Path $Root $relative) -Force
        if ($file.PSIsContainer -or ($file.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'quota-source/direct-file'
        }
        if ($file.Length -ne [long]$member.size -or
            (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant() -cne [string]$member.sha256) {
            throw 'quota-source/member-bytes'
        }
    }
    foreach ($file in Get-ChildItem -LiteralPath $Root -Recurse -Force) {
        if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'quota-source/reparse-entry' }
        if (-not $file.PSIsContainer) {
            $relative = [IO.Path]::GetRelativePath($Root, $file.FullName).Replace('\', '/')
            if (-not $expected.Contains($relative)) { throw 'quota-source/undeclared-member' }
        }
    }
}
