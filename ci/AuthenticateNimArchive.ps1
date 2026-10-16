# Archive-member authority is independent of the selected prefix's own bytes.
# Holds are retained by the caller through selected-compiler probes/publication.
function Read-AuthenticatedNimArchive([string]$Archive, $Hold) {
    $sha = 'fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f'
    if (-not [IO.Path]::IsPathFullyQualified($Archive)) { throw 'nim-archive/absolute-path' }
    $Hold.HoldFile((Split-Path -Parent $Archive), (Split-Path -Leaf $Archive), $sha)
    $zip = [IO.Compression.ZipFile]::OpenRead($Archive)
    $members = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::OrdinalIgnoreCase)
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    [long]$total = 0; $top = $null
    try {
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName
            if ([string]::IsNullOrEmpty($name) -or $name.Contains([char]92) -or $name.Contains(':') -or $name.StartsWith('/')) { throw 'nim-archive/path' }
            $parts = $name.TrimEnd('/').Split('/')
            if ($parts -contains '' -or $parts -contains '.' -or $parts -contains '..' -or -not $seen.Add($name)) { throw 'nim-archive/path' }
            if ($null -eq $top) { $top = $parts[0] }
            if ($parts[0] -cne $top) { throw 'nim-archive/top' }
            $kind = ($entry.ExternalAttributes -shr 16) -band 0xf000
            if ($kind -notin @(0,0x8000,0x4000)) { throw 'nim-archive/kind' }
            if ($name.EndsWith('/')) { continue }
            if ($parts.Length -lt 2) { throw 'nim-archive/root-file' }
            $relative = [string]::Join('/', $parts[1..($parts.Length-1)])
            $stream = $entry.Open(); $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
            [long]$actual = 0
            try {
                $buffer = [byte[]]::new(65536)
                while (($count = $stream.Read($buffer,0,$buffer.Length)) -gt 0) {
                    $actual += $count; $total += $count
                    if ($actual -gt $entry.Length -or $total -gt 134217728) { throw 'nim-archive/decode-bound' }
                    $hash.AppendData($buffer,0,$count)
                }
                if ($actual -ne $entry.Length) { throw 'nim-archive/member-size' }
                $digest = [BitConverter]::ToString($hash.GetHashAndReset()).Replace('-','').ToLowerInvariant()
            } finally { $hash.Dispose(); $stream.Dispose() }
            $members.Add($relative,[pscustomobject]@{path=$relative;size=$actual;sha256=$digest})
        }
        if ($members.Count -ne 5249 -or $total -ne 102062602) { throw 'nim-archive/member-census' }
        $Hold.Verify()
        return ,$members
    } finally { $zip.Dispose() }
}
function Assert-NimPrefixInventory([string]$Prefix, $Members, $Hold) {
    if ($Members.Count -ne 5249) { throw 'nim-prefix/authority-count' }
    $extras = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    # Exact files added by authenticated SDK064 tarball materialization/registerInUnifiedStore.
    [void]$extras.Add('.reprobuild-tarball-receipt.json'); [void]$extras.Add('.repro-receipt')
    $found = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $directories = [Collections.Generic.Queue[string]]::new(); $directories.Enqueue($Prefix)
    while ($directories.Count -gt 0) {
        $directory = $directories.Dequeue(); $Hold.HoldDirectory($directory)
        foreach ($entry in [IO.Directory]::EnumerateFileSystemEntries($directory)) {
            $attributes = [IO.File]::GetAttributes($entry)
            if ($attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'nim-prefix/reparse' }
            if ($attributes -band [IO.FileAttributes]::Directory) { $directories.Enqueue($entry); continue }
            $relative = [IO.Path]::GetRelativePath($Prefix,$entry).Replace([char]92,[char]47)
            if (-not $found.Add($relative)) { throw 'nim-prefix/duplicate' }
            if ($Members.ContainsKey($relative)) {
                if ([IO.FileInfo]::new($entry).Length -ne $Members[$relative].size) { throw 'nim-prefix/member-size' }
                $Hold.HoldFile($Prefix,$relative,$Members[$relative].sha256)
            } elseif (-not $extras.Contains($relative)) {
                # Refusal-only witness: filenames and metadata, never contents.
                # The untrusted member remains rejected without reading its bytes.
                try {
                    [Console]::Error.WriteLine(([ordered]@{
                        schemaId='trace_nim.undeclared_prefix_member.v1'
                        relativePath=$relative; kind='file'; length=[IO.FileInfo]::new($entry).Length
                    } | ConvertTo-Json -Compress))
                } catch {
                    # A vanished member or unavailable log must not mask refusal.
                }
                throw 'nim-prefix/undeclared-member'
            }
        }
    }
    foreach ($member in $Members.Keys) {
        if (-not $found.Contains($member)) { throw 'nim-prefix/missing-member' }
    }
    # Both SDK-defined metadata files must exist and remain unchanged. Only the
    # tarball JSON schema is validated here; the binary receipt is not authority.
    foreach ($extra in $extras) {
        if (-not $found.Contains($extra)) { throw 'nim-prefix/missing-sdk-metadata' }
        $digest = (Get-FileHash -LiteralPath (Join-Path $Prefix $extra) -Algorithm SHA256).Hash.ToLowerInvariant()
        $Hold.HoldFile($Prefix,$extra,$digest)
    }
    $Hold.Verify()
}
