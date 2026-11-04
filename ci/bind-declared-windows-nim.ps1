[CmdletBinding()]
param([Parameter(Mandatory)][string]$AuthenticatedArchive)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (!$IsWindows) { throw 'declared Nim producer requires native Windows' }
$archiveSha = 'fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f'
$archiveUrl = 'https://nim-lang.org/download/nim-2.2.10_x64.zip'
$lockIdentity = "tarball:nim@2.2.10:sha256:$archiveSha"
if (-not $env:REPRO_TOOL_PROFILE_INSPECTION -or
    -not [IO.Path]::IsPathFullyQualified($env:REPRO_TOOL_PROFILE_INSPECTION)) {
  throw 'missing public SDK selected profile inspection'
}
function Assert-DeclaredNimProducer($Profile, $Receipt, [string]$ExportedPrefix) {
  $profileExpected = [ordered]@{
    installMethod='tarball'; packageSelector='nim'; executableName='nim'; packageId='nim@2.2.10';
    tarballUrl=$archiveUrl; tarballSha256=$archiveSha; archiveType='zip';
    declaredExecutablePath='bin/nim.exe'; lockIdentity=$lockIdentity
  }
  $receiptExpected = [ordered]@{
    installMethod='tarball'; packageSelector='nim'; packageId='nim@2.2.10'; url=$archiveUrl;
    sha256=$archiveSha; archiveType='zip'; declaredExecutablePath='bin/nim.exe'; lockIdentity=$lockIdentity
  }
  foreach ($role in @('profile','receipt')) {
    $object = if ($role -eq 'profile') { $Profile } else { $Receipt }
    $expected = if ($role -eq 'profile') { $profileExpected } else { $receiptExpected }
    foreach ($name in $expected.Keys) {
      if ($null -eq $object.PSObject.Properties[$name] -or
          $object.$name -isnot [string] -or $object.$name -cne $expected[$name]) {
        throw "nim-producer/$role/$name"
      }
    }
    if ($null -eq $object.PSObject.Properties['stripComponents'] -or
        ($object.stripComponents -isnot [int] -and $object.stripComponents -isnot [long]) -or
        $object.stripComponents -ne 1) { throw "nim-producer/$role/stripComponents" }
    $pathFields = if ($role -eq 'profile') {
      @('selectedStorePath','realizationBoundary','resolvedExecutablePath')
    } else { @('realizationBoundary') }
    foreach ($name in $pathFields) {
      if ($null -eq $object.PSObject.Properties[$name] -or $object.$name -isnot [string] -or
          -not [IO.Path]::IsPathFullyQualified($object.$name)) { throw "nim-producer/$role/$name" }
    }
  }
  if (-not [IO.Path]::IsPathFullyQualified($ExportedPrefix)) { throw 'nim-producer/exported-prefix' }
  $prefix = [IO.Path]::TrimEndingDirectorySeparator([IO.Path]::GetFullPath($Profile.selectedStorePath))
  # SDK064 writes receipt.realizationBoundary in the extraction directory
  # before moving/publishing it. That typed historical path is not final
  # physical authority; the public profile/exported prefix is, together with
  # complete independent archive-member correspondence below.
  if (-not [string]::Equals($prefix,[IO.Path]::TrimEndingDirectorySeparator([IO.Path]::GetFullPath($Profile.realizationBoundary)),[StringComparison]::OrdinalIgnoreCase)) {
    throw 'nim-producer/profile/realizationBoundary'
  }
  if (-not [string]::Equals($prefix,[IO.Path]::TrimEndingDirectorySeparator([IO.Path]::GetFullPath($ExportedPrefix)),[StringComparison]::OrdinalIgnoreCase)) {
    throw 'nim-producer/exported-prefix'
  }
  if (-not [string]::Equals((Join-Path $prefix 'bin/nim.exe'),[IO.Path]::GetFullPath($Profile.resolvedExecutablePath),[StringComparison]::OrdinalIgnoreCase)) {
    throw 'nim-producer/profile/resolvedExecutablePath'
  }
}
function Assert-AuthenticatedNimMembers($Binding) {
  if ($Binding.ImageSha256 -cne 'ab1bcd5a479e81d2f3c58839b0654a97599aa8c2b015f6b021d6943d5f5b55a2') {
    throw 'nim-member/image'
  }
  if ($Binding.HeaderSha256 -cne '28491d05916eab446de054370808030b33b63fd5623dcd454212adec27ee934d') {
    throw 'nim-member/header'
  }
  $Binding.Verify()
}
Add-Type -Path (Join-Path $PSScriptRoot 'windows-quota-bootstrap/QuotaBootstrapInputs.cs')
. (Join-Path $PSScriptRoot 'AuthenticateNimArchive.ps1')
$archiveAndPrefixHold = [QuotaBootstrapInputs]::new()
try {
    $archiveMembers = Read-AuthenticatedNimArchive $AuthenticatedArchive $archiveAndPrefixHold
    $inspection = Get-Content -LiteralPath $env:REPRO_TOOL_PROFILE_INSPECTION -Raw | ConvertFrom-Json
    $profiles = @($inspection.profiles | Where-Object { $_.packageSelector -ceq 'nim' -and $_.executableName -ceq 'nim' })
    if ($profiles.Count -ne 1) { throw 'expected exactly one declared Nim profile' }
    $profile = $profiles[0]
    $prefix = [string]$profile.selectedStorePath
    if (-not [IO.Path]::IsPathFullyQualified($prefix)) { throw 'missing SDK physical Nim prefix' }
    $receipt = Get-Content -LiteralPath (Join-Path $prefix '.reprobuild-tarball-receipt.json') -Raw | ConvertFrom-Json
    # Positive authority comes from this actual public SDK execution, never a fixture.
    Assert-DeclaredNimProducer $profile $receipt $env:REPRO_PREFIX_NIM
    $controls = [Collections.Generic.List[object]]::new()
    $controls.Add([ordered]@{name='actual-sdk-producer';result='accepted'})
    $historical = $receipt | ConvertTo-Json -Depth 32 | ConvertFrom-Json
    $historical.realizationBoundary = 'Z:\historical-producer\tool-store\tmp\extract.123.456.fe0686a9b298e5b1'
    Assert-DeclaredNimProducer $profile $historical $env:REPRO_PREFIX_NIM
    $controls.Add([ordered]@{name='historical-other-machine-receipt-is-not-physical-authority';result='accepted'})
    foreach ($role in @('profile','receipt')) {
      $original = if ($role -eq 'profile') { $profile } else { $receipt }
      $fields = if ($role -eq 'profile') {
        @('installMethod','packageSelector','executableName','packageId','tarballUrl','tarballSha256','archiveType','stripComponents','declaredExecutablePath','lockIdentity','selectedStorePath','realizationBoundary','resolvedExecutablePath')
      } else {
        @('installMethod','packageSelector','packageId','url','sha256','archiveType','stripComponents','declaredExecutablePath','lockIdentity','realizationBoundary')
      }
      foreach ($field in $fields) {
        foreach ($mutation in @('missing','wrong','wrong-type')) {
          $changed = $original | ConvertTo-Json -Depth 32 | ConvertFrom-Json
          if ($mutation -eq 'missing') { $changed.PSObject.Properties.Remove($field) }
          elseif ($mutation -eq 'wrong-type') { $changed.$field = @('invalid') }
          elseif ($field -eq 'stripComponents') { $changed.$field = 2 }
          else { $changed.$field = 'invalid' }
          $refused = $false
          try {
            if ($role -eq 'profile') { Assert-DeclaredNimProducer $changed $receipt $env:REPRO_PREFIX_NIM }
            else { Assert-DeclaredNimProducer $profile $changed $env:REPRO_PREFIX_NIM }
          } catch {
            if ($_.Exception.Message -cne "nim-producer/$role/$field") { throw }
            $refused = $true
          }
          if (-not $refused) { throw "Nim producer negative accepted: $role/$field/$mutation" }
          $controls.Add([ordered]@{name="$role/$field/$mutation";result='refused'})
        }
      }
    }
    foreach ($export in @('',(Join-Path $env:RUNNER_TEMP 'foreign-nim-payload'))) {
      $refused = $false
      try { Assert-DeclaredNimProducer $profile $receipt $export } catch {
        if ($_.Exception.Message -cne 'nim-producer/exported-prefix') { throw }
        $refused = $true
      }
      if (-not $refused) { throw 'missing/foreign independent Nim prefix was accepted' }
      $controls.Add([ordered]@{name='missing-or-foreign-prefix';result='refused'})
    }
    [ordered]@{scope='actual SDK producer identity and mutation controls';controls=$controls} | ConvertTo-Json -Depth 6
    $selected = Join-Path $prefix 'bin/nim.exe'
    Add-Type -Path (Join-Path $PSScriptRoot 'NimPayloadBinding.cs')
    # Hold and authenticate the independently declared physical payload before any
    # compiler execution. Subsequent original probes still validate actual PATH selection.
    $producer = [NimPayloadBinding]::Bind($prefix,$prefix,$selected,(Join-Path $prefix 'lib'))
    try {
      Assert-AuthenticatedNimMembers $producer
      Assert-NimPrefixInventory $prefix $archiveMembers $archiveAndPrefixHold
      [ordered]@{scope='actual selected prefix matches independently authenticated archive';members=$archiveMembers.Count;archiveSHA256=$archiveSha} | ConvertTo-Json
      # Real disposable copies exercise module-byte and shadow-file refusals.
      $archiveControlRoot = Join-Path $env:RUNNER_TEMP ('nim-archive-members-' + [guid]::NewGuid().ToString('N'))
      [void][IO.Directory]::CreateDirectory($archiveControlRoot)
      $archiveControlError = $null
      try {
        foreach ($memberName in @('lib/system.nim','config/nim.cfg')) {
          $member = $archiveMembers[$memberName]
          $copyPath = Join-Path $archiveControlRoot 'copied-member'
          foreach ($mutation in @('none','changed')) {
            [IO.File]::Copy((Join-Path $prefix $memberName),$copyPath,$false)
            try {
              if ($mutation -eq 'changed') {
                $stream = [IO.File]::Open($copyPath,[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)
                try { $byte=$stream.ReadByte(); $stream.Position=0; $stream.WriteByte([byte]($byte -bxor 1)) } finally { $stream.Dispose() }
              }
              $copyHold = [QuotaBootstrapInputs]::new()
              try {
                $refused = $false
                try { $copyHold.HoldFile($archiveControlRoot,'copied-member',$member.sha256) } catch {
                  $reason=$_.Exception; while ($null -ne $reason.InnerException) { $reason=$reason.InnerException }
                  if ($mutation -ne 'changed' -or $reason.Message -cne 'quota-hold/member-bytes') { throw }
                  $refused=$true
                }
                if (($mutation -eq 'changed') -ne $refused) { throw 'nim-archive/control-result' }
              } finally { $copyHold.Dispose() }
              [ordered]@{scope='actual copied archive module, never executed';member=$memberName;mutation=$mutation;refused=$refused} | ConvertTo-Json
            } finally { [IO.File]::Delete($copyPath) }
          }
        }
        $shadowPath=Join-Path $archiveControlRoot 'shadow.nim'; [IO.File]::WriteAllText($shadowPath,'discard')
        try {
          $shadowHold=[QuotaBootstrapInputs]::new(); $refused=$false
          try {
            try { Assert-NimPrefixInventory $archiveControlRoot $archiveMembers $shadowHold } catch {
              if ($_.Exception.Message -cne 'nim-prefix/undeclared-member') { throw }; $refused=$true
            }
            if (-not $refused) { throw 'nim-archive/shadow-accepted' }
          } finally { $shadowHold.Dispose() }
        } finally { [IO.File]::Delete($shadowPath) }
      } catch { $archiveControlError=$_; throw } finally {
        try { [IO.Directory]::Delete($archiveControlRoot) } catch {
          if ($null -eq $archiveControlError) { throw }; Write-Error -ErrorAction Continue "Nim archive control cleanup also failed: $_"
        }
      }
      if (-not [IO.Path]::IsPathFullyQualified($env:RUNNER_TEMP)) { throw 'missing owned native control root' }
      $controlRoot = Join-Path $env:RUNNER_TEMP ('nim-producer-members-' + [guid]::NewGuid().ToString('N'))
      if (Test-Path -LiteralPath $controlRoot) { throw 'occupied Nim member control namespace' }
      [void][IO.Directory]::CreateDirectory((Join-Path $controlRoot 'bin'))
      [void][IO.Directory]::CreateDirectory((Join-Path $controlRoot 'lib'))
      $controlImage = Join-Path $controlRoot 'bin/nim.exe'
      $controlHeader = Join-Path $controlRoot 'lib/nimbase.h'
      $memberControlError = $null
      try {
        foreach ($mutation in @('none','image','header')) {
          [IO.File]::Copy($producer.ImagePath,$controlImage,$true)
          [IO.File]::Copy((Join-Path $producer.IncludePath 'nimbase.h'),$controlHeader,$true)
          if ($mutation -ne 'none') {
            $target = if ($mutation -eq 'image') { $controlImage } else { $controlHeader }
            $stream = [IO.File]::Open($target,[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)
            try {
              $old = $stream.ReadByte(); $stream.Position = 0; $stream.WriteByte([byte]($old -bxor 1))
            } finally { $stream.Dispose() }
          }
          $copy = [NimPayloadBinding]::Bind($controlRoot,$controlRoot,$controlImage,(Join-Path $controlRoot 'lib'))
          try {
            $refused = $false
            try { Assert-AuthenticatedNimMembers $copy } catch {
              if ($mutation -eq 'none' -or $_.Exception.Message -cne "nim-member/$mutation") { throw }
              $refused = $true
            }
            if (($mutation -eq 'none' -and $refused) -or ($mutation -ne 'none' -and -not $refused)) {
              throw "Nim authenticated member control mismatch: $mutation"
            }
            [ordered]@{scope='actual copied member bytes, never executed';mutation=$mutation;refused=$refused} | ConvertTo-Json
          } finally { $copy.Dispose() }
        }
      } catch {
        $memberControlError = $_
        throw
      } finally {
        try {
          # Only our two known regular copies and exact empty directories; no recursive
          # traversal or deletion of supplier files, junctions or unknown children.
          foreach ($file in @($controlImage,$controlHeader)) {
            if ([IO.File]::Exists($file)) { [IO.File]::Delete($file) }
          }
          [IO.Directory]::Delete((Join-Path $controlRoot 'bin'))
          [IO.Directory]::Delete((Join-Path $controlRoot 'lib'))
          [IO.Directory]::Delete($controlRoot)
        } catch {
          if ($null -eq $memberControlError) { throw }
          Write-Error -ErrorAction Continue "Nim control cleanup also failed: $_"
        }
      }
      # Only the authenticated selected compiler may describe its alias/library paths.
      # Those paths remain distinct from the independent SDK producer prefix above.
      $probe = Join-Path $env:RUNNER_TEMP ('nim-producer-' + [guid]::NewGuid().ToString('N') + '.nim')
      try {
        [IO.File]::WriteAllText($probe, "discard`n")
        $archiveAndPrefixHold.Verify()
        $dump = @(& $selected dump --dump.format:json $probe)
        if ($LASTEXITCODE -ne 0) { throw 'declared Nim public dump failed' }
        $metadata = ($dump -join "`n") | ConvertFrom-Json
        foreach ($name in @('version','nimExe','prefixdir','libpath')) {
          if ($null -eq $metadata.PSObject.Properties[$name] -or $metadata.$name -isnot [string]) {
            throw "invalid selected Nim public dump field/type: $name"
          }
        }
        if ($metadata.version -cne '2.2.10' -or
            -not [string]::Equals([IO.Path]::GetFullPath($metadata.nimExe),$producer.ImagePath,[StringComparison]::OrdinalIgnoreCase)) {
          throw 'selected Nim public dump version/image disagreement'
        }
        $binding = [NimPayloadBinding]::Bind($metadata.prefixdir,$prefix,$selected,$metadata.libpath)
        try {
          Assert-AuthenticatedNimMembers $binding
          $archiveAndPrefixHold.Verify()
          if (-not $env:GITHUB_ENV -or -not $env:GITHUB_PATH -or $prefix -match "[\r\n]") {
            throw 'invalid workflow role publication destinations'
          }
          Add-Content -LiteralPath $env:GITHUB_ENV -Value "REPRO_PREFIX_NIM=$prefix"
          Add-Content -LiteralPath $env:GITHUB_PATH -Value (Join-Path $prefix 'bin')
          [ordered]@{declaredSdkContractRevision='064daf5b154ea6a0758ddb2dcbcbc07685216e44'; archiveSha256=$archiveSha;
            physicalPrefix=$prefix; image=$binding.ImagePath; imageSha256=$binding.ImageSha256;
            headerSha256=$binding.HeaderSha256; profileInspection=$env:REPRO_TOOL_PROFILE_INSPECTION} | ConvertTo-Json -Depth 5
        } finally { $binding.Dispose() }
      } finally {
        if ([IO.File]::Exists($probe)) { [IO.File]::Delete($probe) }
      }

    } finally { $producer.Dispose() }

} finally { $archiveAndPrefixHold.Dispose() }
