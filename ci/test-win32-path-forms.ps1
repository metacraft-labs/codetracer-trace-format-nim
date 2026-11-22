# Pure string checks of the extended-length path conversion (Win32Path) that
# QuotaBootstrapInputs and NimPayloadBinding apply before every raw CreateFileW.
# No filesystem access and no Win32 call is made, so this runs on any host
# with pwsh (Add-Type compiles the DllImports; they are bound only on first use).
# https://learn.microsoft.com/windows/win32/fileio/maximum-file-path-limitation
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not ('QuotaBootstrapInputs' -as [type])) { Add-Type -Path (Join-Path $PSScriptRoot 'windows-quota-bootstrap/QuotaBootstrapInputs.cs') }
if (-not ('NimPayloadBinding' -as [type])) { Add-Type -Path (Join-Path $PSScriptRoot 'NimPayloadBinding.cs') }

# The 272-character path win-ci-bare-001 refused with win32 3 (PR #22 run 44906171087).
$long = 'C:\actions-runner\_work\codetracer-trace-format-nim\codetracer-trace-format-nim\ci\windows-nim-prerequisite\.repro\develop\tool-store\prefixes\nim\2.2.10-c5d1eed0cf00ccb2\dist\nimble\tests\conflictingdepres\nimbledeps\pkgs2\b-0.1.4-16deae68214d584169ab5461ee97aee557e047fb'
$accepted = [ordered]@{
  'C:\x'                     = '\\?\C:\x'
  'd:\a\b c\d.txt'           = '\\?\d:\a\b c\d.txt'
  'C:\'                      = '\\?\C:\'
  '\\server\share'           = '\\?\UNC\server\share'
  '\\server\share\dir\f.nim' = '\\?\UNC\server\share\dir\f.nim'
  '\\?\C:\x'                 = '\\?\C:\x'
  '\\?\UNC\server\share\x'   = '\\?\UNC\server\share\x'
  $long                      = '\\?\' + $long
}
$refused = @(
  '', 'x', 'x\y', '.\x', '..\x', '\x', 'C:x', 'C:', '1:\x', 'C:/x', 'C:\a/b',
  'C:\a\.\b', 'C:\a\..\b', 'C:\a\\b', 'C:\a\', 'C:\a.', 'C:\a ', '\\server',
  '\\server\', '\\.\C:\x', '\\\server\share', '\??\C:\x'
)
$failures = [Collections.Generic.List[string]]::new()
foreach ($type in @([QuotaBootstrapInputs], [NimPayloadBinding])) {
  foreach ($case in $accepted.GetEnumerator()) {
    $actual = $type::Win32Path($case.Key)
    if ($actual -cne $case.Value) { $failures.Add("$($type.Name): '$($case.Key)' -> '$actual', expected '$($case.Value)'") }
  }
  foreach ($case in $refused) {
    try { $actual = $type::Win32Path($case); $failures.Add("$($type.Name): '$case' accepted as '$actual'") }
    catch { if ($_.Exception.GetBaseException() -isnot [ArgumentException]) { throw } }
  }
  # [NullString]::Value: PowerShell would otherwise pass $null to a string parameter as an empty string.
  try { $null = $type::Win32Path([NullString]::Value); $failures.Add("$($type.Name): null accepted") }
  catch { if ($_.Exception.GetBaseException() -isnot [ArgumentNullException]) { throw } }
}
if ($failures.Count -ne 0) { throw ("Win32Path checks failed:`n" + ($failures -join "`n")) }
"Win32Path: $($accepted.Count) accepted and $($refused.Count + 1) refused forms agree in both classes"
