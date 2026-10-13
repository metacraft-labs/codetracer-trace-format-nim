# Call the selected Nimble writers, preserving their installed-package and cache semantics.
import std/[os,json]
import nimblepkg/[options,nimbledatafile,downloadnim,packageinfo,version,sha1hashes,cli]
if paramCount()!=1: quit(64)
let root=paramStr(1)
for name in ["nimbledata2.json","official-nim-releases.json"]:
  if fileExists(root/name) or symlinkExists(root/name): quit(65)
setSuppressMessages(true) # Nimble public dump-mode support keeps stdout machine readable.
var opts=Options(nimbleDir:root)
let installed=getInstalledPkgsMin(root/"pkgs2",opts)
loadNimbleData(opts)
saveNimbleData(opts)
let releases=getOfficialReleases(opts)
let written=parseFile(root/"official-nim-releases.json")
if written.kind!=JArray or written.len!=releases.len or releases.len==0: quit(66)
for index,release in releases:
  if written[index]["version"].getStr() != $release: quit(67)
let data=parseFile(root/"nimbledata2.json")
if data!=opts.nimbleData: quit(68)
var packages=newJArray()
for pkg in installed:
  packages.add(%*{"name":pkg.basicInfo.name,"version": $pkg.basicInfo.version,"checksum": $pkg.basicInfo.checksum})
echo %*{"installedPackages":packages,"releaseCount":releases.len}
