# Call the selected Nimble writers, preserving their installed-package and cache semantics.
import std/[os,json,sets]
import nimblepkg/[options,nimbledatafile,downloadnim,packageinfo,version,sha1hashes,cli,packageparser,reversedeps,packageinfotypes]
if paramCount() notin [1,3]: quit(64)
let root=paramStr(1)
if paramCount()==3:
  if paramStr(2)!="--expected-reader-data": quit(64)
  setSuppressMessages(true)
  var opts=initOptions()
  opts.nimbleDir=root
  let installed=getInstalledPkgs(root/"pkgs2",opts)
  var project=getPkgInfo(paramStr(3),opts)
  project.isLink=true
  let expected=newNimbleDataNode()
  var visited=initHashSet[string]()
  proc visit(pkg: PackageInfo)=
    if pkg.basicInfo.name in visited: return
    visited.incl(pkg.basicInfo.name)
    for requirement in pkg.requires:
      if requirement.name=="nim": continue # Nimble handles its compiler separately.
      var matches: seq[PackageInfo]
      for candidate in installed:
        if candidate.basicInfo.name==requirement.name and candidate.basicInfo.version.withinRange(requirement.ver):
          matches.add(candidate)
      if matches.len!=1: quit(69)
      visit(matches[0])
      addRevDep(expected,matches[0].basicInfo,pkg)
  visit(project)
  echo expected
  quit(0)
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
