# Call the selected Nimble writers, preserving their installed-package and cache semantics.
import std/[os,json,sets,options]
import nimblepkg/[options,nimbledatafile,downloadnim,packageinfo,version,sha1hashes,cli,packageparser,reversedeps,packageinfotypes]
if paramCount() notin [1,3]: quit(64)
let root=paramStr(1)
if paramCount()==3:
  if paramStr(2)!="--expected-reader-data": quit(64)
  setSuppressMessages(true)
  var opts=initOptions()
  opts.nimbleDir=root
  opts.nimBin=some makeNimBin(opts,"nim")
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
# When Nimble cannot fetch the release list from the GitHub API, it catches the
# error and returns a hard-coded list without writing this file
# (nimblepkg/downloadnim.nim, getOfficialReleases). Refuse with exit code 70
# and say why, instead of dying on an unhandled IOError. The usual cause is the
# anonymous per-address API quota. The patched Nimble sends GITHUB_TOKEN when
# it is set (nix/nimble-github-api-auth.patch).
if not fileExists(root/"official-nim-releases.json"):
  stderr.writeLine("construct-runtime-metadata: Nimble did not write official-nim-releases.json: " &
    "the official Nim release query to api.github.com failed" &
    (if existsEnv("GITHUB_TOKEN"): " even with GITHUB_TOKEN set (check the token)."
     else: " without a GITHUB_TOKEN; the anonymous per-address quota is likely spent. Set GITHUB_TOKEN and retry."))
  quit(70)
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
