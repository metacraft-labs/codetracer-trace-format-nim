# EXPECTED-GDH2 — the values `tests/run_gdh2_gates.sh` asserts

Every number below is derived by hand or measured once and then **committed**,
so that an assertion which agrees with the script but disagrees with reality is
visible. This is the `EXPECTED-*.md` convention the Godot fork's drivers use
(`codetracer-engine-godot/scripts/EXPECTED-GDH0.md`), applied to the
trace-format side of GDH-M2.

Owner: `codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.milestones.org`
§ GDH-M2. Design: `.../GDScript-Hot-Reload-Multi-Version-Sources.md` §6.3, §6.4.

---

## 1. The byte-identity baseline (GDH-G9)

`gdh2_no_reload_container_is_byte_identical` compares a container produced by a
binary built from **this repo's pre-campaign revision** against one produced by
a binary built from the working tree. The revision is pinned here rather than in
the driver, and the driver refuses to run if it cannot read these fields.

- baseline_revision: 7a8ebde293efc81af5a41e90ec3244b7542b6d5e
- recording_id: 01890000-0000-7000-8000-0000000091d9
- container_bytes: 147456
- container_sha256: 11d7f98809bac4f0675d29d10b6820b434109d1925b823d1893e1aee5df8676f
- member_meta_dat_sha256: 277d92a05950e07cf75ee5275fd8c980cbd5284350e6ecae172690532f5b9149
- member_steps_dat_sha256: be347ab4b7aa5e8f76e350770da670f3909a1807637ca37adaad91a2110d3e73

**Why that revision.** `7a8ebde` ("Merge pull request #6 from
metacraft-labs/correlation-index") is the parent of `d8cb9d8`, which is GDH-M1 —
so it is the last commit before **any** of this campaign landed, not merely the
last before GDH-M2. Pinning it makes the gate prove that M1 **and** M2 together
are byte-neutral for a recorder that never registers a path version and never
reloads, which is a strictly stronger statement than the milestone entry asks
for and costs nothing extra to measure.

**Why the digests are committed and not regenerated.** Regenerating both sides
with the post-campaign binary makes this gate pass unconditionally, and it is
the cheapest possible hollow-out: an implementer who "refreshes the golden" has
deleted the property without touching an assertion. The driver therefore checks
three independent things, and a change to any one of them turns it red:

1. the two containers are byte-identical to each other (`cmp`);
2. both digests equal `container_sha256` above;
3. the baseline worktree resolves to `baseline_revision` **and** contains none
   of `TagSourceReload`, `registerSourceReload`, `FlagExtHasSourceReload`,
   `registerPathVersion` anywhere under `src/`.

Check 3 is what makes "regenerate the baseline with the post-campaign binary"
a red run rather than a green one, and it is deliberately two conditions: a pin
can be pointed at any revision, but a tree either has the campaign's symbols in
it or does not.

**What the probe records** (`tests/gdh2_identity_probe.nim`), asserted by the
driver before the digests are compared at all — a byte-identity result over an
empty container is free:

- recording_id: `01890000-0000-7000-8000-0000000091d9` on BOTH sides, read back
  out of the container rather than echoed from the source constant
- meta_version: `4` on both sides (a no-reload recording must not move to 5)
- paths: 3
- steps: 24
- calls: 2
- values: 24
- member_meta_dat_bytes: 131
- member_steps_dat_bytes: 76

**Exclusion ranges are forbidden.** The `recordingId` is *pinned*, not skipped.
The writer mints a fresh UUIDv7 per recording when the caller passes none
(`multi_stream_writer.nim` `initMultiStreamWriter`), so two recordings of one
program are not byte-identical for a reason that has nothing to do with this
campaign. An exclusion list grows one entry at a time and each entry is
invisible; if some field turns out to be unpinnable, that is a finding to record
and escalate, not a range to skip.

---

## 2. The two-marker fixture (GDH-G7)

`tests/test_gdh2_reload_marker.nim` builds one container with the production
writer. Its shape, and every number the gate asserts against it:

| | value |
| --- | --- |
| paths.dat entries | 5 |
| path 0 | `res://gdh2/probe.gd`, 40 lines — v1 |
| path 1 | `res://gdh2/autoload.gd`, 12 lines — an unrelated file |
| path 2 | `res://gdh2/probe.gd`, 63 lines — v2 |
| path 3 | `res://gdh2/probe.gd`, 71 lines — v3 |
| exec records | 9 |
| of which real steps | 7 |
| of which reload markers | 2 |
| value-stream records | 9 (parallel-indexed to the exec stream) |
| meta.dat schema version | 5 |
| flags_ext | `0x00000001` (`FlagExtHasSourceReload`) |

Marker 1: `reload_ordinal 1`, `changed [{old 0, new 2, generation 2}]`,
`in_flight_frames 3`.
Marker 2: `reload_ordinal 2`, `changed [{old 2, new 3, generation 3}]`,
`in_flight_frames 0`.

The two markers' `in_flight_frames` differ **on purpose**: a field that carries
the same value at every site is not shown to carry anything.

**The cross-ties, which are what GDH-G7 actually asserts.** A marker that is
merely *present* satisfies "the reload is discoverable" while saying nothing
checkable, and a marker of zeros satisfies it too. So every field is tied to
something the marker did not produce:

| marker field | tied to |
| --- | --- |
| `old_path_id` | the path id the step at `step_index - 1` resolves to, via the global position space |
| `new_path_id` | the path id the step at `step_index + 1` resolves to |
| both | the same `paths.dat` payload string, `res://gdh2/probe.gd` |
| `generation` | the new version's container-side version ordinal, plus one (design §7.0 — the two are off by one BY CONSTRUCTION) |
| `reload_ordinal` | its position in the decoded marker sequence |

`stepPathId` resolves through `tryResolve`, never `resolve`: the unchecked form
clamps an address above the top of the space to the last file, yielding a file
id that exists and a line that was never recorded — the exact silent wrong
answer this campaign exists to remove.

The lines v2 and v3 execute (45, 50, 70) lie **past the end of v1** (40 lines).
That is what makes mis-attribution detectable at all: under a single path entry
those addresses fall inside the *next* file's range and read back as a location
that was never recorded (design §2.1, and GDH-M0's measured 129 of 196 steps).

---

## 3. The negative fixture (`gdh2_unknown_tag_is_refused_by_name`)

A real container whose `steps.dat` carries tag `0x08` and whose `meta.dat` does
**not** declare it. The writer refuses to produce that combination, which is why
the fixture is hand-built: a reader's refusal path is not reachable from any
input the writer can make.

It is built by taking the two-marker container above and rewriting its
`meta.dat` header from schema version 5 to version 4 **in place** — the four
`flags_ext` bytes are shifted out at the front and the tail is left as trailing
bytes, which `readMetaDat` ignores. The rewrite keeps `meta.dat`'s length
unchanged, so no CTFS size or block mapping moves.

Asserted before the refusal is asserted (the header must be proven readable, or
a refusal caused by an unreadable file is indistinguishable from one caused by
the tag — HLX-M1's resolver defect verbatim):

- `meta.version == 4`
- `hasSourceReload == false`
- `flagsExt == 0`
- `pathCount >= 2`
- `recordingId` is 36 characters (so the header shifted correctly rather than
  landing mid-field)

Then:

- reading the step stream FAILS,
- the error contains `tag: 8`,
- the error contains `FlagExtHasSourceReload`,
- and the value stream still holds 9 records, so a decode that *succeeded* with
  a different count would be caught by the comparison rather than by the error
  string.

Control arm: the same container **un**rewritten decodes to 9 records. It runs
FIRST, so a fixture that was malformed for an unrelated reason turns the gate
red before the refusal is read as evidence.

---

## 4. The instrument (`ct-print`, run as a binary)

`tests/run_gdh2_gates.sh` builds `src/codetracer_ct_print.nim` and runs the
**binary** — not the library the corpus links, which the CLI does not import.
GDH-M1 found a real drift between those two copies (`has_line_count_table` was
added to the library alone, so the corpus asserted a key no build of the binary
emitted); the marker's rendering is factored into
`src/codetracer_trace_writer/source_reload_json.nim` so that class stays closed
here.

On the two-marker container, `--events` must contain:

- `"kind":"source_reload"`
- `"reload_ordinal":1` and `"reload_ordinal":2`
- `"in_flight_frames":3`
- `"generation":2`

and `--full` must report `"has_source_reload": true` and `"source_reloads": 2`.

On the no-marker container, `--full` must report `"has_source_reload": false`
and `"source_reloads": 0` — **the key present with a zero, not omitted.** A key
that appears only when a marker exists makes a scan for it pass on a trace that
has none AND on a build that cannot see one.

Completeness, by the line-count-equals-header-counts rule: the number of
`"kind":"step"` entries in `--events` must equal `counts.steps`, and the number
of `"kind":"source_reload"` entries must equal `counts.source_reloads`, and the
marker count must be non-zero.

---

## 5. The falsifier arms, and which gate each is aimed at

| arm | gate | what it mutates |
| --- | --- | --- |
| `gdh2FalsifyConstantOrdinal` | `gdh2_reload_marker_round_trips` | the encoder writes the ordinal as a literal `1` — `repro_hcr_agent.c:1338`'s defect transplanted |
| `gdh2FalsifyZeroedMarker` | `gdh2_reload_marker_round_trips` | the writer drops every validation and emits an all-zero marker |
| `gdh2FalsifyUncountedMarker` | `gdh2_reload_marker_round_trips` | the reader consumes the marker's bytes but does not count the record — a shorter, plausible step stream with no error |
| `gdh2FalsifyAlwaysSetBit` | `gdh2_reload_marker_round_trips` + byte-identity | `writeMetaDat` sets the extended flag on every container |
| `gdh2FalsifySkipUnknownTag` | `gdh2_unknown_tag_is_refused_by_name` | the decoder skips tag `0x08` instead of refusing it |
| `gdh2FalsifyUngatedDecode` | `gdh2_unknown_tag_is_refused_by_name` | the decoder accepts tag `0x08` whatever the container declares |
| *(driver-level)* baseline from HEAD | byte-identity | the baseline is regenerated from the post-campaign tree |

`gdh2FalsifyUncountedMarker` is not in the milestone's own list. It was added
because the arm the milestone *does* name for the count comparison —
`gdh2FalsifySkipUnknownTag` — cascades into a different tag error before the
count check is reached, so it goes red on the error string instead. The count
comparison against the value stream is the assertion the entry's falsifier
clause names, and without this arm nothing demonstrates it has teeth. Recorded
here rather than left implicit: **a gate whose named arm cannot reach its
central assertion is a gate that has not been shown to discriminate.**
