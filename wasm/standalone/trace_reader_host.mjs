// Instantiate the freestanding reader module the way a browser would, hand it
// containers it did not write, and check what it decodes.
//
//   node trace_reader_host.mjs <module.wasm> <corpus.ct> <legacy.ct> \
//                              <misframed.ct> <corpus.json>
//
// The import object is a literal `{}`. That is the point: if the module needed
// anything from the host — a clock, entropy, a file descriptor — instantiation
// would throw here rather than in a build log. Nothing in this file supplies a
// capability; it only moves bytes in and reads answers out.
//
// The expectations come from `corpus.json`, which the HOST build of
// `trace_reader_corpus.nim` computes from the corpus definition. They are not
// read back out of the module, so a reader that returns a self-consistent
// wrong answer fails here.
//
// Three containers, because they exercise different decoders:
//   * `corpus.ct`   — the SPEC framing, plus source views, IO events, spans
//                     and the line-hit index;
//   * `legacy.ct`   — the pre-M24a Nim-v4 framing, selected by three meta.dat
//                     bits this repo's writer never clears;
//   * `misframed.ct`— those same legacy bytes with meta.dat claiming the SPEC
//                     framing, to see whether a mis-discriminated container
//                     fails or answers.

import { readFileSync } from "node:fs";

const [modulePath, corpusPath, legacyPath, misframedPath, expectedPath] =
  process.argv.slice(2);
if (!modulePath || !corpusPath || !legacyPath || !misframedPath || !expectedPath) {
  console.error(
    "usage: trace_reader_host.mjs <module.wasm> <corpus.ct> <legacy.ct>" +
    " <misframed.ct> <corpus.json>");
  process.exit(2);
}

const expected = JSON.parse(readFileSync(expectedPath, "utf8"));
const corpus = readFileSync(corpusPath);
const legacy = readFileSync(legacyPath);
const misframed = readFileSync(misframedPath);

let failures = 0;
const check = (name, actual, want) => {
  const ok = String(actual) === String(want);
  if (!ok) {
    console.error(`    FAIL ${name}: got ${actual}, expected ${want}`);
    failures += 1;
  }
  return ok;
};

const { instance } = await WebAssembly.instantiate(readFileSync(modulePath), {});
const x = instance.exports;
const mem = () => new Uint8Array(x.memory.buffer);

x.ct_init();

// `ct_num` kinds — must stay in step with the table in trace_reader_abi.nim.
const NUM = {
  sourceViewCount: 0, sourceViewPath: 1, sourceViewKind: 2,
  viewsForPathCount: 3, viewsForPathAt: 4,
  ioCount: 5, ioKind: 6, ioStep: 7, ioDecodes: 8,
  spanRecordCount: 9, spanSettledCount: 10, spanId: 11, spanParent: 12,
  spanIsOpen: 13, spanIsExternal: 14, spanStatus: 15, spanStartStep: 16,
  spanEndStep: 17, spanStructural: 18,
  spanTypeEntryCount: 19, spanTypeIdCount: 20, spanTypeIdAt: 21,
  linehitPositionCount: 22, linehitPresent: 23, linehitCount: 24,
  linehitAt: 25, linehitSum: 26,
  valueCount: 27, valueByte: 28,
  hasSourceViewsFlag: 29, hasSpanStreamFlag: 30,
  hasStepStreamFlag: 31, hasValueStreamFlag: 32, hasIoStreamFlag: 33,
};

const STR = {
  path: 0, func: 1, type: 2, varname: 3,
  viewName: 4, viewContent: 5, viewMap: 6,
  ioData: 7, ioMeta: 8,
  spanLabel: 9, spanType: 10, spanMetadata: 11,
  spanExternalRecording: 12, spanExternalPath: 13,
  spanTypeName: 14,
};

const num = (kind, a = 0, b = 0) => x.ct_num(kind, BigInt(a), BigInt(b));

const decoder = new TextDecoder();
const rawBytes = (kind, id) => {
  // 0 is a real length here (an empty sourcemap, an empty IO payload); only
  // a negative return means the reader refused.
  const n = x.ct_str(kind, BigInt(id));
  if (n < 0) return null;
  if (n === 0) return new Uint8Array(0);
  const p = x.ct_str_ptr();
  return mem().slice(p, p + n);
};
const readStr = (kind, id) => {
  const b = rawBytes(kind, id);
  return b === null ? null : decoder.decode(b);
};
const readHex = (kind, id) => {
  const b = rawBytes(kind, id);
  if (b === null) return null;
  return Array.from(b, (v) => v.toString(16).padStart(2, "0")).join("");
};

const load = (bytes, label) => {
  const dst = x.ct_input_alloc(bytes.length);
  if (dst === 0) {
    console.error(`    FAIL ct_input_alloc returned a null pointer for ${label}`);
    process.exit(1);
  }
  mem().set(bytes, dst);
  check(`ct_input_len (${label})`, x.ct_input_len(), bytes.length);
};

// ===========================================================================
// 1. The SPEC-framed corpus
// ===========================================================================

load(corpus, "corpus.ct");

// --- the module's own verdict on the whole corpus --------------------------
check("ct_verify_input", x.ct_verify_input(), 0);

// --- and the same decode, checked from out here ----------------------------
check("ct_open_input", x.ct_open_input(), 0);
check("ct_column_aware", x.ct_column_aware(), 1);
check("ct_step_count", x.ct_step_count(), expected.steps);
check("ct_path_count", x.ct_path_count(), expected.paths.length);
check("ct_function_count", x.ct_function_count(), expected.functions.length);
check("ct_type_count", x.ct_type_count(), expected.types.length);
check("ct_varname_count", x.ct_varname_count(), expected.varnames.length);
check("ct_call_count", x.ct_call_count(), 1);

expected.paths.forEach((s, i) => check(`path[${i}]`, readStr(STR.path, i), s));
expected.functions.forEach((s, i) => check(`function[${i}]`, readStr(STR.func, i), s));
expected.types.forEach((s, i) => check(`type[${i}]`, readStr(STR.type, i), s));
expected.varnames.forEach((s, i) => check(`varname[${i}]`, readStr(STR.varname, i), s));

for (const p of expected.probes) {
  const pos = x.ct_step_position(BigInt(p.index));
  check(`step[${p.index}].position`, pos, p.position);
  check(`step[${p.index}].file`, x.ct_pos_file(pos), p.file);
  check(`step[${p.index}].line`, x.ct_pos_line(pos), p.line);
  check(`step[${p.index}].column`, x.ct_pos_column(pos), p.column);
}

// --- source views ----------------------------------------------------------
check("meta.has_alternate_source_views", num(NUM.hasSourceViewsFlag), 1);
check("sourceViewCount", num(NUM.sourceViewCount), expected.views.length);
expected.views.forEach((v, i) => {
  check(`view[${i}].pathId`, num(NUM.sourceViewPath, i), v.path);
  check(`view[${i}].kind`, num(NUM.sourceViewKind, i), v.kind);
  check(`view[${i}].name`, readStr(STR.viewName, i), v.name);
  check(`view[${i}].content`, readHex(STR.viewContent, i), v.contentHex);
  check(`view[${i}].sourcemap`, readHex(STR.viewMap, i), v.mapHex);
});
// An index past the table must refuse, not wrap onto a real record.
check("view[out of range]", num(NUM.sourceViewPath, expected.views.length), -2);
expected.viewsForPath.forEach((idx, pathId) => {
  check(`viewsForPath[${pathId}].count`,
    num(NUM.viewsForPathCount, pathId), idx.length);
  idx.forEach((want, k) =>
    check(`viewsForPath[${pathId}][${k}]`,
      num(NUM.viewsForPathAt, pathId, k), want));
});
check("viewsForPath[unknown path]", num(NUM.viewsForPathCount, 99), 0);

// --- IO events -------------------------------------------------------------
check("ioEventCount", num(NUM.ioCount), expected.ioEvents.length);
expected.ioEvents.forEach((e, i) => {
  check(`io[${i}].kind`, num(NUM.ioKind, i), e.kind);
  check(`io[${i}].step`, num(NUM.ioStep, i), e.step);
  check(`io[${i}].data`, readHex(STR.ioData, i), e.dataHex);
  check(`io[${i}].metadata`, readHex(STR.ioMeta, i), e.metaHex);
});
check("io[out of range] refuses",
  num(NUM.ioDecodes, expected.ioEvents.length), 0);

// --- spans -----------------------------------------------------------------
check("meta.has_span_stream", num(NUM.hasSpanStreamFlag), 1);
check("spanRecordCount", num(NUM.spanRecordCount), expected.spanRecords);
check("spanSettledCount", num(NUM.spanSettledCount), expected.spans.length);
expected.spans.forEach((s, i) => {
  check(`span[${i}].id`, num(NUM.spanId, i), s.id);
  check(`span[${i}].parent`, num(NUM.spanParent, i), s.parent);
  check(`span[${i}].isOpen`, num(NUM.spanIsOpen, i), s.isOpen);
  check(`span[${i}].isExternal`, num(NUM.spanIsExternal, i), s.isExternal);
  check(`span[${i}].status`, num(NUM.spanStatus, i), s.status);
  check(`span[${i}].startStep`, num(NUM.spanStartStep, i), s.startStep);
  check(`span[${i}].endStep`, num(NUM.spanEndStep, i), s.endStep);
  check(`span[${i}].structural`, num(NUM.spanStructural, i), s.structural);
  check(`span[${i}].label`, readStr(STR.spanLabel, i), s.label);
  check(`span[${i}].spanType`, readStr(STR.spanType, i), s.spanType);
  // Metadata order is part of the contract; this comparison is order-sensitive
  // because the flattened form preserves the sequence.
  check(`span[${i}].metadata`, readStr(STR.spanMetadata, i), s.metadata);
  check(`span[${i}].externalRecording`,
    readStr(STR.spanExternalRecording, i), s.externalRecording);
  check(`span[${i}].externalPath`,
    readStr(STR.spanExternalPath, i), s.externalPath);
});
// `spantype.ns`: the interned span-type index, read out by name so the check
// does not depend on interning order.
const typeEntryCount = num(NUM.spanTypeEntryCount);
const typeIndexByName = new Map();
for (let i = 0; i < typeEntryCount; i++) {
  typeIndexByName.set(readStr(STR.spanTypeName, i), i);
}
for (const t of expected.spanTypes) {
  const i = typeIndexByName.get(t.name);
  if (i === undefined) {
    console.error(`    FAIL spantype.ns is missing "${t.name}"`);
    failures += 1;
    continue;
  }
  check(`spanType[${t.name}].count`, num(NUM.spanTypeIdCount, i), t.ids.length);
  t.ids.forEach((want, k) =>
    check(`spanType[${t.name}][${k}]`, num(NUM.spanTypeIdAt, i, k), want));
}

// --- line hits -------------------------------------------------------------
check("linehitPositionCount",
  num(NUM.linehitPositionCount), expected.linehitPositions);
for (const p of expected.linehitProbes) {
  check(`linehits[${p.position}].present`,
    num(NUM.linehitPresent, p.position), 1);
  check(`linehits[${p.position}].count`,
    num(NUM.linehitCount, p.position), p.count);
  check(`linehits[${p.position}].first`,
    num(NUM.linehitAt, p.position, 0), p.first);
  check(`linehits[${p.position}].last`,
    num(NUM.linehitAt, p.position, p.count - 1), p.last);
  // The sum is what separates "the right number of step ids" from "the right
  // step ids": a shifted or duplicated list keeps the count and moves this.
  check(`linehits[${p.position}].sum`,
    num(NUM.linehitSum, p.position), p.sum);
}
check("linehits[unexecuted position] absent",
  num(NUM.linehitPresent, expected.linehitAbsent), 0);

// ===========================================================================
// 2. The legacy Nim-v4 framing
// ===========================================================================

load(legacy, "legacy.ct");
check("ct_verify_legacy_input", x.ct_verify_legacy_input(), 0);
check("ct_open_input (legacy)", x.ct_open_input(), 0);

// The discriminator itself. If any of these were set the container would be
// SPEC-framed and everything below would be testing the wrong decoder.
check("legacy meta.has_step_stream", num(NUM.hasStepStreamFlag), 0);
check("legacy meta.has_value_stream", num(NUM.hasValueStreamFlag), 0);
check("legacy meta.has_io_event_stream", num(NUM.hasIoStreamFlag), 0);
check("legacy is line-only", x.ct_column_aware(), 0);

check("legacy step count", x.ct_step_count(), expected.legacy.steps);
check("legacy path count", x.ct_path_count(), expected.legacy.paths.length);
expected.legacy.paths.forEach((s, i) =>
  check(`legacy path[${i}]`, readStr(STR.path, i), s));
check("legacy function[0]", readStr(STR.func, 0), expected.legacy.function);
check("legacy type[0]", readStr(STR.type, 0), expected.legacy.type);
check("legacy varname[0]", readStr(STR.varname, 0), expected.legacy.varname);

expected.legacy.gli.forEach((want, i) =>
  check(`legacy step[${i}].position`, x.ct_step_position(BigInt(i)), want));

expected.legacy.valuesHex.forEach((wantHex, i) => {
  check(`legacy values[${i}].count`, num(NUM.valueCount, i), 1);
  const want = wantHex.match(/../g) ?? [];
  const got = want.map((_, k) => Number(num(NUM.valueByte, i, k)))
    .map((v) => v.toString(16).padStart(2, "0")).join("");
  check(`legacy values[${i}]`, got, wantHex);
});

check("legacy io count", num(NUM.ioCount), expected.legacy.io.length);
expected.legacy.io.forEach((e, i) => {
  check(`legacy io[${i}].kind`, num(NUM.ioKind, i), e.kind);
  check(`legacy io[${i}].step`, num(NUM.ioStep, i), e.step);
  check(`legacy io[${i}].data`, readHex(STR.ioData, i), e.dataHex);
  // The legacy record has no metadata field at all; a SPEC-mode decode of the
  // same bytes would produce one.
  check(`legacy io[${i}].metadata`, readHex(STR.ioMeta, i), "");
});

// ===========================================================================
// 3. Legacy bytes, SPEC-framing flags
// ===========================================================================
//
// Bits: 1 opened, 2 a step count came back, 4 that count was wrong,
//       8 a position resolved, 16 that position was wrong.
// The claim is that wasm agrees with the host about what happens, not that the
// outcome is the desired one — the host measured it, and it is recorded in
// `corpus.json` rather than predicted here.

load(misframed, "misframed.ct");
const bits = x.ct_probe_misframed();
check("ct_probe_misframed", bits, expected.misframedBits);
if ((bits & 4) !== 0 || (bits & 16) !== 0) {
  console.log(`    NOTE: a mis-discriminated container answered with WRONG` +
    ` data (bits ${bits}) rather than refusing`);
} else {
  console.log(`    mis-discriminated container refuses rather than answering` +
    ` (bits ${bits})`);
}

// ===========================================================================
// 4. The module's own container, for the round-trip comparison
// ===========================================================================
// `trace_reader_only_standalone.wasm` links no writer, so this half is absent
// there by design rather than by omission.
if (typeof x.ct_build !== "function") {
  console.log("    (reader-only module: no writer linked, round trip not applicable)");
} else {
  check("ct_build", x.ct_build(), 0);
  const wasmLen = x.ct_len();
  check("ct_len == host container length", wasmLen, corpus.length);
  const wasmBytes = mem().slice(x.ct_ptr(), x.ct_ptr() + wasmLen);
  let firstDiff = -1;
  for (let i = 0; i < Math.min(wasmLen, corpus.length); i++) {
    if (wasmBytes[i] !== corpus[i]) { firstDiff = i; break; }
  }
  if (firstDiff >= 0) {
    console.log(`    note: wasm-written and host-written containers first differ at byte ${firstDiff}`);
  } else if (wasmLen === corpus.length) {
    console.log("    wasm-written container is byte-identical to the host-written one");
  }
}

if (failures > 0) {
  console.error(`    ${failures} check(s) failed`);
  process.exit(1);
}
console.log(`    ${expected.steps} steps, ${expected.probes.length} decoded positions,` +
  ` ${expected.paths.length + expected.functions.length + expected.types.length + expected.varnames.length}` +
  ` interned strings, ${expected.views.length} source views,` +
  ` ${expected.ioEvents.length} IO events, ${expected.spans.length} spans,` +
  ` ${expected.linehitProbes.length} line-hit lists,` +
  ` ${expected.legacy.steps} legacy-framed steps — all as expected`);
