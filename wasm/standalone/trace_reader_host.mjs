// Instantiate the freestanding reader module the way a browser would, hand it
// a container it did not write, and check what it decodes.
//
//   node trace_reader_host.mjs <module.wasm> <corpus.ct> <corpus.json>
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

import { readFileSync } from "node:fs";

const [modulePath, corpusPath, expectedPath] = process.argv.slice(2);
if (!modulePath || !corpusPath || !expectedPath) {
  console.error("usage: trace_reader_host.mjs <module.wasm> <corpus.ct> <corpus.json>");
  process.exit(2);
}

const expected = JSON.parse(readFileSync(expectedPath, "utf8"));
const corpus = readFileSync(corpusPath);

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

// --- hand the module bytes it did not write --------------------------------
const dst = x.ct_input_alloc(corpus.length);
if (dst === 0) {
  console.error("    FAIL ct_input_alloc returned a null pointer");
  process.exit(1);
}
mem().set(corpus, dst);
check("ct_input_len", x.ct_input_len(), corpus.length);

// --- the module's own verdict on the whole corpus --------------------------
const verdict = x.ct_verify_input();
check("ct_verify_input", verdict, 0);

// --- and the same decode, checked from out here ----------------------------
check("ct_open_input", x.ct_open_input(), 0);
check("ct_column_aware", x.ct_column_aware(), 1);
check("ct_step_count", x.ct_step_count(), expected.steps);
check("ct_path_count", x.ct_path_count(), expected.paths.length);
check("ct_function_count", x.ct_function_count(), expected.functions.length);
check("ct_type_count", x.ct_type_count(), expected.types.length);
check("ct_varname_count", x.ct_varname_count(), expected.varnames.length);
check("ct_call_count", x.ct_call_count(), 1);

const decoder = new TextDecoder();
const readStr = (kind, id) => {
  const n = x.ct_str(kind, BigInt(id));
  if (n < 0) return null;
  const p = x.ct_str_ptr();
  return decoder.decode(mem().slice(p, p + n));
};

const KIND = { path: 0, func: 1, type: 2, varname: 3 };
expected.paths.forEach((s, i) => check(`path[${i}]`, readStr(KIND.path, i), s));
expected.functions.forEach((s, i) => check(`function[${i}]`, readStr(KIND.func, i), s));
expected.types.forEach((s, i) => check(`type[${i}]`, readStr(KIND.type, i), s));
expected.varnames.forEach((s, i) => check(`varname[${i}]`, readStr(KIND.varname, i), s));

for (const p of expected.probes) {
  const pos = x.ct_step_position(BigInt(p.index));
  check(`step[${p.index}].position`, pos, p.position);
  check(`step[${p.index}].file`, x.ct_pos_file(pos), p.file);
  check(`step[${p.index}].line`, x.ct_pos_line(pos), p.line);
  check(`step[${p.index}].column`, x.ct_pos_column(pos), p.column);
}

// --- the module's own container, for the round-trip comparison -------------
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
  ` interned strings — all as expected`);
