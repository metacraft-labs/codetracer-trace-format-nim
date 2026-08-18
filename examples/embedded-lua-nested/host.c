/**************************************************************************/
/*  host.c — CodeTracer nested-trace spike: native host + embedded Lua VM  */
/*  GDScript-Recorder milestone N3 (Generalization — NO engine fork).      */
/**************************************************************************/
//
// WHAT THIS PROVES
// ----------------
// The GDScript recorder (G-series / N1 / N2) proved materialized CTFS tracing
// of an embedded scripting VM via a Godot ENGINE FORK (patching the GDScript
// bytecode interpreter). N3 proves the SAME nested-materialized-trace approach
// generalizes with NO fork: a native C host EMBEDS a scripting VM (Lua) and
// drives the source-level trace entirely through the VM's OWN built-in per-line
// hook (lua_sethook + LUA_MASKLINE | LUA_MASKCALL | LUA_MASKRET). Not a single
// line of Lua's own source is modified — the hook is a public C-API seam Lua
// ships for debuggers. This is the generalization claim: the "materialized
// trace of an embedded VM" pattern needs an engine fork ONLY when the VM lacks a
// per-line hook (GDScript's case); when the VM exposes one (Lua, CPython,
// Ruby), the host uses it directly.
//
// It LINKS the same CTFS writer every CodeTracer recorder links
// (libcodetracer_trace_writer.a + codetracer_trace_writer.h from
// codetracer-trace-format-nim) — codetracer_trace_writer_init() once,
// trace_writer_new(program, 2) for the CTFS multi-stream format — and produces a
// real Lua `.ct` that opens in ct-print (the canonical Nim CTFS decoder).
//
// THE HOST<->VM JOIN (the nested correlation, mirroring N1)
// ---------------------------------------------------------
// Wire contract: codetracer-trace-format-spec/nested-trace-correlation.md.
// At each host<->VM boundary the host samples an (GEID, tick) join key from its
// OWN native coordinate and emits a `ct-nested-join:lua` event into the Lua
// trace's events.dat special-event channel (the exact channel + field layout N1
// authored — no new CTFS stream, no C-ABI change). Three sites, mirroring N1:
//   * call-enter  — host -> Lua (the lua_pcall into the script chunk).
//   * native-call — Lua -> host callback (host_note, a Lua-callable C function):
//                   the crossing where the native host trace IS the continuation
//                   of the Lua source step. The load-bearing site for zoom.
//   * call-exit   — Lua -> host (lua_pcall returns).
//
// WHAT IS REAL vs THE HOST-GEID STAND-IN (honest)
// -----------------------------------------------
//   REAL: the embedded Lua VM (nixpkgs lua-5.4.7), the VM's own line/call/ret
//   hook, the CTFS writer, the produced `.ct`, ct-print, and the native
//   coordinate log the join keys resolve against (host_native_index.json — the
//   host EMITS a real, monotonically-allocated (GEID, tick) sequence and writes
//   it out; the join keys resolve against that real, independently-emitted
//   index, not a fabricated one).
//   STAND-IN: the GEID *source* is a host-side monotonic counter (g_host_geid),
//   standing in for the MCR interposer's GEID allocator (ct_mcr_now /
//   ct_mcr_mark_span_*). Under a real ct-mcr run a native host would sample the
//   real GEID from the interposer exactly here; the counter is documented as the
//   host's native coordinate, analogous to N1's CT_MCR_GEID shim. The SEQUENCE
//   the host emits is real; only its origin is the stand-in.
//
// NO MOCKS: real Lua, real host, real writer, real `.ct`, real ct-print.
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"

// The CTFS writer's C header names a parameter `mutable` (a C++ keyword). This
// is a C translation unit, so no shim is needed; included as-is.
#include "codetracer_trace_writer.h"

// --------------------------------------------------------------------------
// Writer state (mirrors the Python/Ruby/GDScript recorders' lifecycle).
// --------------------------------------------------------------------------
static trace_writer_t g_w = NULL;
static int g_started = 0; // has trace_writer_start emitted the first step?
static value_encoder_t g_enc = NULL;
static uint64_t g_t_float = 0, g_t_string = 0, g_t_bool = 0;
static const char *g_script_path = "script.lua";

// --------------------------------------------------------------------------
// Host native coordinate (the GEID source stand-in). See the header comment:
// a real ct-mcr native host samples these from the interposer; here a monotonic
// counter stands in, and the emitted sequence is written out for the verifier.
// --------------------------------------------------------------------------
#define HOST_GEID_BASE 1000ull
#define HOST_TICK_BASE 500000ull
#define HOST_TICK_STEP 1000ull
static uint64_t g_host_geid = HOST_GEID_BASE;
static uint64_t g_host_tick = HOST_TICK_BASE;

// The native coordinate log the join keys resolve against (a REAL record the
// host emits — the "native trace" side of the correlation, written to
// host_native_index.json for the verifier).
#define MAX_NATIVE 256
static struct {
	uint64_t geid;
	uint64_t tick;
	const char *desc;
} g_native[MAX_NATIVE];
static int g_native_n = 0;

// Allocate the next native (GEID, tick) for a host-side native event and record
// it in the native index. Returns the allocated coordinate via out params.
static void host_alloc_geid(const char *desc, uint64_t *out_geid, uint64_t *out_tick) {
	uint64_t g = g_host_geid, t = g_host_tick;
	if (g_native_n < MAX_NATIVE) {
		g_native[g_native_n].geid = g;
		g_native[g_native_n].tick = t;
		g_native[g_native_n].desc = desc;
		g_native_n++;
	}
	g_host_geid++;
	g_host_tick += HOST_TICK_STEP;
	*out_geid = g;
	*out_tick = t;
}

// Deferred host->Lua call-enter join: allocated before lua_pcall (no Lua step
// exists yet), emitted onto step 0 from the first LINE hook (mirrors the fork's
// deferred-marker discipline). g_host_checksum makes the run's result depend on
// the native side too (deterministic).
static int g_pending_enter = 0;
static uint64_t g_enter_geid = 0, g_enter_tick = 0;
static uint64_t g_host_checksum = 0;

static const char *CT_JOIN_TAG = "ct-nested-join:lua";

// Emit one nested-trace join event bound to the current step (the
// trace_writer_next_step_index() - 1 rule N1 defined; consumers read `step`
// from the payload). site: 0 call-enter / 1 call-exit / 2 native-call.
static void emit_join(int site, uint64_t geid, uint64_t tick) {
	if (!g_w || !g_started) {
		return; // inert before a step exists (byte-identical standalone discipline)
	}
	uint64_t next = trace_writer_next_step_index(g_w);
	uint64_t step = (next > 0) ? (next - 1) : 0;
	const char *site_str = (site == 0) ? "call-enter" : (site == 1) ? "call-exit"
																	: "native-call";
	uint64_t thread = 1; // single-threaded host; main == 1 (N1 convention)
	char content[224];
	snprintf(content, sizeof(content),
			"%s geid=%llu tick=%llu step=%llu site=%s thread=%llu",
			CT_JOIN_TAG, (unsigned long long)geid, (unsigned long long)tick,
			(unsigned long long)step, site_str, (unsigned long long)thread);
	char meta[192];
	snprintf(meta, sizeof(meta),
			"geid=%llu tick=%llu step=%llu site=%s thread=%llu",
			(unsigned long long)geid, (unsigned long long)tick,
			(unsigned long long)step, site_str, (unsigned long long)thread);
	trace_writer_register_special_event(g_w, FFI_EVENT_TRACE_LOG_EVENT, meta, content);
}

// --------------------------------------------------------------------------
// Value capture: encode a scalar Lua local via the writer's streaming ct_value_*
// CBOR encoder + register_variable_cbor (the exact pipeline G4 uses), or the
// simpler register_variable_int for integers.
// --------------------------------------------------------------------------
static void capture_scalar(lua_State *L, const char *name, int idx) {
	if (lua_isinteger(L, idx)) {
		trace_writer_register_variable_int(g_w, name, (int64_t)lua_tointeger(L, idx),
				FFI_TYPE_INT, "int");
		return;
	}
	if (lua_isnumber(L, idx)) {
		ct_value_encoder_reset(g_enc);
		ct_value_write_float(g_enc, (double)lua_tonumber(L, idx), g_t_float);
		size_t n = 0;
		const uint8_t *b = ct_value_get_bytes(g_enc, &n);
		if (b && n) {
			trace_writer_register_variable_cbor(g_w, name, b, n);
		}
		return;
	}
	if (lua_isboolean(L, idx)) {
		ct_value_encoder_reset(g_enc);
		ct_value_write_bool_typed(g_enc, lua_toboolean(L, idx), g_t_bool);
		size_t n = 0;
		const uint8_t *b = ct_value_get_bytes(g_enc, &n);
		if (b && n) {
			trace_writer_register_variable_cbor(g_w, name, b, n);
		}
		return;
	}
	if (lua_type(L, idx) == LUA_TSTRING) {
		size_t slen = 0;
		const char *s = lua_tolstring(L, idx, &slen);
		ct_value_encoder_reset(g_enc);
		ct_value_write_string(g_enc, (const uint8_t *)s, slen, g_t_string);
		size_t n = 0;
		const uint8_t *b = ct_value_get_bytes(g_enc, &n);
		if (b && n) {
			trace_writer_register_variable_cbor(g_w, name, b, n);
		}
		return;
	}
}

// --------------------------------------------------------------------------
// THE PER-LINE HOOK — the whole materialized trace comes from here. NO fork:
// this is Lua's own lua_Hook, installed with lua_sethook.
// --------------------------------------------------------------------------
static void ct_lua_hook(lua_State *L, lua_Debug *ar) {
	if (ar->event == LUA_HOOKLINE) {
		lua_getinfo(L, "Sl", ar);
		const char *src = ar->source;
		if (src && src[0] == '@') {
			src++; // strip Lua's '@' file-chunk marker
		}
		if (!src || src[0] == '\0') {
			src = g_script_path;
		}
		if (!g_started) {
			g_started = 1;
			trace_writer_start(g_w, src, (int64_t)ar->currentline);
			// Emit the deferred host->Lua call-enter join onto this first step.
			if (g_pending_enter) {
				g_pending_enter = 0;
				emit_join(0 /* call-enter */, g_enter_geid, g_enter_tick);
			}
		} else {
			trace_writer_register_step(g_w, src, (int64_t)ar->currentline);
		}
		// Capture the frame's named scalar locals (skip Lua's internal
		// "(temporary)" / "(for state)" slots, which start with '(').
		for (int i = 1; i <= 32; i++) {
			const char *name = lua_getlocal(L, ar, i);
			if (!name) {
				break;
			}
			if (name[0] != '(') {
				capture_scalar(L, name, -1);
			}
			lua_pop(L, 1); // pop the value lua_getlocal pushed
		}
	} else if (ar->event == LUA_HOOKCALL || ar->event == LUA_HOOKTAILCALL) {
		lua_getinfo(L, "Sn", ar);
		const char *name = (ar->name && ar->name[0]) ? ar->name : "(main)";
		const char *src = ar->source;
		if (src && src[0] == '@') {
			src++;
		}
		if (!src || src[0] == '\0') {
			src = g_script_path;
		}
		size_t fid = trace_writer_ensure_function_id(g_w, name, src,
				(int64_t)ar->linedefined);
		trace_writer_register_call(g_w, fid);
	} else if (ar->event == LUA_HOOKRET) {
		trace_writer_register_return(g_w);
	}
}

// --------------------------------------------------------------------------
// host_note(key, value) — a Lua-callable C function: the Lua -> host callback
// boundary (the native-call join site). Each call allocates a fresh native GEID
// (the host's native coordinate for this crossing) and emits a native-call join
// into the Lua trace.
// --------------------------------------------------------------------------
static int host_note(lua_State *L) {
	const char *key = luaL_optstring(L, 1, "?");
	lua_Integer val = luaL_optinteger(L, 2, 0);
	uint64_t g = 0, t = 0;
	host_alloc_geid("host_note", &g, &t);
	emit_join(2 /* native-call */, g, t);
	// Native-side work folded into the deterministic checksum.
	g_host_checksum += (uint64_t)val + (uint64_t)(unsigned char)key[0];
	return 0; // no Lua return values
}

int main(int argc, char **argv) {
	const char *out_dir = getenv("CT_LUA_TRACE");
	if (!out_dir || out_dir[0] == '\0') {
		fprintf(stderr, "host: set CT_LUA_TRACE=<out-dir>\n");
		return 2;
	}
	const char *script = (argc > 1) ? argv[1] : "script.lua";
	g_script_path = script;

	// ---- CTFS writer bring-up (the codetracer-nim / python / ruby pattern) --
	codetracer_trace_writer_init();
	g_w = trace_writer_new("lua_trace", FFI_TRACE_FORMAT_BINARY); // format 2 == CTFS
	if (!g_w) {
		fprintf(stderr, "host: trace_writer_new failed: %s\n", trace_writer_last_error());
		return 3;
	}
	char events_path[4096];
	snprintf(events_path, sizeof(events_path), "%s/events.bin", out_dir);
	trace_writer_set_workdir(g_w, out_dir);
	trace_writer_begin_metadata(g_w, "");
	trace_writer_begin_events(g_w, events_path);
	trace_writer_begin_paths(g_w, "");
	g_enc = ct_value_encoder_new();
	g_t_float = trace_writer_ensure_type_id(g_w, FFI_TYPE_FLOAT, "float");
	g_t_string = trace_writer_ensure_type_id(g_w, FFI_TYPE_STRING, "string");
	g_t_bool = trace_writer_ensure_type_id(g_w, FFI_TYPE_BOOL, "bool");

	// ---- native event: host start (native-only, before the VM enter) --------
	uint64_t sg = 0, st = 0;
	host_alloc_geid("host_start", &sg, &st);

	// ---- embed Lua, install the per-line hook, run the script ---------------
	lua_State *L = luaL_newstate();
	luaL_openlibs(L);
	lua_register(L, "host_note", host_note); // the Lua -> host callback

	if (luaL_loadfile(L, script) != LUA_OK) {
		fprintf(stderr, "host: load %s: %s\n", script, lua_tostring(L, -1));
		return 4;
	}

	// The VM's OWN per-line hook — the entire nested trace flows from here.
	lua_sethook(L, ct_lua_hook, LUA_MASKLINE | LUA_MASKCALL | LUA_MASKRET, 0);

	// host -> VM call boundary: allocate the call-enter native GEID and defer the
	// join to the first Lua step (no step exists yet).
	host_alloc_geid("host->vm enter (lua_pcall)", &g_enter_geid, &g_enter_tick);
	g_pending_enter = 1;

	int rc = lua_pcall(L, 0, 1, 0);
	if (rc != LUA_OK) {
		fprintf(stderr, "host: run %s: %s\n", script, lua_tostring(L, -1));
		return 5;
	}

	// VM -> host return boundary: allocate the call-exit native GEID and emit the
	// call-exit join, bound to the last recorded Lua step.
	int64_t result = (int64_t)luaL_optinteger(L, -1, 0);
	uint64_t xg = 0, xt = 0;
	host_alloc_geid("vm->host return (lua_pcall)", &xg, &xt);
	emit_join(1 /* call-exit */, xg, xt);
	lua_close(L);

	// ---- native event: host shutdown (native-only, after the VM exit) -------
	uint64_t hg = 0, ht = 0;
	host_alloc_geid("host_shutdown", &hg, &ht);

	// ---- serialize the Lua .ct ----------------------------------------------
	trace_writer_finish_events(g_w);
	trace_writer_finish_metadata(g_w);
	trace_writer_finish_paths(g_w);
	trace_writer_close(g_w);
	trace_writer_free(g_w);
	if (g_enc) {
		ct_value_encoder_free(g_enc);
	}

	// ---- emit the host native coordinate index (the join keys resolve against
	//      this real, independently-emitted native trace) ---------------------
	char idx_path[4096];
	snprintf(idx_path, sizeof(idx_path), "%s/host_native_index.json", out_dir);
	FILE *f = fopen(idx_path, "w");
	if (f) {
		fprintf(f, "{\n  \"base_geid\": %llu,\n  \"base_tick\": %llu,\n  \"events\": [\n",
				(unsigned long long)HOST_GEID_BASE, (unsigned long long)HOST_TICK_BASE);
		for (int i = 0; i < g_native_n; i++) {
			fprintf(f, "    {\"geid\": %llu, \"tick\": %llu, \"desc\": \"%s\"}%s\n",
					(unsigned long long)g_native[i].geid,
					(unsigned long long)g_native[i].tick,
					g_native[i].desc, (i + 1 < g_native_n) ? "," : "");
		}
		fprintf(f, "  ]\n}\n");
		fclose(f);
	}

	// ---- deterministic result / checksum ------------------------------------
	uint64_t checksum = (uint64_t)result + g_host_checksum + (uint64_t)g_native_n;
	printf("CT_N3_RESULT result=%lld host_checksum=%llu native_events=%d checksum=%llu\n",
			(long long)result, (unsigned long long)g_host_checksum, g_native_n,
			(unsigned long long)checksum);
	return 0;
}
