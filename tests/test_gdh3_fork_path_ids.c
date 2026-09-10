/*
 * GDH-M3 — the C ABI can mint a version, and a host stops re-deriving path ids.
 *
 * Two gates of
 * codetracer-specs/Planned-Features/GDScript-Hot-Reload-Multi-Version-Sources.milestones.org
 * § GDH-M3:
 *
 *   1. gdh3_fork_uses_the_writers_own_path_id
 *   2. gdh3_refused_registration_reaches_the_c_caller
 *
 * allowed_mocks: none, and none are used. This program links the REAL
 * libcodetracer_trace_writer.a through the REAL include/codetracer_trace_writer.h
 * — the same archive and the same header the Godot fork vendors — and drives it
 * with the same call sequence the fork's source bundler performs:
 *
 *     trace_writer_register_step(writer, path, line);
 *     id = <the path id for `path`>;
 *     trace_writer_register_source_view(writer, id, 0, name, ..., bytes, ...);
 *
 * The only thing under test is where `id` comes from. Before GDH-M3 the fork
 * re-derived it from a LOCAL MIRROR of the writer's private interning counter
 * (`g_ct_next_path_id`, gdscript_ct_trace.cpp), correct only while the writer
 * interns in first-seen order from 0. `trace_writer_register_path_version`
 * makes that false, and the mirror is then wrong SILENTLY: every source view
 * after the first reload attaches to the wrong file.
 *
 * -DGDH3_FALSIFY_MIRROR_COUNTER reinstates the mirror, faithfully — including
 * the string-keyed lookup that makes the reloaded file resolve back to the
 * FIRST version's id. With one version the mirror and the writer agree and the
 * gate passes (that is the `--single-version` control arm); with two it must go
 * red, which is precisely why the defect is latent today.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "codetracer_trace_writer.h"

static const char *PROBE = "res://gdh3/probe.gd";
static const char *OTHER = "res://gdh3/autoload.gd";

static const char *V1_TEXT =
    "extends Node\n# version one\nfunc probe():\n\tprint(\"GDH3_V1\")\n";
static const char *V2_TEXT =
    "extends Node\n# version two, longer\n# pad\n# pad\nfunc probe():\n"
    "\tprint(\"GDH3_V2\")\n";
static const char *OTHER_TEXT = "extends Node\nfunc other():\n\tpass\n";

#define GATE_FAIL(gate, ...) do { \
    fprintf(stderr, "GDH3-FAIL[%s]: ", gate); \
    fprintf(stderr, __VA_ARGS__); \
    fprintf(stderr, "\n  last_error: %s\n", trace_writer_last_error()); \
    return 1; \
} while (0)

/* -------------------------------------------------------------------------
 * The mirror, reinstated only under the falsifier define.
 * ------------------------------------------------------------------------- */
#ifdef GDH3_FALSIFY_MIRROR_COUNTER
#define MIRROR_MAX 8
static const char *g_mirror_paths[MIRROR_MAX];
static uint64_t g_mirror_ids[MIRROR_MAX];
static int g_mirror_len = 0;
static uint64_t g_ct_next_path_id = 0;

/* gdscript_ct_note_and_bundle_path_locked, as it stood before GDH-M3:
 * first sight of a path STRING mints the next mirrored id; a string already
 * seen answers with the id it was first given. */
static uint64_t mirror_path_id(const char *path) {
    for (int i = 0; i < g_mirror_len; i++) {
        if (strcmp(g_mirror_paths[i], path) == 0) {
            return g_mirror_ids[i];
        }
    }
    uint64_t id = g_ct_next_path_id++;
    g_mirror_paths[g_mirror_len] = path;
    g_mirror_ids[g_mirror_len] = id;
    g_mirror_len++;
    return id;
}
#endif

static uint64_t path_id_for_bundle(trace_writer_t w, const char *path) {
#ifdef GDH3_FALSIFY_MIRROR_COUNTER
    (void)w;
    return mirror_path_id(path);
#elif defined(GDH3_FALSIFY_SILENT_SKIP_RELOAD)
    /* Added by review.  The real bundler
     * (gdscript_ct_trace.cpp gdscript_ct_note_and_bundle_path_locked) treats
     * CT_TW_INVALID_PATH_ID as "the writer does not know this path" and
     * RETURNS — deliberately, so an unknown path is skipped rather than
     * bundled against an invented id.  That degradation is honest only while
     * the sentinel really means "unknown".  This arm makes the lookup answer
     * the sentinel for a path the writer knows perfectly well, so the fork
     * takes its skip path for a file it was supposed to bundle: the reloaded
     * version's text never reaches the container, nothing fails, and the
     * recording still opens.  That is GDH-M0's silent mode wearing the
     * error-handling branch as a disguise.  No other arm reaches it — the
     * mirror counter bundles the WRONG id, a value the gate can compare,
     * whereas this one bundles NOTHING and leaves nothing to compare.
     *
     * Scoped to the POST-RELOAD lookup, and the scoping is the arm's
     * discipline rather than a convenience: an unconditional sentinel breaks
     * v1's bundle too, so the single-version control goes red and the arm is
     * shown to fail everywhere instead of to discriminate.  Measured — the
     * first draft did exactly that and the control caught it, which is the
     * control doing its job. */
    {
        static int probe_lookups = 0;
        if (strcmp(path, PROBE) == 0 && ++probe_lookups >= 2) {
            return CT_TW_INVALID_PATH_ID;
        }
        return trace_writer_current_path_id(w, path);
    }
#else
    return trace_writer_current_path_id(w, path);
#endif
}

static int64_t bundle(trace_writer_t w, uint64_t path_id, const char *name,
                      const char *text) {
    return trace_writer_register_source_view(
        w, path_id, /*view_kind=*/0, name, strlen(name),
        (const uint8_t *)text, strlen(text), NULL, 0);
}

/* -------------------------------------------------------------------------
 * Gate 1's producer.  Writes the container and prints, on stdout, the id the
 * WRITER assigned to each registration and the id each source view was
 * attached under.  The verifier reads the container and compares.
 * ------------------------------------------------------------------------- */
static int produce(const char *out_dir, int two_versions) {
    codetracer_trace_writer_init();
    trace_writer_t w = trace_writer_new("gdh3_probe", FFI_TRACE_FORMAT_BINARY);
    if (!w) GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                      "trace_writer_new returned NULL");

    char events[1024];
    snprintf(events, sizeof(events), "%s/events.bin", out_dir);
    trace_writer_set_workdir(w, out_dir);
    trace_writer_begin_metadata(w, "");
    if (trace_writer_begin_events(w, events) != 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "begin_events failed");
    trace_writer_begin_paths(w, "");

    /* THE HEADER GAP, exercised rather than asserted.  Until GDH-M3 neither
     * of these two entry points was DECLARED in the header the fork vendors,
     * so a host could not turn bit 14 on at all — the concrete reason GDH-M0
     * measured meta.dat bit 14 clear.  Versioned paths are defined only on
     * the bit-14 record layout, so this call is a prerequisite, not a flavour. */
    if (trace_writer_enable_line_count_table(w) != 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "trace_writer_enable_line_count_table failed");
    if (trace_writer_register_path_with_line_count(w, PROBE, 40) != 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "register_path_with_line_count(%s) failed", PROBE);
    if (trace_writer_register_path_with_line_count(w, OTHER, 12) != 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "register_path_with_line_count(%s) failed", OTHER);

    /* v1 runs, and its source is bundled under the id the writer gave it. */
    trace_writer_start(w, PROBE, 3);
    uint64_t v1 = path_id_for_bundle(w, PROBE);
    if (v1 == CT_TW_INVALID_PATH_ID)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "no path id for %s", PROBE);
    if (bundle(w, v1, PROBE, V1_TEXT) < 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "register_source_view(v1) failed");
    printf("writer_v1_id=%llu\n", (unsigned long long)v1);
    printf("bundled_v1_under=%llu\n", (unsigned long long)v1);

    /* An unrelated file, stepped and bundled between the two versions, so a
     * mirror that drifts has somewhere to drift TO. */
    trace_writer_register_step(w, OTHER, 4);
    uint64_t oid = path_id_for_bundle(w, OTHER);
    if (oid == CT_TW_INVALID_PATH_ID)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "no path id for %s", OTHER);
    if (bundle(w, oid, OTHER, OTHER_TEXT) < 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                  "register_source_view(other) failed");
    printf("writer_other_id=%llu\n", (unsigned long long)oid);

    trace_writer_register_step(w, PROBE, 4);

    if (two_versions) {
        /* THE RELOAD.  The writer mints a second id for the same string; the
         * new version's source is bundled under whatever `path_id_for_bundle`
         * answers.  This is the single line the milestone is about. */
        uint64_t v2 = trace_writer_register_path_version(w, PROBE, 63);
        if (v2 == CT_TW_INVALID_PATH_ID)
            GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                      "register_path_version(%s, 63) failed", PROBE);
        printf("writer_v2_id=%llu\n", (unsigned long long)v2);

        uint64_t bundle_id = path_id_for_bundle(w, PROBE);
#ifdef GDH3_FALSIFY_SILENT_SKIP_RELOAD
        /* The fork's own reaction to the sentinel, copied verbatim: skip and
         * carry on.  It must NOT be a GATE_FAIL — a producer that reported
         * the skip would be a producer that noticed, and the point of this
         * arm is that the fork does not.
         *
         * It still PRINTS `bundled_v2_under` with the writer's real v2 id,
         * for two reasons.  Plumbing: `verify_bundle` needs the key, and an
         * arm that went red because a stdout line was missing would have been
         * caught by the harness rather than by the container.  Substance:
         * announcing the id it would have used is exactly what a host in this
         * state believes — it reports a bundle it never performed, which is
         * the silent-self-pass shape in its purest form.  The claim is on
         * stdout and the evidence is absent from the container, and only the
         * verifier reading the container can tell the two apart. */
        if (bundle_id == CT_TW_INVALID_PATH_ID) {
            printf("bundled_v2_under=%llu\n", (unsigned long long)v2);
            trace_writer_register_step(w, PROBE, 45);
            goto done;
        }
#else
        if (bundle_id == CT_TW_INVALID_PATH_ID)
            GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                      "no path id for %s after the reload", PROBE);
#endif
        if (bundle(w, bundle_id, PROBE, V2_TEXT) < 0)
            GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id",
                      "register_source_view(v2) failed");
        printf("bundled_v2_under=%llu\n", (unsigned long long)bundle_id);

        /* A step on a line that exists ONLY in v2.  Under a single path entry
         * this is refused (or mis-addressed); under two it is ordinary. */
        trace_writer_register_step(w, PROBE, 45);
    }

#ifdef GDH3_FALSIFY_SILENT_SKIP_RELOAD
done:
#endif
    trace_writer_finish_events(w);
    trace_writer_finish_metadata(w);
    trace_writer_finish_paths(w);
    if (trace_writer_close(w) != 0)
        GATE_FAIL("gdh3_fork_uses_the_writers_own_path_id", "close failed");
    trace_writer_free(w);
    printf("container=%s/gdh3_probe.ct\n", out_dir);
    return 0;
}

/* -------------------------------------------------------------------------
 * Gate 2 — a refused registration reaches the C caller.
 * ------------------------------------------------------------------------- */
static int refusal(const char *out_dir) {
    const char *G = "gdh3_refused_registration_reaches_the_c_caller";
    codetracer_trace_writer_init();
    trace_writer_t w = trace_writer_new("gdh3_refusal", FFI_TRACE_FORMAT_BINARY);
    if (!w) GATE_FAIL(G, "trace_writer_new returned NULL");

    char events[1024];
    snprintf(events, sizeof(events), "%s/refusal-events.bin", out_dir);
    trace_writer_set_workdir(w, out_dir);
    trace_writer_begin_metadata(w, "");
    if (trace_writer_begin_events(w, events) != 0)
        GATE_FAIL(G, "begin_events failed");
    trace_writer_begin_paths(w, "");
    if (trace_writer_enable_line_count_table(w) != 0)
        GATE_FAIL(G, "enable_line_count_table failed");
    if (trace_writer_register_path_with_line_count(w, PROBE, 40) != 0)
        GATE_FAIL(G, "register_path_with_line_count failed");

    /* CONTROL ARM FIRST: a VALID registration must return a usable id and
     * leave last_error EMPTY.  It runs first so that "the refusal set an
     * error" is measured against a call that did not, rather than against an
     * assumption. */
    trace_writer_clear_last_error();
    uint64_t good = trace_writer_register_path_version(w, PROBE, 63);
    if (good == CT_TW_INVALID_PATH_ID)
        GATE_FAIL(G, "CONTROL ARM: a valid registration was refused");
    if (strlen(trace_writer_last_error()) != 0)
        GATE_FAIL(G, "CONTROL ARM: a valid registration left last_error "
                     "non-empty (`%s`), so a non-empty buffer cannot be read "
                     "as `this call failed`", trace_writer_last_error());
    printf("control_valid_id=%llu\n", (unsigned long long)good);

    /* ANTI-VACUITY: clear the buffer and PROVE it is empty before the call.
     * Asserting only `non-empty afterwards` would pass on a message an
     * earlier call left behind — trap 5, a sentinel that collides with a
     * legitimate value. */
    trace_writer_clear_last_error();
    if (strlen(trace_writer_last_error()) != 0)
        GATE_FAIL(G, "last_error is not empty after clear_last_error, so any "
                     "message seen after the next call cannot be attributed "
                     "to it");

    uint64_t bad = trace_writer_register_path_version(w, PROBE, 0);

    /* last_error is checked BEFORE the return value, and the order is
     * deliberate.  The milestone's falsifier for this gate is "make the
     * entry point discard the writer's Result and return a plausible id",
     * and it says the gate must go red ON LAST_ERROR BEING EMPTY.  A gate
     * that checked the id first would go red on the id and never exercise
     * the assertion it was written for. */
    const char *err = trace_writer_last_error();
    if (strlen(err) == 0)
        GATE_FAIL(G, "the refusal set NO last_error. This is the defect the "
                     "writer's own history records for three other void entry "
                     "points: a C caller's steps went missing with nothing in "
                     "last_error to say why. It returned %llu",
                  (unsigned long long)bad);
    if (strstr(err, PROBE) == NULL)
        GATE_FAIL(G, "the refusal does not name the path (`%s`); got: %s",
                  PROBE, err);
    if (bad != CT_TW_INVALID_PATH_ID)
        GATE_FAIL(G, "a zero line count returned a plausible id (%llu) "
                     "instead of CT_TW_INVALID_PATH_ID. A file sized 0 shares "
                     "its base with the next file, so the caller would go on "
                     "to attach source views and steps to a version the space "
                     "cannot address", (unsigned long long)bad);
    printf("refusal_error=%s\n", err);

    /* And a second refusal shape: a path this writer has never seen must NOT
     * be answered with a freshly minted id.  A query that registered what it
     * was asked about would put a file in paths.dat the recording never
     * executed. */
    trace_writer_clear_last_error();
    uint64_t unknown = trace_writer_current_path_id(w, "res://gdh3/never.gd");
    if (unknown != CT_TW_INVALID_PATH_ID)
        GATE_FAIL(G, "current_path_id answered %llu for a path that was never "
                     "registered", (unsigned long long)unknown);
    if (strlen(trace_writer_last_error()) == 0)
        GATE_FAIL(G, "current_path_id refused an unknown path silently");

    trace_writer_finish_events(w);
    trace_writer_finish_metadata(w);
    trace_writer_finish_paths(w);
    trace_writer_close(w);
    trace_writer_free(w);
    printf("PASS: gdh3_refused_registration_reaches_the_c_caller\n");
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
            "usage: %s <bundle|bundle-single|refusal> <out_dir>\n", argv[0]);
        return 2;
    }
    if (strcmp(argv[1], "bundle") == 0)        return produce(argv[2], 1);
    if (strcmp(argv[1], "bundle-single") == 0) return produce(argv[2], 0);
    if (strcmp(argv[1], "refusal") == 0)       return refusal(argv[2]);
    fprintf(stderr, "unknown mode `%s`; a selector that matches nothing turns "
                    "every arm green\n", argv[1]);
    return 2;
}
