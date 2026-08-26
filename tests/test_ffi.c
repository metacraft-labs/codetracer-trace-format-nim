#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "codetracer_trace_writer.h"

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s\n  at %s:%d\n  last_error: %s\n", \
                msg, __FILE__, __LINE__, trace_writer_last_error()); \
        return 1; \
    } \
} while(0)

static int file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

int main(void) {
    /* Initialize Nim runtime */
    codetracer_trace_writer_init();

    printf("=== C FFI Test for codetracer_trace_writer ===\n\n");

    /* Create a trace writer */
    trace_writer_t writer = trace_writer_new("test_program", FFI_TRACE_FORMAT_BINARY);
    ASSERT(writer != NULL, "trace_writer_new should return non-NULL");
    printf("[OK] trace_writer_new\n");

    /* Set workdir */
    trace_writer_set_workdir(writer, "/tmp/test_workdir");
    printf("[OK] trace_writer_set_workdir\n");

    /* Begin metadata/events/paths (compatibility stubs) */
    ASSERT(trace_writer_begin_metadata(writer, "/tmp/meta.json") == 0,
        "begin_metadata should succeed");
    ASSERT(trace_writer_begin_events(writer, "/tmp/events.bin") == 0,
        "begin_events should succeed");
    ASSERT(trace_writer_begin_paths(writer, "/tmp/paths.json") == 0,
        "begin_paths should succeed");
    printf("[OK] begin_metadata/events/paths\n");

    /* Start tracing */
    trace_writer_start(writer, "/test/main.py", 1);
    printf("[OK] trace_writer_start\n");

    /* Register a step */
    trace_writer_register_step(writer, "/test/main.py", 5);
    printf("[OK] trace_writer_register_step\n");

    /* Register a function */
    size_t fn_id = trace_writer_ensure_function_id(
        writer, "hello", "/test/main.py", 10);
    ASSERT(fn_id != (size_t)-1, "ensure_function_id should return valid ID");
    printf("[OK] trace_writer_ensure_function_id (id=%zu)\n", fn_id);

    /* Ensure same function returns same ID */
    size_t fn_id2 = trace_writer_ensure_function_id(
        writer, "hello", "/test/main.py", 10);
    ASSERT(fn_id2 == fn_id, "same function should return same ID");
    printf("[OK] ensure_function_id deduplication\n");

    /* Register a type */
    size_t type_id = trace_writer_ensure_type_id(writer, FFI_TYPE_INT, "int");
    ASSERT(type_id != (size_t)-1, "ensure_type_id should return valid ID");
    printf("[OK] trace_writer_ensure_type_id (id=%zu)\n", type_id);

    /* Register a call */
    trace_writer_register_call(writer, fn_id);
    printf("[OK] trace_writer_register_call\n");

    /* Register a variable with integer value */
    trace_writer_register_variable_int(writer, "x", 42, FFI_TYPE_INT, "int");
    printf("[OK] trace_writer_register_variable_int\n");

    /* Register a variable with raw value */
    trace_writer_register_variable_raw(
        writer, "msg", "\"hello world\"", FFI_TYPE_STRING, "str");
    printf("[OK] trace_writer_register_variable_raw\n");

    /* Register a return with integer */
    trace_writer_register_return_int(writer, 0, FFI_TYPE_INT, "int");
    printf("[OK] trace_writer_register_return_int\n");

    /* Register a special event */
    trace_writer_register_special_event(
        writer, FFI_EVENT_WRITE_FILE, "stdout", "Hello, World!\n");
    printf("[OK] trace_writer_register_special_event\n");

    /* Finish events/metadata/paths */
    ASSERT(trace_writer_finish_events(writer) == 0, "finish_events should succeed");
    ASSERT(trace_writer_finish_metadata(writer) == 0, "finish_metadata should succeed");
    ASSERT(trace_writer_finish_paths(writer) == 0, "finish_paths should succeed");
    printf("[OK] finish_events/metadata/paths\n");

    /* Close the writer */
    ASSERT(trace_writer_close(writer) == 0, "close should succeed");
    printf("[OK] trace_writer_close\n");

    /* Verify the .ct file exists (it's created in the same dir as events path) */
    ASSERT(file_exists("/tmp/test_program.ct"), "test_program.ct should exist in /tmp");
    printf("[OK] /tmp/test_program.ct file exists\n");

    /* Free the writer */
    trace_writer_free(writer);
    printf("[OK] trace_writer_free\n");

    /* Test NULL handle safety */
    trace_writer_free(NULL);
    ASSERT(trace_writer_close(NULL) != 0, "close(NULL) should return non-zero (error)");
    trace_writer_register_step(NULL, "/foo", 1);
    printf("[OK] NULL handle safety\n");

    /* Clean up */
    remove("/tmp/test_program.ct");

    printf("\n=== All tests passed! ===\n\n");

    /* ================================================================
     * meta.dat roundtrip test: write to buffer, read back, verify
     * ================================================================ */
    printf("=== C FFI meta.dat roundtrip test ===\n\n");

    {
        const char* prog = "my_program";
        const char* wd = "/home/user/project";
        const char* arg_strs[] = { "--verbose", "-o", "out.txt" };
        const uint8_t* arg_ptrs[] = {
            (const uint8_t*)arg_strs[0],
            (const uint8_t*)arg_strs[1],
            (const uint8_t*)arg_strs[2]
        };
        size_t arg_lens[] = { strlen(arg_strs[0]), strlen(arg_strs[1]), strlen(arg_strs[2]) };

        const char* path_strs[] = { "/src/main.c", "/src/util.c" };
        const uint8_t* path_ptrs[] = {
            (const uint8_t*)path_strs[0],
            (const uint8_t*)path_strs[1]
        };
        size_t path_lens[] = { strlen(path_strs[0]), strlen(path_strs[1]) };

        const char* rec_id = "test-recorder-v1";
        /* M-REC-1: canonical UUIDv7.  Hard-coded so the test
         * exercises caller-supplied id rather than the mint path. */
        const char* recording_id = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";

        uint8_t* buf = NULL;
        size_t buf_len = 0;

        int rc = ct_write_meta_dat_to_buffer(
            (const uint8_t*)prog, strlen(prog),
            (const uint8_t*)wd, strlen(wd),
            arg_ptrs, arg_lens, 3,
            path_ptrs, path_lens, 2,
            (const uint8_t*)rec_id, strlen(rec_id),
            (const uint8_t*)recording_id, strlen(recording_id),
            &buf, &buf_len);
        ASSERT(rc == 0, "ct_write_meta_dat_to_buffer should succeed");
        ASSERT(buf != NULL, "output buffer should be non-NULL");
        ASSERT(buf_len > 8, "output buffer should have at least header bytes");
        printf("[OK] ct_write_meta_dat_to_buffer (len=%zu)\n", buf_len);

        /* Check magic bytes */
        ASSERT(buf[0] == 'C' && buf[1] == 'T' && buf[2] == 'M' && buf[3] == 'D',
            "magic bytes should be CTMD");
        printf("[OK] magic bytes\n");

        /* Read back */
        meta_dat_reader_t reader = ct_read_meta_dat(buf, buf_len);
        ASSERT(reader != NULL, "ct_read_meta_dat should return non-NULL");
        printf("[OK] ct_read_meta_dat\n");

        /* Verify program */
        size_t len;
        const uint8_t* p = ct_meta_dat_program(reader, &len);
        ASSERT(p != NULL && len == strlen(prog), "program length mismatch");
        ASSERT(memcmp(p, prog, len) == 0, "program content mismatch");
        printf("[OK] program = \"%.*s\"\n", (int)len, (const char*)p);

        /* Verify workdir */
        p = ct_meta_dat_workdir(reader, &len);
        ASSERT(p != NULL && len == strlen(wd), "workdir length mismatch");
        ASSERT(memcmp(p, wd, len) == 0, "workdir content mismatch");
        printf("[OK] workdir = \"%.*s\"\n", (int)len, (const char*)p);

        /* Verify args */
        ASSERT(ct_meta_dat_args_count(reader) == 3, "args count should be 3");
        for (size_t i = 0; i < 3; i++) {
            p = ct_meta_dat_arg(reader, i, &len);
            ASSERT(p != NULL && len == strlen(arg_strs[i]), "arg length mismatch");
            ASSERT(memcmp(p, arg_strs[i], len) == 0, "arg content mismatch");
            printf("[OK] arg[%zu] = \"%.*s\"\n", i, (int)len, (const char*)p);
        }

        /* Verify paths */
        ASSERT(ct_meta_dat_paths_count(reader) == 2, "paths count should be 2");
        for (size_t i = 0; i < 2; i++) {
            p = ct_meta_dat_path(reader, i, &len);
            ASSERT(p != NULL && len == strlen(path_strs[i]), "path length mismatch");
            ASSERT(memcmp(p, path_strs[i], len) == 0, "path content mismatch");
            printf("[OK] path[%zu] = \"%.*s\"\n", i, (int)len, (const char*)p);
        }

        /* Verify recorder_id */
        p = ct_meta_dat_recorder_id(reader, &len);
        ASSERT(p != NULL && len == strlen(rec_id), "recorder_id length mismatch");
        ASSERT(memcmp(p, rec_id, len) == 0, "recorder_id content mismatch");
        printf("[OK] recorder_id = \"%.*s\"\n", (int)len, (const char*)p);

        /* Out of bounds arg/path should return NULL */
        ASSERT(ct_meta_dat_arg(reader, 99, &len) == NULL, "out-of-bounds arg should be NULL");
        ASSERT(ct_meta_dat_path(reader, 99, &len) == NULL, "out-of-bounds path should be NULL");
        printf("[OK] out-of-bounds safety\n");

        /* Free reader and buffer */
        ct_meta_dat_free(reader);
        ct_free_buffer(buf);
        printf("[OK] ct_meta_dat_free + ct_free_buffer\n");

        /* NULL safety */
        ct_meta_dat_free(NULL);
        ct_free_buffer(NULL);
        ASSERT(ct_read_meta_dat(NULL, 0) == NULL, "read NULL data should return NULL");
        printf("[OK] NULL safety\n");
    }

    /* Test ct_write_meta_dat via trace writer handle */
    {
        trace_writer_t w = trace_writer_new("meta_test_prog", FFI_TRACE_FORMAT_BINARY);
        ASSERT(w != NULL, "trace_writer_new for meta_dat test");

        trace_writer_set_workdir(w, "/tmp/meta_workdir");
        ASSERT(trace_writer_begin_events(w, "/tmp/events.bin") == 0,
            "begin_events for meta_dat test");

        trace_writer_start(w, "/test/main.py", 1);
        trace_writer_register_step(w, "/test/helper.py", 10);

        const char* rec = "ffi-recorder";
        int rc = ct_write_meta_dat(w, (const uint8_t*)rec, strlen(rec));
        ASSERT(rc == 0, "ct_write_meta_dat should succeed");
        printf("[OK] ct_write_meta_dat via trace writer handle\n");

        ASSERT(trace_writer_close(w) == 0, "close for meta_dat test");
        trace_writer_free(w);

        /* Verify the .ct file was created (in /tmp, same dir as events path) */
        ASSERT(file_exists("/tmp/meta_test_prog.ct"), "meta_test_prog.ct should exist in /tmp");
        printf("[OK] /tmp/meta_test_prog.ct exists with meta.dat\n");
        remove("/tmp/meta_test_prog.ct");
    }

    printf("\n=== All meta.dat tests passed! ===\n\n");

    /* ================================================================
     * Old single-stream format test (BINARY_V0)
     * Ensures backward compat: useMultiStream=false
     * ================================================================ */
    printf("=== Old format (BINARY_V0) backward-compat test ===\n\n");
    {
        trace_writer_t w = trace_writer_new("old_format_test", FFI_TRACE_FORMAT_BINARY_V0);
        ASSERT(w != NULL, "trace_writer_new (old format)");

        trace_writer_set_workdir(w, "/tmp/old_workdir");
        ASSERT(trace_writer_begin_events(w, "/tmp/old_events.bin") == 0,
            "begin_events (old format)");

        trace_writer_start(w, "/test/old_main.py", 1);
        trace_writer_register_step(w, "/test/old_main.py", 5);

        size_t fn = trace_writer_ensure_function_id(w, "old_func", "/test/old_main.py", 10);
        ASSERT(fn != (size_t)-1, "ensure_function_id (old format)");

        size_t tid = trace_writer_ensure_type_id(w, FFI_TYPE_INT, "int");
        ASSERT(tid != (size_t)-1, "ensure_type_id (old format)");

        trace_writer_register_call(w, fn);
        trace_writer_register_variable_int(w, "x", 99, FFI_TYPE_INT, "int");
        trace_writer_register_return_int(w, 0, FFI_TYPE_INT, "int");

        ASSERT(trace_writer_close(w) == 0, "close (old format)");
        ASSERT(file_exists("/tmp/old_format_test.ct"), "old_format_test.ct should exist");
        printf("[OK] old format roundtrip\n");

        trace_writer_free(w);
        remove("/tmp/old_format_test.ct");
    }
    printf("\n=== Old format test passed! ===\n\n");

    /* ================================================================
     * Multi-stream format test (BINARY = 2)
     * Verifies the new multi-stream writer produces a valid .ct file
     * ================================================================ */
    printf("=== Multi-stream format test ===\n\n");
    {
        trace_writer_t w = trace_writer_new("ms_test", FFI_TRACE_FORMAT_BINARY);
        ASSERT(w != NULL, "trace_writer_new (multi-stream)");

        trace_writer_set_workdir(w, "/tmp/ms_workdir");
        ASSERT(trace_writer_begin_events(w, "/tmp/ms_events.bin") == 0,
            "begin_events (multi-stream)");

        /* Start tracing */
        trace_writer_start(w, "/test/ms_main.py", 1);
        printf("[OK] ms: start\n");

        /* Register steps with variables */
        trace_writer_register_step(w, "/test/ms_main.py", 5);
        trace_writer_register_variable_int(w, "counter", 42, FFI_TYPE_INT, "int");
        trace_writer_register_variable_raw(w, "name", "\"alice\"", FFI_TYPE_STRING, "str");
        printf("[OK] ms: step + variables\n");

        /* Register another step */
        trace_writer_register_step(w, "/test/ms_helper.py", 10);
        trace_writer_register_variable_int(w, "result", 100, FFI_TYPE_INT, "int");
        printf("[OK] ms: second step + variable\n");

        /* Function call/return */
        size_t fn = trace_writer_ensure_function_id(w, "compute", "/test/ms_helper.py", 8);
        ASSERT(fn != (size_t)-1, "ensure_function_id (multi-stream)");
        trace_writer_register_call(w, fn);
        trace_writer_register_return_int(w, 42, FFI_TYPE_INT, "int");
        printf("[OK] ms: call/return\n");

        /* IO event */
        trace_writer_register_special_event(w, FFI_EVENT_WRITE, "stdout", "Hello!\n");
        printf("[OK] ms: special event\n");

        /* RS-M1 spans: an in-flight (open) record, its completion for the
         * SAME span_id (last-record-wins), and an external-bound span. */
        {
            const char* keys[] = { "http.method", "http.url",
                                   "http.status_code", "http.duration_ms" };
            const char* vals[] = { "GET", "/api/users/42", "200", "12" };

            /* open record: no end fields, status unknown */
            ASSERT(trace_writer_register_span(w,
                       1, 0, SPAN_FLAG_OPEN, SPAN_STATUS_UNKNOWN,
                       1700000000000000000ULL, 0,
                       0, 7, 100, 0,
                       NULL, NULL, "web-request", "GET /api/users/42",
                       SPAN_STRUCTURAL_CONTIGUOUS |
                           SPAN_STRUCTURAL_SHARES_TIMELINE,
                       keys, vals, 2) == 0,
                   "register_span (open)");

            /* a live recorder can publish it before closing */
            ASSERT(trace_writer_flush_spans(w) == 0, "flush_spans");

            /* completion record for the same span_id */
            ASSERT(trace_writer_register_span(w,
                       1, 0, 0, SPAN_STATUS_OK,
                       1700000000000000000ULL, 1700000000012000000ULL,
                       0, 7, 100, 350,
                       NULL, NULL, "web-request", "GET /api/users/42",
                       SPAN_STRUCTURAL_CONTIGUOUS |
                           SPAN_STRUCTURAL_SHARES_TIMELINE,
                       keys, vals, 4) == 0,
                   "register_span (completion)");

            /* external-bound span: execution lives in another container */
            ASSERT(trace_writer_register_span(w,
                       2, 0, SPAN_FLAG_EXTERNAL, SPAN_STATUS_ERROR,
                       1700000000000000000ULL, 1700000000500000000ULL,
                       0, 0, 0, 0,
                       "01949fcc-7d92-7e9c-cccc-dddddddddddd",
                       "requests/req-0002.ct",
                       "web-request", "POST /api/orders",
                       SPAN_STRUCTURAL_SHARES_TIMELINE,
                       keys, vals, 4) == 0,
                   "register_span (external)");

            /* an open record carrying end fields must be REJECTED, not
             * silently normalised — the fail-closed contract at the ABI. */
            ASSERT(trace_writer_register_span(w,
                       3, 0, SPAN_FLAG_OPEN, SPAN_STATUS_UNKNOWN,
                       1700000000000000000ULL, 1700000000000000001ULL,
                       0, 0, 0, 99,
                       NULL, NULL, "web-request", "bad",
                       0, NULL, NULL, 0) != 0,
                   "register_span must reject an open record with end fields");

            printf("[OK] ms: spans (open + completion + external + reject)\n");
        }

        /* Close — this flushes pending step and writes .ct file */
        ASSERT(trace_writer_close(w) == 0, "close (multi-stream)");
        ASSERT(file_exists("/tmp/ms_test.ct"), "ms_test.ct should exist");
        printf("[OK] ms: close + file exists\n");

        /* Verify file is non-empty */
        {
            struct stat st;
            ASSERT(stat("/tmp/ms_test.ct", &st) == 0, "stat ms_test.ct");
            ASSERT(st.st_size > 100, "ms_test.ct should be non-trivial size");
            printf("[OK] ms: file size = %ld bytes\n", (long)st.st_size);
        }

        trace_writer_free(w);
        remove("/tmp/ms_test.ct");
    }
    printf("\n=== Multi-stream format test passed! ===\n");

    /* ================================================================
     * MT1/MT2: replay-observation chokepoints on a STREAMING writer
     * The three chokepoints (register_step / register_call /
     * register_return) that MCR observes at replay append to in-memory
     * chunk buffers and perform NO per-call disk write. Under the streaming
     * writer (createCtfsStreaming, the FFI default), the .ct container is
     * opened at init and written incrementally as chunks seal DURING
     * recording -- NOT buffered in RAM and dumped whole at close (the old
     * blockchain-suited mode). The distinguishing, robust property: the
     * container file exists on disk while recording is still in progress.
     * We prove it: begin recording, assert the container is already present
     * on disk, drive all three chokepoints 2000x, then finalize at close.
     * No mock: real FFI writer, real /tmp filesystem.
     * ================================================================ */
    printf("=== MT1/MT2 chokepoint streaming property ===\n\n");
    {
        remove("/tmp/cp_buffer_test.ct");
        trace_writer_t w = trace_writer_new("cp_buffer_test", FFI_TRACE_FORMAT_BINARY);
        ASSERT(w != NULL, "cp: trace_writer_new");
        trace_writer_set_workdir(w, "/tmp/cp_workdir");
        ASSERT(trace_writer_begin_events(w, "/tmp/cp_events.bin") == 0, "cp: begin_events");
        trace_writer_start(w, "/test/cp.py", 1);

        /* Streaming writer opens the container at init: it is on disk NOW,
         * mid-recording, which the old buffer-and-dump mode never was until
         * close. This is the property MCR's shared-writer attach needs. */
        ASSERT(file_exists("/tmp/cp_buffer_test.ct"),
            "cp: streaming container exists on disk DURING recording (not buffered to close)");
        printf("[OK] cp: streaming container present on disk mid-recording\n");

        size_t fn = trace_writer_ensure_function_id(w, "cp_func", "/test/cp.py", 1);
        ASSERT(fn != (size_t)-1, "cp: ensure_function_id");

        for (int i = 0; i < 2000; i++) {
            trace_writer_register_step(w, "/test/cp.py", 2 + (i % 8));
            trace_writer_register_call(w, fn);
            trace_writer_register_return(w);
        }

        ASSERT(trace_writer_close(w) == 0, "cp: close");
        {
            struct stat st;
            ASSERT(stat("/tmp/cp_buffer_test.ct", &st) == 0 && st.st_size > 0,
                "cp: streaming container finalized at close");
            printf("[OK] cp: streaming container finalized at close (%ld bytes)\n", (long)st.st_size);
        }
        trace_writer_free(w);
        remove("/tmp/cp_buffer_test.ct");
    }
    printf("\n=== MT1/MT2 chokepoint streaming test passed! ===\n");

    /* ----------------------------------------------------------------
     * CTFS container — post-hoc internal files (M38b).
     *
     * The ABI half of `ct_container_create` / `ct_container_append_files`:
     * that the symbols are exported, that the three parallel arrays are
     * read the way the header says, and that a refusal comes back as a
     * non-zero return with a message rather than as a silent success.
     * The *layout* these produce is adjudicated elsewhere, by readers that
     * share no code with the writer — `tests/test_container_append.nim`
     * and the wasm recorder's `internal/ctfs/ffi_crossread_test.go`.
     * ---------------------------------------------------------------- */
    {
        const char* path = "/tmp/ct_container_append_test.ct";
        remove(path);
        ASSERT(ct_container_create(path, 0) == 0, "ct_container_create");
        ASSERT(file_exists(path), "the container file should exist");
        printf("[OK] container: create\n");

        {
            static unsigned char body_a[70000];
            static const unsigned char body_b[3] = { 7, 8, 9 };
            const char* names[3] = { "meta.dat", "steps.dat", "snaptab.dat" };
            const unsigned char* contents[3] = { body_a, body_b, NULL };
            size_t lengths[3] = { sizeof(body_a), sizeof(body_b), 0 };
            size_t i;
            struct stat st;
            for (i = 0; i < sizeof(body_a); i++) {
                body_a[i] = (unsigned char)((i * 7 + i / 4096) % 251);
            }
            ASSERT(ct_container_append_files(path, names, contents, lengths, 3) == 0,
                   "ct_container_append_files");
            ASSERT(stat(path, &st) == 0, "stat the appended container");
            ASSERT(st.st_size % 4096 == 0,
                   "an appended container must stay a whole number of blocks");
            ASSERT((size_t)st.st_size > sizeof(body_a),
                   "the container should have grown by at least the payload");
            printf("[OK] container: append (%ld bytes)\n", (long)st.st_size);

            /* Append-only: a name that already exists must be refused, and
             * the refusal must carry a message. */
            {
                const char* dup_names[1] = { "meta.dat" };
                const unsigned char* dup_contents[1] = { body_b };
                size_t dup_lengths[1] = { sizeof(body_b) };
                ASSERT(ct_container_append_files(path, dup_names, dup_contents,
                                                 dup_lengths, 1) != 0,
                       "appending an existing name must fail");
                ASSERT(trace_writer_last_error()[0] != '\0',
                       "a refused append must set an error message");
                printf("[OK] container: append refuses an existing name\n");
            }

            /* A name outside the base40 alphabet must be refused rather than
             * silently truncated to its encodable prefix. */
            {
                const char* bad_names[1] = { "snap!pages" };
                const unsigned char* bad_contents[1] = { body_b };
                size_t bad_lengths[1] = { sizeof(body_b) };
                ASSERT(ct_container_append_files(path, bad_names, bad_contents,
                                                 bad_lengths, 1) != 0,
                       "appending an unencodable name must fail");
                printf("[OK] container: append refuses an unencodable name\n");
            }

            /* A container that is not a container. */
            ASSERT(ct_container_append_files("/tmp/ct_container_append_test.missing",
                                             names, contents, lengths, 1) != 0,
                   "appending to a nonexistent container must fail");
            printf("[OK] container: append refuses a nonexistent container\n");
        }
        remove(path);
    }
    printf("\n=== CTFS container append test passed! ===\n");

    return 0;
}
