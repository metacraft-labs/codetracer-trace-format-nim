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
    ASSERT(trace_writer_begin_metadata(writer, "/tmp/meta.json"),
        "begin_metadata should succeed");
    ASSERT(trace_writer_begin_events(writer, "/tmp/events.bin"),
        "begin_events should succeed");
    ASSERT(trace_writer_begin_paths(writer, "/tmp/paths.json"),
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
    ASSERT(trace_writer_finish_events(writer), "finish_events should succeed");
    ASSERT(trace_writer_finish_metadata(writer), "finish_metadata should succeed");
    ASSERT(trace_writer_finish_paths(writer), "finish_paths should succeed");
    printf("[OK] finish_events/metadata/paths\n");

    /* Close the writer */
    ASSERT(trace_writer_close(writer), "close should succeed");
    printf("[OK] trace_writer_close\n");

    /* Verify the .ct file exists */
    ASSERT(file_exists("test_program.ct"), "test_program.ct should exist");
    printf("[OK] test_program.ct file exists\n");

    /* Free the writer */
    trace_writer_free(writer);
    printf("[OK] trace_writer_free\n");

    /* Test NULL handle safety */
    trace_writer_free(NULL);
    ASSERT(!trace_writer_close(NULL), "close(NULL) should return false");
    trace_writer_register_step(NULL, "/foo", 1);
    printf("[OK] NULL handle safety\n");

    /* Clean up */
    remove("test_program.ct");

    printf("\n=== All tests passed! ===\n");
    return 0;
}
