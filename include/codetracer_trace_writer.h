#ifndef CODETRACER_TRACE_WRITER_H
#define CODETRACER_TRACE_WRITER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle to the trace writer */
typedef void* trace_writer_t;

/* --------------------------------------------------------------------------
 * FFI enums (must match Rust codetracer_trace_writer_ffi repr(C) values)
 * -------------------------------------------------------------------------- */

enum FfiTraceFormat {
    FFI_TRACE_FORMAT_JSON = 0,
    FFI_TRACE_FORMAT_BINARY_V0 = 1,
    FFI_TRACE_FORMAT_BINARY = 2
};

enum FfiTypeKind {
    FFI_TYPE_SEQ = 0,
    FFI_TYPE_SET = 1,
    FFI_TYPE_HASH_SET = 2,
    FFI_TYPE_ORDERED_SET = 3,
    FFI_TYPE_ARRAY = 4,
    FFI_TYPE_VARARGS = 5,
    FFI_TYPE_STRUCT = 6,
    FFI_TYPE_INT = 7,
    FFI_TYPE_FLOAT = 8,
    FFI_TYPE_STRING = 9,
    FFI_TYPE_CSTRING = 10,
    FFI_TYPE_CHAR = 11,
    FFI_TYPE_BOOL = 12,
    FFI_TYPE_LITERAL = 13,
    FFI_TYPE_REF = 14,
    FFI_TYPE_RECURSION = 15,
    FFI_TYPE_RAW = 16,
    FFI_TYPE_ENUM = 17,
    FFI_TYPE_ENUM16 = 18,
    FFI_TYPE_ENUM32 = 19,
    FFI_TYPE_C = 20,
    FFI_TYPE_TABLE_KIND = 21,
    FFI_TYPE_UNION = 22,
    FFI_TYPE_POINTER = 23,
    FFI_TYPE_ERROR = 24,
    FFI_TYPE_FUNCTION_KIND = 25,
    FFI_TYPE_TYPE_VALUE = 26,
    FFI_TYPE_TUPLE = 27,
    FFI_TYPE_VARIANT = 28,
    FFI_TYPE_HTML = 29,
    FFI_TYPE_NONE = 30,
    FFI_TYPE_NON_EXPANDED = 31,
    FFI_TYPE_ANY = 32,
    FFI_TYPE_SLICE = 33
};

enum FfiEventLogKind {
    FFI_EVENT_WRITE = 0,
    FFI_EVENT_WRITE_FILE = 1,
    FFI_EVENT_WRITE_OTHER = 2,
    FFI_EVENT_READ = 3,
    FFI_EVENT_READ_FILE = 4,
    FFI_EVENT_READ_OTHER = 5,
    FFI_EVENT_READ_DIR = 6,
    FFI_EVENT_OPEN_DIR = 7,
    FFI_EVENT_CLOSE_DIR = 8,
    FFI_EVENT_SOCKET = 9,
    FFI_EVENT_OPEN = 10,
    FFI_EVENT_ERROR = 11,
    FFI_EVENT_TRACE_LOG_EVENT = 12,
    FFI_EVENT_EVM_EVENT = 13
};

/* --------------------------------------------------------------------------
 * Initialization (call once before using any other function)
 * -------------------------------------------------------------------------- */

void codetracer_trace_writer_init(void);

/* --------------------------------------------------------------------------
 * Error handling
 * -------------------------------------------------------------------------- */

const char* trace_writer_last_error(void);

/* --------------------------------------------------------------------------
 * Lifecycle
 * -------------------------------------------------------------------------- */

trace_writer_t trace_writer_new(const char* program, int format);
void trace_writer_free(trace_writer_t handle);
int trace_writer_close(trace_writer_t handle);

/* --------------------------------------------------------------------------
 * File I/O — begin / finish (compatibility with Rust API)
 * -------------------------------------------------------------------------- */

int trace_writer_begin_metadata(trace_writer_t handle, const char* path);
int trace_writer_finish_metadata(trace_writer_t handle);
int trace_writer_begin_events(trace_writer_t handle, const char* path);
int trace_writer_finish_events(trace_writer_t handle);
int trace_writer_begin_paths(trace_writer_t handle, const char* path);
int trace_writer_finish_paths(trace_writer_t handle);

/* --------------------------------------------------------------------------
 * Tracing primitives
 * -------------------------------------------------------------------------- */

void trace_writer_start(trace_writer_t handle, const char* path, int64_t line);
void trace_writer_set_workdir(trace_writer_t handle, const char* workdir);
void trace_writer_register_step(trace_writer_t handle,
                                const char* path, int64_t line);

size_t trace_writer_ensure_function_id(trace_writer_t handle,
    const char* name, const char* path, int64_t line);

size_t trace_writer_ensure_type_id(trace_writer_t handle,
    int kind, const char* lang_type);

void trace_writer_register_call(trace_writer_t handle, size_t function_id);
void trace_writer_register_return(trace_writer_t handle);

void trace_writer_register_return_int(trace_writer_t handle,
                                      int64_t value,
                                      int type_kind,
                                      const char* type_name);

void trace_writer_register_return_raw(trace_writer_t handle,
                                      const char* value_repr,
                                      int type_kind,
                                      const char* type_name);

void trace_writer_register_variable_int(trace_writer_t handle,
                                        const char* name,
                                        int64_t value,
                                        int type_kind,
                                        const char* type_name);

void trace_writer_register_variable_raw(trace_writer_t handle,
                                        const char* name,
                                        const char* value_repr,
                                        int type_kind,
                                        const char* type_name);

void trace_writer_register_variable_cbor(trace_writer_t handle,
    const char* name,
    const uint8_t* cbor_data,
    size_t cbor_len);

void trace_writer_register_return_cbor(trace_writer_t handle,
    const uint8_t* cbor_data,
    size_t cbor_len);

void trace_writer_register_special_event(trace_writer_t handle,
    int kind, const char* metadata, const char* content);

/* --------------------------------------------------------------------------
 * meta.dat — write via trace writer handle
 * -------------------------------------------------------------------------- */

int ct_write_meta_dat(trace_writer_t handle,
                      const uint8_t* recorder_id, size_t recorder_id_len);

/* --------------------------------------------------------------------------
 * meta.dat — standalone buffer write
 * -------------------------------------------------------------------------- */

int ct_write_meta_dat_to_buffer(
    const uint8_t* program, size_t program_len,
    const uint8_t* workdir, size_t workdir_len,
    const uint8_t* const* args, const size_t* arg_lens, size_t args_count,
    const uint8_t* const* paths, const size_t* path_lens, size_t paths_count,
    const uint8_t* recorder_id, size_t recorder_id_len,
    uint8_t** out_buf, size_t* out_len);

void ct_free_buffer(uint8_t* buf);

/* --------------------------------------------------------------------------
 * meta.dat — reader handle
 * -------------------------------------------------------------------------- */

typedef void* meta_dat_reader_t;

meta_dat_reader_t ct_read_meta_dat(const uint8_t* data, size_t len);
const uint8_t* ct_meta_dat_program(meta_dat_reader_t h, size_t* out_len);
const uint8_t* ct_meta_dat_workdir(meta_dat_reader_t h, size_t* out_len);
size_t ct_meta_dat_args_count(meta_dat_reader_t h);
const uint8_t* ct_meta_dat_arg(meta_dat_reader_t h, size_t idx, size_t* out_len);
size_t ct_meta_dat_paths_count(meta_dat_reader_t h);
const uint8_t* ct_meta_dat_path(meta_dat_reader_t h, size_t idx, size_t* out_len);
const uint8_t* ct_meta_dat_recorder_id(meta_dat_reader_t h, size_t* out_len);
void ct_meta_dat_free(meta_dat_reader_t h);

/* --------------------------------------------------------------------------
 * Streaming value encoder (zero-allocation CBOR)
 * -------------------------------------------------------------------------- */

typedef void* value_encoder_t;

value_encoder_t ct_value_encoder_new(void);
void ct_value_encoder_free(value_encoder_t h);
void ct_value_encoder_reset(value_encoder_t h);

int ct_value_write_int(value_encoder_t h, int64_t value, uint64_t type_id);
int ct_value_write_float(value_encoder_t h, double value, uint64_t type_id);
int ct_value_write_bool(value_encoder_t h, int value);
int ct_value_write_bool_typed(value_encoder_t h, int value, uint64_t type_id);
int ct_value_write_string(value_encoder_t h, const uint8_t* data, size_t len, uint64_t type_id);
int ct_value_write_none(value_encoder_t h);
int ct_value_write_none_typed(value_encoder_t h, uint64_t type_id);
int ct_value_write_raw(value_encoder_t h, const uint8_t* data, size_t len, uint64_t type_id);
int ct_value_write_error(value_encoder_t h, const uint8_t* data, size_t len, uint64_t type_id);

int ct_value_begin_struct(value_encoder_t h, uint64_t type_id, int field_count);
int ct_value_begin_sequence(value_encoder_t h, uint64_t type_id, int element_count);
int ct_value_begin_tuple(value_encoder_t h, uint64_t type_id, int element_count);
int ct_value_end_compound(value_encoder_t h);

const uint8_t* ct_value_get_bytes(value_encoder_t h, size_t* out_len);

#ifdef __cplusplus
}
#endif

#endif /* CODETRACER_TRACE_WRITER_H */
