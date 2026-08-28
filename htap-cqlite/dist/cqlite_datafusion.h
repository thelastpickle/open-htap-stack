/*
 * cqlite_datafusion.h — the C boundary over the cqlite DataFusion reader.
 *
 * SQL over Cassandra SSTable files as they lie, in the calling process, with no
 * Cassandra, no snapshot and no JVM.  The library owns the DataFusion session,
 * plans and runs the SQL itself, and hands rows back through the Arrow C Data
 * Interface, so a caller's Arrow version and this library's need not match.
 *
 * Hand-written and kept beside the crate it declares; the crate's own tests
 * drive these exports as C does.
 *
 * The rules
 * ---------
 *  1. Returns are CQLITE_OK on success and negative on failure.  On a negative
 *     return, an export with an `err` out-parameter sets *err when the caller
 *     passed one, and never gives both a value and an error.  Free the error
 *     with cqlite_error_free.
 *  2. Ownership.  Every pointer this library returns is freed by its matching
 *     *_free or *_close, except cqlite_build_info, which lives for the process.
 *     Every char * a caller passes is borrowed for the call only, and anything
 *     the library keeps it copies.
 *  3. Panics do not cross.  A caught panic is CQLITE_ERROR_PANIC, and reaching
 *     one is a defect in the library.
 *  4. Rows cross as an ArrowArrayStream.  The caller allocates the struct, the
 *     library populates it, and the caller then owns the release callback and
 *     must call it exactly once.
 *  5. Cancellation is per statement.  cqlite_cancel stops the one statement and
 *     no other, from any thread, at any time, any number of times.
 *  6. Thread safety.  A session may be used from several threads at once, and so
 *     may a statement.  What a caller must not do is free a handle while another
 *     thread is inside a call that holds it.
 *
 * Check cqlite_abi_version() == CQLITE_ABI_VERSION once at load and register
 * nothing if the numbers differ; that check is the whole of the version coupling
 * this design has.
 *
 * The declared column types
 * -------------------------
 * A registered table's columns are Boolean, Int8, Int16, Int32, Int64, Float32,
 * Float64, Utf8, Binary, Date32, Time64(ns) or Timestamp(ms, no zone), and
 * registration is refused if a column is anything else.  A statement's output
 * can be wider, because an aggregate introduces types no column has: count(*)
 * is Int64 whatever it counts.
 */

#ifndef CQLITE_DATAFUSION_H
#define CQLITE_DATAFUSION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The Arrow C Data Interface, under its own guards, so including arrow's own
 * abi.h beside this header is neither required nor a redefinition. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
  const char* (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* The boundary version this header declares. */
#define CQLITE_ABI_VERSION 1

/* The call succeeded. */
#define CQLITE_OK 0
/* The call failed for a reason none of the codes below names. */
#define CQLITE_ERROR (-1)
/* The files are not there to be read yet: a missing directory, a path that is
 * not a directory, or a table Cassandra has not flushed.  This state can pass on
 * its own, where CQLITE_ERROR_SCHEMA cannot, so report it as a decline. */
#define CQLITE_ERROR_NOT_READY (-2)
/* The CREATE TABLE statement does not parse, or names a CQL type the reader
 * cannot map to Arrow. */
#define CQLITE_ERROR_SCHEMA (-3)
/* An argument was null, was not UTF-8, or did not fit the type it names. */
#define CQLITE_ERROR_BAD_ARGUMENT (-4)
/* A panic was caught at the boundary.  A defect in the library. */
#define CQLITE_ERROR_PANIC (-5)

/* Opaque handles.  A session holds the registered tables; a statement is one
 * running query; an error carries a message. */
typedef struct CqliteSession cqlite_session;
typedef struct CqliteStatement cqlite_statement;
typedef struct CqliteError cqlite_error;

/* How a table is opened.  A zero field takes the library's own default, which is
 * the measured one, so a caller with nothing to say passes a zeroed struct or a
 * null pointer.  Each count is refused above 1048576, because each is a length
 * something allocates: a value near UINT64_MAX would abort the process. */
typedef struct {
  /* How many slices of the token ring a full scan divides into.  1 by default,
   * and measured: the walk repeats most of a slice's work, so N slices cost N
   * times the processor time and buy no wall clock. */
  uint64_t splits;
  /* How many rows accumulate before a batch is emitted.  8192 by default. */
  uint64_t batch_rows;
  /* How many of the partitions a predicate names are read at a time.  1 by
   * default, and measured: the seek merger decodes every row of every partition
   * it is given before the merge starts, some 3.9 GB per million rows. */
  uint64_t key_chunk;
} cqlite_open_options;

/* What a table's directory holds now. */
typedef struct {
  uint64_t files;          /* How many SSTable files the directory holds. */
  uint64_t bytes;          /* Their total size. */
  int64_t data_age_secs;   /* Seconds since the newest was written, or -1. */
} cqlite_discovery;

/* What one statement read. */
typedef struct {
  /* How many table scans the statement planned.  A statement reading one table
   * twice counts two: each scan lists the directory again. */
  uint64_t tables;
  /* The files those scans opened, summed. */
  uint64_t files;
  /* Their total size, summed.  The size of the files opened, which is not what
   * the statement read when it names partitions; quote a rate from it only for
   * a statement with no WHERE. */
  uint64_t bytes;
  /* The time spent opening readers, summed over the scans. */
  double reader_open_ms;
  /* Seconds since the newest file any scan opened was written, or -1.  The
   * largest of the scans' ages, because an answer is as stale as its stalest
   * table. */
  int64_t data_age_secs;
} cqlite_scan;

/* The three structs cross by layout, so their sizes are pinned here as well as
 * in the crate's own tests: every field is fixed-width and 8-byte aligned, and a
 * compiler that pads one of them differently is caught at the caller's compile
 * time rather than at its first wrong figure. */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(cqlite_open_options) == 24, "cqlite_open_options is 24 bytes");
_Static_assert(sizeof(cqlite_discovery) == 24, "cqlite_discovery is 24 bytes");
_Static_assert(sizeof(cqlite_scan) == 40, "cqlite_scan is 40 bytes");
#endif

/* The boundary version this library was built with. */
uint32_t cqlite_abi_version(void);

/* What this library is: its version, its DataFusion version, and the commit it
 * was built from.  Lives for the process, so never freed by the caller. */
const char* cqlite_build_info(void);

/* Opens a session, writing it to *out. */
int cqlite_session_open(cqlite_session** out, cqlite_error** err);

/* Closes a session, freeing it.  A null pointer is ignored.  Safe to call while
 * one of this session's statements is still streaming. */
void cqlite_session_close(cqlite_session* session);

/* Registers `directory` as the table `name`, whose shape `create_table_cql`
 * gives.  `options` may be null.  Registering a name twice replaces the first
 * table.  No SSTable is read here, so a table Cassandra has not flushed
 * registers without complaint and declines at query time instead. */
int cqlite_register_table(const cqlite_session* session,
                          const char* name,
                          const char* directory,
                          const char* create_table_cql,
                          const cqlite_open_options* options,
                          cqlite_error** err);

/* Removes the table `name`, which must be registered. */
int cqlite_deregister_table(const cqlite_session* session,
                            const char* name,
                            cqlite_error** err);

/* Writes what the table `name` holds now to *out, reading no SSTable.  Fails
 * with CQLITE_ERROR_NOT_READY if the directory holds no SSTable. */
int cqlite_discover(const cqlite_session* session,
                    const char* name,
                    cqlite_discovery* out,
                    cqlite_error** err);

/* Plans and starts `sql`, populating *out_stream with its rows and *out_stmt
 * with a handle on the statement.  Nothing has been read when this returns: the
 * scan runs as the stream is pulled, so a failure of the files themselves
 * arrives from the stream rather than from here.  *out_stream is overwritten
 * rather than released first, as the Arrow C Data Interface expects, so pass an
 * uninitialised or an empty struct.  The caller owns both: release the stream
 * through its own release callback exactly once, and close the statement with
 * cqlite_stmt_close. */
int cqlite_query(const cqlite_session* session,
                 const char* sql,
                 struct ArrowArrayStream* out_stream,
                 cqlite_statement** out_stmt,
                 cqlite_error** err);

/* Stops the statement.  From any thread, at any time, any number of times.  The
 * drain then fails with a message that says the scan was cancelled.  Returns
 * CQLITE_ERROR_BAD_ARGUMENT for a null pointer and reports nothing else. */
int cqlite_cancel(const cqlite_statement* stmt);

/* Writes what the statement read to *out.  Readable while it runs, and complete
 * once its stream has ended. */
int cqlite_stmt_scan(const cqlite_statement* stmt, cqlite_scan* out);

/* Closes a statement, freeing it.  A null pointer is ignored.  The stream is
 * separate and is released separately; closing the statement first gives up only
 * the ability to cancel what is left of the scan. */
void cqlite_stmt_close(cqlite_statement* stmt);

/* The message an error carries, borrowed until the error is freed.  Null for a
 * null error, and never null otherwise. */
const char* cqlite_error_message(const cqlite_error* err);

/* Frees an error.  A null pointer is ignored, and any message pointer taken for
 * this error is invalid afterwards. */
void cqlite_error_free(cqlite_error* err);

#ifdef __cplusplus
}
#endif

#endif /* CQLITE_DATAFUSION_H */
