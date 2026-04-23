{.push raises: [].}

## Tests for split-by-time trace directories (CTFS split trace).

import std/[os, strutils]
import results
import codetracer_ctfs/types
import codetracer_ctfs/container
import codetracer_ctfs/split_trace

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc createTempDir(suffix: string): string =
  ## Create a temporary directory for testing. Returns the path.
  let dir = getTempDir() / "test_split_trace_" & suffix
  try:
    createDir(dir)
  except IOError, OSError:
    discard
  dir

proc cleanupDir(dir: string) =
  ## Remove a test directory and all its contents.
  try:
    removeDir(dir)
  except OSError:
    discard

proc writeTestSplit(dir: string, index: int) =
  ## Write a minimal valid CTFS container as a split file.
  var ctfs = createCtfs()
  let path = dir / splitFileName(index)
  let res = ctfs.writeCtfsToFile(path)
  doAssert res.isOk, "failed to write test split: " & res.error

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

proc test_split_trace_file_naming() =
  ## Verify file naming produces zero-padded 6-digit names with .ct extension.
  doAssert splitFileName(0) == "000000.ct"
  doAssert splitFileName(1) == "000001.ct"
  doAssert splitFileName(42) == "000042.ct"
  doAssert splitFileName(999999) == "999999.ct"

  # Verify they sort correctly as strings.
  let names = [
    splitFileName(0), splitFileName(1), splitFileName(9),
    splitFileName(10), splitFileName(100), splitFileName(999)]
  for i in 1 ..< names.len:
    doAssert names[i] > names[i - 1],
      "sort order broken: " & names[i] & " <= " & names[i - 1]

  echo "PASS: test_split_trace_file_naming"

proc test_split_trace_write_read() =
  ## Create a temp directory, write 3 split files via SplitTraceWriter,
  ## open as SplitTraceReader, verify splitCount == 3 and each is valid CTFS.
  let dir = createTempDir("write_read")
  defer: cleanupDir(dir)

  # Create writer and write 3 splits.
  var writerRes = initSplitTraceWriter(dir)
  doAssert writerRes.isOk, "initSplitTraceWriter failed: " & writerRes.error
  var writer = writerRes.get()

  for i in 0 ..< 3:
    let res = writer.startNewSplit()
    doAssert res.isOk, "startNewSplit failed at i=" & $i & ": " & res.error

    # Add a file to the CTFS to make it non-trivial.
    let fileRes = writer.currentCtfs.addFile("data" & $i)
    doAssert fileRes.isOk, "addFile failed: " & fileRes.error

  # Finalize to write the last split.
  let finRes = writer.finalize()
  doAssert finRes.isOk, "finalize failed: " & finRes.error

  doAssert writer.splitCount() == 3,
    "writer splitCount: " & $writer.splitCount() & " != 3"

  # Open as reader.
  let readerRes = openSplitTrace(dir)
  doAssert readerRes.isOk, "openSplitTrace failed: " & readerRes.error
  var reader = readerRes.get()

  doAssert reader.splitCount() == 3,
    "reader splitCount: " & $reader.splitCount() & " != 3"

  # Verify each split is a valid CTFS container.
  for i in 0 ..< reader.splitCount():
    let bytesRes = reader.readSplitBytes(i)
    doAssert bytesRes.isOk, "readSplitBytes failed at i=" & $i & ": " & bytesRes.error
    let data = bytesRes.get()
    doAssert data.len > 0, "split " & $i & " is empty"
    doAssert isValidCtfs(data), "split " & $i & " is not valid CTFS"

  # Verify file paths are sorted and correctly named.
  for i in 0 ..< reader.splitCount():
    let pathRes = reader.splitPath(i)
    doAssert pathRes.isOk
    doAssert pathRes.get().endsWith(splitFileName(i)),
      "path mismatch: " & pathRes.get() & " does not end with " & splitFileName(i)

  echo "PASS: test_split_trace_write_read"

proc test_split_trace_delete() =
  ## Create 3 split files, delete the middle one, verify remaining 2 are readable.
  let dir = createTempDir("delete")
  defer: cleanupDir(dir)

  # Write 3 splits directly.
  writeTestSplit(dir, 0)
  writeTestSplit(dir, 1)
  writeTestSplit(dir, 2)

  var readerRes = openSplitTrace(dir)
  doAssert readerRes.isOk, "openSplitTrace failed: " & readerRes.error
  var reader = readerRes.get()
  doAssert reader.splitCount() == 3

  # Delete the middle split (index 1 = 000001.ct).
  let delRes = reader.deleteSplit(1)
  doAssert delRes.isOk, "deleteSplit failed: " & delRes.error

  doAssert reader.splitCount() == 2,
    "after delete, splitCount: " & $reader.splitCount() & " != 2"

  # Verify remaining splits are still readable.
  for i in 0 ..< reader.splitCount():
    let bytesRes = reader.readSplitBytes(i)
    doAssert bytesRes.isOk, "readSplitBytes failed after delete at i=" & $i
    doAssert isValidCtfs(bytesRes.get()), "split " & $i & " invalid after delete"

  # Verify the deleted file no longer exists.
  let deletedPath = dir / splitFileName(1)
  doAssert not fileExists(deletedPath), "deleted file still exists: " & deletedPath

  echo "PASS: test_split_trace_delete"

proc test_split_trace_empty_directory() =
  ## Verify opening an empty directory returns an error.
  let dir = createTempDir("empty")
  defer: cleanupDir(dir)

  let res = openSplitTrace(dir)
  doAssert res.isErr, "expected error for empty directory"
  doAssert "no .ct files" in res.error

  echo "PASS: test_split_trace_empty_directory"

proc test_split_trace_nonexistent_directory() =
  ## Verify opening a nonexistent directory returns an error.
  let res = openSplitTrace("/tmp/nonexistent_split_trace_test_dir_xyz")
  doAssert res.isErr, "expected error for nonexistent directory"

  echo "PASS: test_split_trace_nonexistent_directory"

proc test_split_trace_writer_current_path() =
  ## Verify currentSplitPath returns correct paths.
  let dir = createTempDir("path")
  defer: cleanupDir(dir)

  var writerRes = initSplitTraceWriter(dir)
  doAssert writerRes.isOk
  var writer = writerRes.get()

  let r0 = writer.startNewSplit()
  doAssert r0.isOk
  doAssert writer.currentSplitPath() == dir / "000000.ct"

  let r1 = writer.startNewSplit()
  doAssert r1.isOk
  doAssert writer.currentSplitPath() == dir / "000001.ct"

  let r2 = writer.startNewSplit()
  doAssert r2.isOk
  doAssert writer.currentSplitPath() == dir / "000002.ct"

  let finRes = writer.finalize()
  doAssert finRes.isOk

  echo "PASS: test_split_trace_writer_current_path"

proc test_split_trace_out_of_range() =
  ## Verify out-of-range access returns errors.
  let dir = createTempDir("oor")
  defer: cleanupDir(dir)

  writeTestSplit(dir, 0)

  var readerRes = openSplitTrace(dir)
  doAssert readerRes.isOk
  var reader = readerRes.get()

  let pathRes = reader.splitPath(-1)
  doAssert pathRes.isErr, "expected error for negative index"

  let pathRes2 = reader.splitPath(999)
  doAssert pathRes2.isErr, "expected error for out of range index"

  let bytesRes = reader.readSplitBytes(999)
  doAssert bytesRes.isErr, "expected error for out of range readSplitBytes"

  let delRes = reader.deleteSplit(999)
  doAssert delRes.isErr, "expected error for out of range deleteSplit"

  echo "PASS: test_split_trace_out_of_range"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

when isMainModule:
  test_split_trace_file_naming()
  test_split_trace_write_read()
  test_split_trace_delete()
  test_split_trace_empty_directory()
  test_split_trace_nonexistent_directory()
  test_split_trace_writer_current_path()
  test_split_trace_out_of_range()
  echo "All split trace tests passed."
