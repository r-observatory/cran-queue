# Integrity / completeness core for the primary published DB (queue.db).

test_that("summary_integrity_core reports filename, bytes, sha256, tables, complete", {
  db <- build_queue_db(hist_rows = HISTORY_BOOTSTRAP_MIN + 1L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db, complete = TRUE)

  expect_equal(core$db_filename, basename(db))
  # db_bytes is a double (not cast to integer) so files >= ~2 GiB do not
  # overflow to NA; compare against the uncast file.size() directly.
  expect_type(core$db_bytes, "double")
  expect_equal(core$db_bytes, file.size(db))
  # sha256 is lowercase 64-char hex of the exact file bytes
  expect_match(core$db_sha256, "^[0-9a-f]{64}$")
  # tables maps every user table to its row count (no sqlite_% internals)
  expect_false(any(grepl("^sqlite_", names(core$tables))))
  expect_equal(core$tables$queue_snapshots, 3L)
  expect_equal(core$tables$queue_stats, 2L)
  expect_equal(core$tables$queue_history_daily, HISTORY_BOOTSTRAP_MIN + 1L)
  expect_true(core$complete)
})

test_that("summary_integrity_core sha256 matches an independent digest of the bytes", {
  # Compute the expected hash via an external CLI tool, independent of
  # file_sha256()'s own preferred backend (digest/openssl), so this test
  # genuinely cross-checks the code path instead of re-running the same
  # library. Skip only if neither tool is on PATH (both are expected on CI).
  sha256sum_bin <- Sys.which("sha256sum")
  shasum_bin    <- Sys.which("shasum")
  if (!nzchar(sha256sum_bin) && !nzchar(shasum_bin)) {
    skip("neither sha256sum nor shasum is on PATH")
  }

  db <- build_queue_db(hist_rows = 0L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db)

  if (nzchar(sha256sum_bin)) {
    out <- system2(sha256sum_bin, shQuote(db), stdout = TRUE)
  } else {
    out <- system2(shasum_bin, c("-a", "256", shQuote(db)), stdout = TRUE)
  }
  independent <- tolower(sub("\\s.*$", "", out[1]))

  expect_equal(core$db_sha256, independent)
})

test_that("queue_history_complete derives completeness from the historical backfill", {
  # Below the importer's own gate -> a partial/bootstrap DB.
  partial <- build_queue_db(hist_rows = HISTORY_BOOTSTRAP_MIN)
  on.exit(unlink(partial), add = TRUE)
  expect_false(queue_history_complete(partial))

  # Backfill present -> a full, non-partial DB.
  full <- build_queue_db(hist_rows = HISTORY_BOOTSTRAP_MIN + 1L)
  on.exit(unlink(full), add = TRUE)
  expect_true(queue_history_complete(full))
})

test_that("write_manifest emits generated_at plus the integrity core as top-level fields", {
  db <- build_queue_db(hist_rows = HISTORY_BOOTSTRAP_MIN + 1L)
  on.exit(unlink(db), add = TRUE)
  complete <- queue_history_complete(db)
  core <- summary_integrity_core(db, complete = complete)

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)

  write_manifest(tmp, core)

  parsed <- jsonlite::fromJSON(tmp)

  # freshness field present and non-empty
  expect_true(nzchar(parsed$generated_at))
  # integrity/completeness core lives at the TOP level, not nested
  expect_equal(parsed$db_filename, "queue.db")
  expect_equal(parsed$db_sha256, core$db_sha256)
  expect_equal(parsed$db_bytes, file.size(db))
  expect_match(parsed$db_sha256, "^[0-9a-f]{64}$")
  expect_equal(parsed$tables$queue_snapshots, 3L)
  expect_true(parsed$complete)
})
