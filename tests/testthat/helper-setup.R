# testthat runs with the working directory set to tests/testthat, so the
# pipeline helpers under test live two levels up in scripts/. Sourcing here (a
# helper-*.R file, auto-loaded before the tests) makes file_sha256(),
# summary_integrity_core(), write_manifest() and queue_history_complete()
# available whether the suite is launched via tests/testthat.R or test_dir().
.repo_root <- normalizePath(file.path(getwd(), "..", ".."))
source(file.path(.repo_root, "scripts", "helpers.R"))

# Build a tiny, real queue.db on disk using the pipeline's own schema so the
# integrity core is computed against genuine SQLite bytes. `hist_rows` controls
# whether the historical backfill is present (drives the completeness signal).
build_queue_db <- function(hist_rows = 0L) {
  dir <- tempfile("queue-db-")
  dir.create(dir)
  path <- file.path(dir, "queue.db")
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "
    CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL,
      version TEXT,
      folder TEXT NOT NULL,
      submitted_at TEXT
    )")
  DBI::dbWriteTable(con, "queue_snapshots", data.frame(
    snapshot_time = "2026-07-15 00:00:00",
    package = c("Aaa", "Bbb", "Ccc"),
    version = c("1.0", "2.1", "0.3"),
    folder = c("inspect", "pending", "pending"),
    submitted_at = "2026-07-14 12:00",
    stringsAsFactors = FALSE), append = TRUE)

  DBI::dbExecute(con, "
    CREATE TABLE queue_stats (
      month TEXT, folder TEXT, total_packages INTEGER,
      PRIMARY KEY (month, folder))")
  DBI::dbWriteTable(con, "queue_stats", data.frame(
    month = "2026-07", folder = c("inspect", "pending"),
    total_packages = c(1L, 2L), stringsAsFactors = FALSE), append = TRUE)

  DBI::dbExecute(con, "
    CREATE TABLE queue_history_daily (
      date TEXT NOT NULL, folder TEXT NOT NULL, package_count INTEGER NOT NULL,
      PRIMARY KEY (date, folder))")
  if (hist_rows > 0L) {
    DBI::dbWriteTable(con, "queue_history_daily", data.frame(
      date = sprintf("hist-%05d", seq_len(hist_rows)),
      folder = "inspect", package_count = 1L,
      stringsAsFactors = FALSE), append = TRUE)
  }
  path
}
