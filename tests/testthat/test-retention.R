# queue.db lives in the release asset, so each run's output is the next run's
# input and a run that publishes less than it started with cuts the history off
# for good. On 2026-07-16 that happened and every run stayed green, because
# nothing compared what was about to be published against what already was.

cov_db <- function(snapshots = NULL, history = NULL) {
  path <- file.path(tempfile("retain-db-"), "queue.db")
  dir.create(dirname(path))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL, version TEXT, folder TEXT NOT NULL, submitted_at TEXT)")
  DBI::dbExecute(con, "CREATE TABLE queue_history_daily (
      date TEXT NOT NULL, folder TEXT NOT NULL, package_count INTEGER NOT NULL,
      PRIMARY KEY (date, folder))")
  if (!is.null(snapshots)) DBI::dbWriteTable(con, "queue_snapshots", snapshots, append = TRUE)
  if (!is.null(history)) DBI::dbWriteTable(con, "queue_history_daily", history, append = TRUE)
  path
}

snaps <- function(times) {
  data.frame(snapshot_time = times, package = "Aaa", version = "1.0",
             folder = "newbies", submitted_at = "2026-08-01 00:00",
             stringsAsFactors = FALSE)
}

hist_rows <- function(dates) {
  data.frame(date = dates, folder = "newbies", package_count = 1L,
             stringsAsFactors = FALSE)
}

test_that("coverage reports the reach of each table, not just its size", {
  db <- cov_db(snaps(c("2026-03-09 21:02:32", "2026-08-13 17:52:38")),
               hist_rows(c("2020-09-12", "2026-08-13")))

  cov <- queue_coverage(db)

  expect_equal(cov$queue_snapshots$rows, 2L)
  expect_equal(cov$queue_snapshots$min, "2026-03-09 21:02:32")
  expect_equal(cov$queue_history_daily$dates, 2L)
  expect_equal(cov$queue_history_daily$min, "2020-09-12")
})

test_that("a run that keeps everything and adds to it is accepted", {
  prior <- list(tables = list(queue_snapshots = 100L, queue_history_daily = 50L),
                coverage = list(
                  queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
                  queue_history_daily = list(dates = 50L, min = "2020-09-12")))
  now <- list(queue_snapshots = list(rows = 108L, min = "2026-03-09 21:02:32"),
              queue_history_daily = list(dates = 51L, min = "2020-09-12"))

  expect_equal(retention_violations(now, prior), character(0))
})

test_that("a re-bootstrap that drops the accumulated snapshots is refused", {
  # The 2026-07-16 shape exactly: 323,063 rows replaced by one fresh scrape.
  prior <- list(tables = list(queue_snapshots = 323063L, queue_history_daily = 10645L),
                coverage = list(
                  queue_snapshots = list(rows = 323063L, min = "2026-03-09 21:02:32"),
                  queue_history_daily = list(dates = 2002L, min = "2020-09-12")))
  now <- list(queue_snapshots = list(rows = 254L, min = "2026-07-16 22:56:42"),
              queue_history_daily = list(dates = 2002L, min = "2020-09-12"))

  bad <- retention_violations(now, prior)

  expect_true(length(bad) > 0)
  expect_match(paste(bad, collapse = " "), "323063")
  expect_match(paste(bad, collapse = " "), "254")
})

test_that("snapshots that keep their count but lose their earliest reach are refused", {
  # A window that slid forward is a loss even when the row count looks healthy,
  # which a count-only check would wave through.
  prior <- list(coverage = list(
    queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
    queue_history_daily = list(dates = 50L, min = "2020-09-12")))
  now <- list(queue_snapshots = list(rows = 100L, min = "2026-07-16 22:56:42"),
              queue_history_daily = list(dates = 50L, min = "2020-09-12"))

  expect_match(paste(retention_violations(now, prior), collapse = " "),
               "earliest snapshot")
})

test_that("losing the pre-scraper years from the daily history is refused", {
  prior <- list(coverage = list(
    queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
    queue_history_daily = list(dates = 2002L, min = "2020-09-12")))
  now <- list(queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
              queue_history_daily = list(dates = 30L, min = "2026-07-16"))

  bad <- paste(retention_violations(now, prior), collapse = " ")

  expect_match(bad, "earliest day")
  expect_match(bad, "2002")
})

test_that("the daily history may shrink in rows while covering the same days", {
  # The rollup rewrites a day from our own snapshots, and a day can genuinely
  # carry fewer folders than the cransays backfill recorded for it. Days are the
  # invariant, row count is not.
  prior <- list(tables = list(queue_history_daily = 11469L),
                coverage = list(
                  queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
                  queue_history_daily = list(dates = 2002L, min = "2020-09-12", rows = 11469L)))
  now <- list(queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
              queue_history_daily = list(dates = 2002L, min = "2020-09-12", rows = 11400L))

  expect_equal(retention_violations(now, prior), character(0))
})

test_that("a prior manifest with no coverage still guards the snapshot count", {
  # Every release published before this shipped carries `tables` but no
  # `coverage`, so the check has to work against those too rather than passing
  # vacuously on the very releases it most needs to compare against.
  prior <- list(tables = list(queue_snapshots = 323063L, queue_history_daily = 10645L))
  now <- list(queue_snapshots = list(rows = 254L, min = "2026-07-16 22:56:42"),
              queue_history_daily = list(dates = 2002L, min = "2020-09-12"))

  expect_match(paste(retention_violations(now, prior), collapse = " "), "323063")
})

test_that("a genuine cold start has nothing to compare against and is allowed", {
  expect_equal(retention_violations(
    list(queue_snapshots = list(rows = 254L, min = "2026-07-16 22:56:42"),
         queue_history_daily = list(dates = 2002L, min = "2020-09-12")),
    NULL), character(0))
})
