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

# The archive tables cannot be rebuilt: CRAN keeps about four weeks in the folder.
archive_cov_db <- function(episodes = NULL, reads = NULL) {
  path <- cov_db(snaps("2026-03-09 21:02:32"), hist_rows("2020-09-12"))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  ensure_archive_tables(con)
  if (!is.null(episodes)) DBI::dbWriteTable(con, "queue_archive_episodes", episodes, append = TRUE)
  if (!is.null(reads)) DBI::dbWriteTable(con, "queue_archive_reads", reads, append = TRUE)
  path
}

base_now <- function() {
  list(queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
       queue_history_daily = list(dates = 50L, min = "2020-09-12"))
}

base_prior <- function(...) {
  list(coverage = c(list(
    queue_snapshots = list(rows = 100L, min = "2026-03-09 21:02:32"),
    queue_history_daily = list(dates = 50L, min = "2020-09-12")), list(...)))
}

test_that("coverage reports both archive tables by count and earliest date", {
  db <- archive_cov_db(
    episodes = data.frame(package = c("Aaa", "Bbb"), version = "1.0",
                          mtime = "2026-09-20 10:00", size_kb = 20,
                          first_seen = c("2026-10-01 00:12:00", "2026-10-02 01:05:00"),
                          last_seen = "2026-10-02 01:05:00", stringsAsFactors = FALSE),
    reads = data.frame(read_at = c("2026-10-01 00:12:00", "2026-10-02 01:05:00"),
                       listed = c(1L, 2L), stringsAsFactors = FALSE))

  cov <- queue_coverage(db)

  expect_equal(cov$queue_archive_episodes,
               list(rows = 2L, min = "2026-10-01 00:12:00", max = "2026-10-02 01:05:00"))
  expect_equal(cov$queue_archive_reads,
               list(rows = 2L, min = "2026-10-01 00:12:00", max = "2026-10-02 01:05:00"))
})

test_that("coverage leaves the archive tables out until they exist", {
  cov <- queue_coverage(cov_db(snaps("2026-03-09 21:02:32"), hist_rows("2020-09-12")))

  expect_null(cov$queue_archive_episodes)
  expect_null(cov$queue_archive_reads)
})

test_that("fewer archive episodes or reads than the last release is refused", {
  prior <- base_prior(
    queue_archive_episodes = list(rows = 330L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 5L, min = "2026-10-01 00:12:00"))
  now <- c(base_now(), list(
    queue_archive_episodes = list(rows = 329L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 4L, min = "2026-10-01 00:12:00")))

  bad <- paste(retention_violations(now, prior), collapse = " ")

  expect_match(bad, "queue_archive_episodes fell from 330 rows to 329")
  expect_match(bad, "queue_archive_reads fell from 5 rows to 4")
})

test_that("an earliest first_seen or read_at that moved forward is refused", {
  prior <- base_prior(
    queue_archive_episodes = list(rows = 330L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 5L, min = "2026-10-01 00:12:00"))
  now <- c(base_now(), list(
    queue_archive_episodes = list(rows = 340L, min = "2026-10-02 01:05:00"),
    queue_archive_reads = list(rows = 6L, min = "2026-10-02 01:05:00")))

  bad <- paste(retention_violations(now, prior), collapse = " ")

  expect_match(bad, "queue_archive_episodes.first_seen moved forward")
  expect_match(bad, "queue_archive_reads.read_at moved forward")
})

test_that("an archive table that vanished is a loss, not a table to skip", {
  prior <- base_prior(queue_archive_episodes = list(rows = 330L, min = "2026-10-01 00:12:00"))

  expect_match(paste(retention_violations(base_now(), prior), collapse = " "),
               "queue_archive_episodes fell from 330 rows to 0")
})

test_that("growth in the archive tables is accepted", {
  prior <- base_prior(
    queue_archive_episodes = list(rows = 330L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 5L, min = "2026-10-01 00:12:00"))
  now <- c(base_now(), list(
    queue_archive_episodes = list(rows = 341L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 6L, min = "2026-10-01 00:12:00")))

  expect_equal(retention_violations(now, prior), character(0))
})

test_that("a manifest from before the archive tables passes", {
  now <- c(base_now(), list(
    queue_archive_episodes = list(rows = 326L, min = "2026-10-01 00:12:00"),
    queue_archive_reads = list(rows = 1L, min = "2026-10-01 00:12:00")))

  expect_equal(retention_violations(now, base_prior()), character(0))
})

test_that("a prior read of an empty folder, published with a null earliest date, passes", {
  # jsonlite writes an NA earliest date as null, which comes back as NULL.
  prior <- jsonlite::fromJSON(as.character(jsonlite::toJSON(
    base_prior(queue_archive_episodes = list(rows = 0L, min = NA_character_),
               queue_archive_reads = list(rows = 1L, min = "2026-10-01 00:12:00")),
    auto_unbox = TRUE, null = "null")), simplifyVector = FALSE)
  now <- c(base_now(), list(
    queue_archive_episodes = list(rows = 11L, min = "2026-10-02 01:05:00"),
    queue_archive_reads = list(rows = 2L, min = "2026-10-01 00:12:00")))

  expect_null(prior$coverage$queue_archive_episodes$min)
  expect_equal(retention_violations(now, prior), character(0))
})

test_that("coverage and the retention check include the per-folder read log", {
  path <- cov_db(snaps("2026-03-09 21:02:32"), hist_rows("2020-09-12"))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  record_folder_reads(con, "2026-10-01 00:12:00",
                      data.frame(folder = c("inspect", "newbies"), outcome = "ok",
                                 listed = c(1L, 2L), stringsAsFactors = FALSE))
  DBI::dbDisconnect(con)

  cov <- queue_coverage(path)

  expect_equal(cov$queue_folder_reads,
               list(rows = 2L, min = "2026-10-01 00:12:00", max = "2026-10-01 00:12:00"))
  prior <- base_prior(queue_folder_reads = list(rows = 30L, min = "2026-09-30 00:00:00"))
  bad <- paste(retention_violations(cov, prior), collapse = " ")
  expect_match(bad, "queue_folder_reads fell from 30 rows to 2")
  expect_match(bad, "queue_folder_reads.snapshot_time moved forward")
})
