# queue_snapshots holds one row per package per scrape, so a scrape that found
# an empty queue writes nothing and is indistinguishable afterwards from a scrape
# that never ran. Both simply have no rows for that moment. queue_scrapes records
# the scrape itself, so "the queue was empty" and "we were not looking" stop
# being the same fact.

scrape_db <- function(snapshots = NULL, scrapes = NULL) {
  path <- file.path(tempfile("scrape-db-"), "queue.db")
  dir.create(dirname(path))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL, version TEXT, folder TEXT NOT NULL, submitted_at TEXT)")
  DBI::dbExecute(con, "CREATE TABLE queue_scrapes (
      snapshot_time TEXT PRIMARY KEY, package_count INTEGER NOT NULL)")
  if (!is.null(snapshots)) DBI::dbWriteTable(con, "queue_snapshots", snapshots, append = TRUE)
  if (!is.null(scrapes))   DBI::dbWriteTable(con, "queue_scrapes", scrapes, append = TRUE)
  con
}

snap <- function(time, package, folder = "newbies") {
  data.frame(snapshot_time = time, package = package, version = "1.0",
             folder = folder, submitted_at = "2026-08-01 00:00",
             stringsAsFactors = FALSE)
}

scrapes <- function(con) {
  DBI::dbGetQuery(con, "SELECT snapshot_time, package_count FROM queue_scrapes
                          ORDER BY snapshot_time")
}

test_that("a scrape that found packages is recorded with its count", {
  con <- scrape_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  record_scrape(con, "2026-08-15 10:00:00", 8L)

  expect_equal(scrapes(con),
               data.frame(snapshot_time = "2026-08-15 10:00:00", package_count = 8L,
                          stringsAsFactors = FALSE))
})

test_that("a scrape that found an empty queue is recorded as zero, not skipped", {
  # The whole point: this is the observation that currently vanishes.
  con <- scrape_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  record_scrape(con, "2026-08-15 11:00:00", 0L)

  expect_equal(nrow(scrapes(con)), 1L)
  expect_equal(scrapes(con)$package_count, 0L)
})

test_that("re-recording a scrape replaces it rather than duplicating it", {
  con <- scrape_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  record_scrape(con, "2026-08-15 10:00:00", 8L)
  record_scrape(con, "2026-08-15 10:00:00", 9L)

  expect_equal(nrow(scrapes(con)), 1L)
  expect_equal(scrapes(con)$package_count, 9L)
})

test_that("the census derives one row per scrape from the snapshots already stored", {
  # Six years of snapshots predate this table, so the record has to be
  # reconstructed from them rather than starting empty.
  con <- scrape_db(snapshots = rbind(
    snap("2026-08-14 09:00:00", c("Aaa", "Bbb")),
    snap("2026-08-14 22:00:00", "Aaa"),
    snap("2026-08-15 09:00:00", c("Aaa", "Bbb", "Ccc"))))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  n <- backfill_scrapes(con)

  expect_equal(n, 3L)
  expect_equal(scrapes(con)$package_count, c(2L, 1L, 3L))
})

test_that("the census does not disturb a scrape already recorded as empty", {
  # An empty scrape has no snapshots to derive from, so a backfill that rebuilt
  # the table from queue_snapshots alone would erase exactly the rows this table
  # exists to hold.
  con <- scrape_db(
    snapshots = snap("2026-08-15 09:00:00", "Aaa"),
    scrapes = data.frame(snapshot_time = "2026-08-15 03:00:00", package_count = 0L,
                         stringsAsFactors = FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  backfill_scrapes(con)

  expect_equal(scrapes(con)$snapshot_time,
               c("2026-08-15 03:00:00", "2026-08-15 09:00:00"))
  expect_equal(scrapes(con)$package_count, c(0L, 1L))
})

test_that("the census is idempotent", {
  con <- scrape_db(snapshots = snap("2026-08-15 09:00:00", c("Aaa", "Bbb")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  backfill_scrapes(con); backfill_scrapes(con)

  expect_equal(nrow(scrapes(con)), 1L)
  expect_equal(scrapes(con)$package_count, 2L)
})

test_that("a day is unobserved only when no scrape ran, empty or otherwise", {
  con <- scrape_db(scrapes = data.frame(
    snapshot_time = c("2026-08-13 09:00:00", "2026-08-15 09:00:00"),
    package_count = c(4L, 0L), stringsAsFactors = FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  obs <- observed_days(con, "2026-08-13", "2026-08-15")

  expect_equal(obs$date, c("2026-08-13", "2026-08-14", "2026-08-15"))
  expect_equal(obs$scrapes, c(1L, 0L, 1L))
  expect_equal(obs$observed, c(TRUE, FALSE, TRUE))
})
