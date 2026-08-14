# The daily rollup turns the append-only hourly snapshot stream into the one
# row per (day, folder) series the site's queue chart reads. Before this
# existed, queue_history_daily was written once by import-history.R and never
# again, so the chart's last point was frozen on the day of the last bootstrap.

# A queue.db holding only the two tables the rollup touches, so a test can state
# its snapshot stream and its starting history explicitly.
rollup_db <- function(snapshots = NULL, history = NULL) {
  path <- file.path(tempfile("rollup-db-"), "queue.db")
  dir.create(dirname(path))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)

  DBI::dbExecute(con, "
    CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL,
      version TEXT,
      folder TEXT NOT NULL,
      submitted_at TEXT
    )")
  DBI::dbExecute(con, "
    CREATE TABLE queue_history_daily (
      date TEXT NOT NULL, folder TEXT NOT NULL, package_count INTEGER NOT NULL,
      PRIMARY KEY (date, folder))")

  if (!is.null(snapshots)) {
    DBI::dbWriteTable(con, "queue_snapshots", snapshots, append = TRUE)
  }
  if (!is.null(history)) {
    DBI::dbWriteTable(con, "queue_history_daily", history, append = TRUE)
  }
  con
}

snap <- function(time, package, folder) {
  data.frame(snapshot_time = time, package = package, version = "1.0",
             folder = folder, submitted_at = "2026-08-01 00:00",
             stringsAsFactors = FALSE)
}

daily <- function(con) {
  DBI::dbGetQuery(con, "SELECT date, folder, package_count
                          FROM queue_history_daily ORDER BY date, folder")
}

test_that("the rollup writes one row per folder for a day the snapshots cover", {
  con <- rollup_db(snap("2026-08-12 22:00:00", c("Aaa", "Bbb", "Ccc"),
                        c("newbies", "newbies", "pending")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con),
               data.frame(date = c("2026-08-12", "2026-08-12"),
                          folder = c("newbies", "pending"),
                          package_count = c(2L, 1L),
                          stringsAsFactors = FALSE))
})

test_that("reviewer initials collapse into the human bucket the history already uses", {
  # The pre-scraper backfill records a package sitting with a named CRAN
  # reviewer as "human"; the live scrape records the reviewer's own initials.
  # Left unmapped, every reviewer would open a new series on the chart.
  con <- rollup_db(snap("2026-08-12 22:00:00", c("Aaa", "Bbb", "Ccc", "Ddd"),
                        c("UL", "KL", "LH", "newbies")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con)$folder, c("human", "newbies"))
  expect_equal(daily(con)$package_count, c(3L, 1L))
})

test_that("a day is counted from its last snapshot, not from every snapshot", {
  # Each hourly run re-lists the whole queue, so summing the day's snapshots
  # would multiply the queue by the number of runs that day.
  con <- rollup_db(rbind(
    snap("2026-08-12 09:00:00", c("Aaa", "Bbb"), c("newbies", "newbies")),
    snap("2026-08-12 22:00:00", c("Aaa"), c("newbies"))))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con)$package_count, 1L)
})

test_that("a package listed twice in one snapshot counts once", {
  con <- rollup_db(snap("2026-08-12 22:00:00", c("Aaa", "Aaa"),
                        c("newbies", "newbies")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con)$package_count, 1L)
})

test_that("the pre-scraper backfill rows are left untouched", {
  # queue_history_daily reaches back to 2020 via cransays; the snapshot stream
  # only starts in 2026. A rollup that rebuilt the whole table would throw the
  # earlier years away.
  con <- rollup_db(
    snapshots = snap("2026-08-12 22:00:00", "Aaa", "newbies"),
    history = data.frame(date = c("2020-09-12", "2024-01-01"),
                         folder = "inspect", package_count = 7L,
                         stringsAsFactors = FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con)$date, c("2020-09-12", "2024-01-01", "2026-08-12"))
  expect_equal(daily(con)$package_count, c(7L, 7L, 1L))
})

test_that("re-running the rollup replaces a day rather than duplicating it", {
  con <- rollup_db(snap("2026-08-12 22:00:00", c("Aaa", "Bbb"),
                        c("newbies", "newbies")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)
  roll_up_daily_history(con)

  expect_equal(nrow(daily(con)), 1L)
  expect_equal(daily(con)$package_count, 2L)
})

test_that("a folder that empties during the day drops out of that day's counts", {
  # The rollup runs every hour against a day still in progress. Upserting alone
  # would leave the morning's row for a folder that has since been cleared,
  # reporting packages that are no longer queued.
  con <- rollup_db(snap("2026-08-12 09:00:00", c("Aaa", "Bbb"),
                        c("publish", "newbies")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)
  expect_true("publish" %in% daily(con)$folder)

  DBI::dbWriteTable(con, "queue_snapshots",
                    snap("2026-08-12 22:00:00", "Bbb", "newbies"), append = TRUE)
  roll_up_daily_history(con)

  expect_equal(daily(con)$folder, "newbies")
})

test_that("the rollup backfills every day the snapshots cover, not just the last", {
  # The table has been frozen for weeks, so the first run after this ships has
  # to close the whole gap, not just add today.
  con <- rollup_db(rbind(
    snap("2026-08-10 22:00:00", "Aaa", "newbies"),
    snap("2026-08-11 22:00:00", "Bbb", "newbies"),
    snap("2026-08-12 22:00:00", "Ccc", "newbies")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  roll_up_daily_history(con)

  expect_equal(daily(con)$date, c("2026-08-10", "2026-08-11", "2026-08-12"))
})

test_that("an empty snapshot stream leaves the history alone", {
  con <- rollup_db(history = data.frame(date = "2020-09-12", folder = "inspect",
                                        package_count = 7L,
                                        stringsAsFactors = FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_equal(roll_up_daily_history(con), 0L)
  expect_equal(nrow(daily(con)), 1L)
})
