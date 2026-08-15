# queue_snapshots answers questions about a MOMENT. Every question about a
# SUBMISSION (how long did this package+version sit, what folder did it end in)
# has to derive the submission list by scanning the whole stream first: 23.0s of
# a 23.8s query, against 0.04s once the same answer is precomputed.
# queue_submissions is that precompute, one row per (package, version).

sub_db <- function(snapshots = NULL, submissions = NULL) {
  path <- file.path(tempfile("submissions-db-"), "queue.db")
  dir.create(dirname(path))
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL, version TEXT, folder TEXT NOT NULL, submitted_at TEXT)")
  DBI::dbExecute(con, "CREATE TABLE queue_submissions (
      package TEXT NOT NULL, version TEXT NOT NULL, first_seen TEXT NOT NULL,
      last_seen TEXT NOT NULL, submitted_at TEXT, last_folder TEXT NOT NULL,
      n_observations INTEGER NOT NULL, PRIMARY KEY (package, version))")
  if (!is.null(snapshots)) DBI::dbWriteTable(con, "queue_snapshots", snapshots, append = TRUE)
  if (!is.null(submissions)) DBI::dbWriteTable(con, "queue_submissions", submissions, append = TRUE)
  con
}

snap <- function(time, package, folder = "newbies", version = "1.0",
                 submitted_at = "2026-08-01 00:00") {
  data.frame(snapshot_time = time, package = package, version = version,
             folder = folder, submitted_at = submitted_at,
             stringsAsFactors = FALSE)
}

submissions <- function(con) {
  DBI::dbGetQuery(con, "SELECT package, version, first_seen, last_seen, submitted_at,
                               last_folder, n_observations
                          FROM queue_submissions ORDER BY package, version")
}

test_that("each package+version becomes one row spanning the scrapes that saw it", {
  con <- sub_db(rbind(
    snap("2026-08-12 09:00:00", c("Aaa", "Bbb")),
    snap("2026-08-12 22:00:00", "Aaa"),
    snap("2026-08-13 09:00:00", "Aaa")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(submissions(con)$package, c("Aaa", "Bbb"))
  expect_equal(submissions(con)$first_seen,
               c("2026-08-12 09:00:00", "2026-08-12 09:00:00"))
  expect_equal(submissions(con)$last_seen,
               c("2026-08-13 09:00:00", "2026-08-12 09:00:00"))
  expect_equal(submissions(con)$n_observations, c(3L, 1L))
})

test_that("two versions of one package are two submissions", {
  con <- sub_db(rbind(
    snap("2026-08-12 09:00:00", "Aaa", version = "1.0"),
    snap("2026-08-13 09:00:00", "Aaa", version = "1.1")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(submissions(con)$version, c("1.0", "1.1"))
  expect_equal(submissions(con)$n_observations, c(1L, 1L))
})

test_that("a package seen in two folders in one scrape is observed once", {
  # CRAN's incoming genuinely holds a copy in waiting while another is in
  # recheck, and our scrape records both. Counting rows rather than scrapes
  # would inflate the time this submission is said to have spent queued.
  con <- sub_db(snap("2026-08-12 09:00:00", c("Aaa", "Aaa"),
                     folder = c("recheck", "waiting")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(nrow(submissions(con)), 1L)
  expect_equal(submissions(con)$n_observations, 1L)
})

test_that("submitted_at carries CRAN's own timestamp, ignoring the absent spellings", {
  # About a fifth of the stream has no submission time, recorded variously as
  # NULL, empty, or the string "NA". Treated as text, "NA" would sort below
  # every real timestamp and become the submission time of anything it touched.
  con <- sub_db(rbind(
    snap("2026-08-12 09:00:00", "Aaa", submitted_at = "NA"),
    snap("2026-08-12 22:00:00", "Aaa", submitted_at = ""),
    snap("2026-08-13 09:00:00", "Aaa", submitted_at = "2026-08-11 14:28")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(submissions(con)$submitted_at, "2026-08-11 14:28")
  expect_equal(submissions(con)$first_seen, "2026-08-12 09:00:00")
})

test_that("a submission CRAN never timestamped keeps its own first_seen", {
  con <- sub_db(snap("2026-08-12 09:00:00", "Aaa", submitted_at = NA_character_))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_true(is.na(submissions(con)$submitted_at))
  expect_equal(submissions(con)$first_seen, "2026-08-12 09:00:00")
})

test_that("last_folder is the folder of the last scrape, not the first or the commonest", {
  con <- sub_db(rbind(
    snap("2026-08-10 09:00:00", "Aaa", folder = "newbies"),
    snap("2026-08-11 09:00:00", "Aaa", folder = "newbies"),
    snap("2026-08-12 09:00:00", "Aaa", folder = "recheck")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(submissions(con)$last_folder, "recheck")
})

test_that("reviewer initials collapse into the human bucket the history already uses", {
  # Same vocabulary as queue_history_daily. Left unmapped, a submission last
  # seen with a reviewer would report that reviewer's initials as an outcome
  # folder of its own.
  con <- sub_db(snap("2026-08-12 09:00:00", c("Aaa", "Bbb"), folder = c("UL", "recheck")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(submissions(con)$last_folder, c("human", "recheck"))
})

test_that("a submission last seen in two folders at once resolves to one folder, stably", {
  con <- sub_db(snap("2026-08-12 09:00:00", c("Aaa", "Aaa"),
                     folder = c("recheck", "waiting")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)
  once <- submissions(con)$last_folder
  rebuild_submissions(con)

  expect_equal(nrow(submissions(con)), 1L)
  expect_equal(submissions(con)$last_folder, once)
  expect_true(once %in% c("recheck", "waiting"))
})

test_that("the two spellings of an unknown version are one submission, not two", {
  # The scraper writes NULL when a filename does not parse; the cransays
  # archive writes the string "NA" for the same thing.
  con <- sub_db(rbind(
    snap("2026-08-12 09:00:00", "Aaa", version = NA_character_),
    snap("2026-08-13 09:00:00", "Aaa", version = "NA")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  rebuild_submissions(con)

  expect_equal(nrow(submissions(con)), 1L)
  expect_equal(submissions(con)$version, "NA")
  expect_equal(submissions(con)$n_observations, 2L)
})

test_that("a database that has never held the table gets a full build on the first run", {
  con <- sub_db(rbind(
    snap("2026-08-12 09:00:00", c("Aaa", "Bbb")),
    snap("2026-08-13 09:00:00", "Ccc")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  n <- update_submissions(con, "2026-08-13 09:00:00")

  expect_equal(n, 3L)
  expect_equal(submissions(con)$package, c("Aaa", "Bbb", "Ccc"))
})

test_that("the hourly update only touches the packages in this run's scrape", {
  # The whole reason to be incremental: a full rebuild reads all 3.55M rows,
  # while a run's scrape names a couple of hundred packages.
  con <- sub_db(snap("2026-08-12 09:00:00", c("Aaa", "Bbb")))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  rebuild_submissions(con)

  DBI::dbWriteTable(con, "queue_snapshots",
                    snap("2026-08-13 09:00:00", c("Aaa", "Ccc"), folder = "publish"),
                    append = TRUE)
  update_submissions(con, "2026-08-13 09:00:00")

  got <- submissions(con)
  expect_equal(got$package, c("Aaa", "Bbb", "Ccc"))
  expect_equal(got$last_seen,
               c("2026-08-13 09:00:00", "2026-08-12 09:00:00", "2026-08-13 09:00:00"))
  expect_equal(got$last_folder, c("publish", "newbies", "publish"))
  expect_equal(got$n_observations, c(2L, 1L, 1L))
})

test_that("re-running a scrape's update neither duplicates it nor counts it twice", {
  # The workflow re-runs, and a run that failed after the snapshot was written
  # leaves the same scrape to be processed again.
  con <- sub_db(snap("2026-08-12 09:00:00", "Aaa"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  update_submissions(con, "2026-08-12 09:00:00")
  update_submissions(con, "2026-08-12 09:00:00")

  expect_equal(nrow(submissions(con)), 1L)
  expect_equal(submissions(con)$n_observations, 1L)
})

test_that("history imported behind the table rebuilds it rather than leaving it short", {
  # backfill-snapshots.R loads six years of older scrapes in one go, and most of
  # what it brings belongs to packages no current scrape will ever name. An
  # update narrowed to this scrape's packages would leave those submissions out
  # of the table entirely.
  con <- sub_db(snap("2026-08-12 09:00:00", "Aaa"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  rebuild_submissions(con)

  DBI::dbWriteTable(con, "queue_snapshots",
                    snap("2020-09-12 07:13:00", c("Aaa", "Zzz")), append = TRUE)
  update_submissions(con, "2026-08-12 09:00:00")

  expect_equal(submissions(con)$package, c("Aaa", "Zzz"))
  expect_equal(submissions(con)$first_seen,
               c("2020-09-12 07:13:00", "2020-09-12 07:13:00"))
  expect_equal(submissions(con)$n_observations, c(2L, 1L))
})

test_that("building scrape by scrape gives the same table as building it all at once", {
  # The incremental path is only allowed to be cheaper, not different.
  stream <- rbind(
    snap("2026-08-10 09:00:00", c("Aaa", "Bbb"), folder = c("newbies", "pretest")),
    snap("2026-08-10 22:00:00", c("Aaa", "Bbb", "Ccc"),
         folder = c("UL", "pretest", "newbies"), submitted_at = "NA"),
    snap("2026-08-11 09:00:00", c("Aaa", "Aaa", "Ccc"),
         folder = c("recheck", "waiting", "newbies"), version = c("1.0", "1.0", NA)),
    snap("2026-08-12 09:00:00", c("Bbb", "Ccc"),
         folder = c("publish", "newbies"), version = c("2.0", "NA")))

  incremental <- sub_db()
  on.exit(DBI::dbDisconnect(incremental), add = TRUE)
  for (t in unique(stream$snapshot_time)) {
    DBI::dbWriteTable(incremental, "queue_snapshots",
                      stream[stream$snapshot_time == t, ], append = TRUE)
    update_submissions(incremental, t)
  }

  full <- sub_db(snapshots = stream)
  on.exit(DBI::dbDisconnect(full), add = TRUE)
  rebuild_submissions(full)

  expect_equal(submissions(incremental), submissions(full))
  expect_gt(nrow(submissions(full)), 0L)
})

test_that("an empty scrape leaves the submissions alone", {
  con <- sub_db(snap("2026-08-12 09:00:00", "Aaa"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  rebuild_submissions(con)

  expect_equal(update_submissions(con, "2026-08-12 22:00:00"), 0L)
  expect_equal(nrow(submissions(con)), 1L)
  expect_equal(submissions(con)$last_seen, "2026-08-12 09:00:00")
})

test_that("coverage declares how far the submissions reach, and tolerates their absence", {
  # Same shape as queue_scrapes: a database published before the table existed
  # must still produce a manifest rather than failing on a missing table.
  con <- sub_db(rbind(snap("2026-08-12 09:00:00", "Aaa"),
                      snap("2026-08-13 09:00:00", "Bbb")))
  path <- DBI::dbGetInfo(con)$dbname
  DBI::dbExecute(con, "CREATE TABLE queue_history_daily (
      date TEXT NOT NULL, folder TEXT NOT NULL, package_count INTEGER NOT NULL,
      PRIMARY KEY (date, folder))")
  rebuild_submissions(con)
  DBI::dbDisconnect(con)

  cov <- queue_coverage(path)

  expect_equal(cov$queue_submissions$rows, 2L)
  expect_equal(cov$queue_submissions$min, "2026-08-12 09:00:00")
  expect_equal(cov$queue_submissions$max, "2026-08-13 09:00:00")

  older <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(older, "DROP TABLE queue_submissions")
  DBI::dbDisconnect(older)

  expect_null(queue_coverage(path)$queue_submissions)
})

# --- follow-ups from review of the first cut ---

test_that("a database that has never HELD the table gets a full build, not an error", {
  # The earlier test of this created the table empty in its fixture, so it only
  # proved the empty case. update.R happens to run CREATE TABLE IF NOT EXISTS
  # first, but every sibling helper tolerates the table being absent and a
  # consumer calling this directly should not have to know the ordering.
  con <- sub_db(snapshots = snap("2026-08-15 09:00:00", "Aaa"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS queue_submissions")

  n <- update_submissions(con, "2026-08-15 09:00:00")

  expect_equal(n, 1L)
  expect_equal(submissions(con)$package, "Aaa")
})

test_that("a shrunken submissions table is refused against the release before it", {
  # queue_submissions is monotone over an append-only stream, so a fall is
  # always a fault. Without this the manifest happily records a short table and
  # the run publishes green, which is the hole the coverage comment promised was
  # covered.
  prior <- list(coverage = list(
    queue_snapshots     = list(rows = 100L, min = "2020-09-12 07:13:00"),
    queue_history_daily = list(dates = 50L, min = "2020-09-12"),
    queue_submissions   = list(rows = 82048L)))
  now <- list(queue_snapshots     = list(rows = 100L, min = "2020-09-12 07:13:00"),
              queue_history_daily = list(dates = 50L, min = "2020-09-12"),
              queue_submissions   = list(rows = 82046L))

  bad <- paste(retention_violations(now, prior), collapse = " ")

  expect_match(bad, "queue_submissions")
  expect_match(bad, "82048")
})

test_that("a growing submissions table is accepted", {
  prior <- list(coverage = list(
    queue_snapshots     = list(rows = 100L, min = "2020-09-12 07:13:00"),
    queue_history_daily = list(dates = 50L, min = "2020-09-12"),
    queue_submissions   = list(rows = 82048L)))
  now <- list(queue_snapshots     = list(rows = 108L, min = "2020-09-12 07:13:00"),
              queue_history_daily = list(dates = 50L, min = "2020-09-12"),
              queue_submissions   = list(rows = 82050L))

  expect_equal(retention_violations(now, prior), character(0))
})

test_that("a prior release that predates the table does not trip the check", {
  prior <- list(coverage = list(
    queue_snapshots     = list(rows = 100L, min = "2020-09-12 07:13:00"),
    queue_history_daily = list(dates = 50L, min = "2020-09-12")))
  now <- list(queue_snapshots     = list(rows = 100L, min = "2020-09-12 07:13:00"),
              queue_history_daily = list(dates = 50L, min = "2020-09-12"),
              queue_submissions   = list(rows = 82048L))

  expect_equal(retention_violations(now, prior), character(0))
})
