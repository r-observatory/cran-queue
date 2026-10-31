# Yearly parts: closed years of queue_snapshots publish once as history-YYYY,
# the hourly release carries the current year, and a run puts them back
# together into exactly the database it split.

# A queue.db spanning 2024-2026 whose ids are not in time order, as in the real
# one: 2026's live rows were written first and the older years imported later.
build_split_db <- function() {
  dir <- tempfile("split-")
  dir.create(dir)
  path <- file.path(dir, "queue.db")
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(con, "
    CREATE TABLE queue_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_time TEXT NOT NULL,
      package TEXT NOT NULL, version TEXT, folder TEXT NOT NULL, submitted_at TEXT)")
  DBI::dbExecute(con, "CREATE INDEX idx_qs_time ON queue_snapshots(snapshot_time)")
  rows <- function(times, pkgs) data.frame(
    snapshot_time = times, package = pkgs, version = "1.0", folder = "pending",
    submitted_at = NA_character_, stringsAsFactors = FALSE)
  DBI::dbWriteTable(con, "queue_snapshots",
                    rows(c("2026-03-09 10:00:00", "2026-03-09 10:00:00", "2026-07-01 08:00:00.5"),
                         c("Aaa", "Bbb", "Aaa")), append = TRUE)
  DBI::dbWriteTable(con, "queue_snapshots",
                    rows(c("2024-05-01 00:00:00", "2024-12-31 23:59:59", "2025-01-01 00:00:00"),
                         c("Old", "Old", "Mid")), append = TRUE)
  DBI::dbExecute(con, "
    CREATE TABLE queue_scrapes (snapshot_time TEXT PRIMARY KEY, package_count INTEGER NOT NULL)")
  DBI::dbExecute(con, "INSERT INTO queue_scrapes VALUES ('2024-05-01 00:00:00', 1), ('2026-07-01 08:00:00.5', 1)")
  DBI::dbExecute(con, "
    CREATE TABLE queue_history_daily (
      date TEXT NOT NULL, folder TEXT NOT NULL, package_count INTEGER NOT NULL,
      PRIMARY KEY (date, folder))")
  DBI::dbExecute(con, "INSERT INTO queue_history_daily VALUES ('2024-05-01', 'pending', 1), ('2026-07-01', 'pending', 1)")
  DBI::dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
  path
}

snapshot_rows <- function(path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(con, "SELECT * FROM queue_snapshots ORDER BY id")
}

table_rows <- function(path, tbl) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(con, sprintf('SELECT * FROM "%s" ORDER BY 1, 2', tbl))
}

test_that("a year's part holds exactly that year's rows with their ids", {
  db <- build_split_db()
  out <- file.path(dirname(db), "queue-2024.db")

  expect_equal(export_year(db, 2024, out), 2L)
  part <- snapshot_rows(out)
  expect_equal(part$id, c(4L, 5L))
  expect_true(all(substr(part$snapshot_time, 1, 4) == "2024"))
})

test_that("the current part keeps this year's rows, every other table and the id sequence", {
  db <- build_split_db()
  out <- file.path(dirname(db), "current.db")

  export_current(db, out, 2026)

  expect_equal(snapshot_rows(out)$id, 1:3)
  expect_equal(nrow(table_rows(out, "queue_scrapes")), 2L)
  expect_equal(nrow(table_rows(out, "queue_history_daily")), 2L)
  con <- DBI::dbConnect(RSQLite::SQLite(), out)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  expect_equal(DBI::dbGetQuery(con, "SELECT seq FROM sqlite_sequence WHERE name = 'queue_snapshots'")$seq, 6L)
})

test_that("the first split publishes every closed year and describes each part", {
  db <- build_split_db()

  layout <- build_layout(db, 2026, prior = NULL)

  expect_equal(vapply(layout$parts, function(p) as.integer(p$year), 1L), c(2024L, 2025L))
  expect_equal(length(layout$new_parts), 2L)
  p24 <- layout$parts[[1]]
  expect_equal(p24$tag, "history-2024")
  expect_equal(p24$asset, "queue-2024.db.zst")
  expect_equal(p24$rows, 2L)
  expect_equal(c(p24$min, p24$max), c("2024-05-01 00:00:00", "2024-12-31 23:59:59"))
  expect_equal(p24$asset_sha256, file_sha256(layout$new_parts[[1]]$path))
  expect_equal(snapshot_rows(layout$current_db)$id, 1:3)
  expect_true(file.exists(layout$current_zst))
})

test_that("later runs reuse the published parts and publish nothing new", {
  db <- build_split_db()
  first <- build_layout(db, 2026, prior = NULL)

  again <- build_layout(db, 2026, prior = list(parts = first$parts))

  expect_length(again$new_parts, 0L)
  expect_equal(again$parts, first$parts)
})

test_that("the first run of a new year publishes the year that just closed", {
  db <- build_split_db()
  first <- build_layout(db, 2026, prior = NULL)

  rolled <- build_layout(db, 2027, prior = list(parts = first$parts))

  expect_equal(vapply(rolled$new_parts, function(p) p$tag, ""), "history-2026")
  expect_equal(nrow(snapshot_rows(rolled$current_db)), 0L)
})

test_that("a closed year that changed, or vanished, stops the split", {
  db <- build_split_db()
  first <- build_layout(db, 2026, prior = NULL)
  changed <- first$parts
  changed[[1]]$rows <- 3L
  expect_error(build_layout(db, 2026, prior = list(parts = changed)),
               "closed year 2024 no longer matches history-2024")

  extra <- c(list(modifyList(first$parts[[1]], list(year = 2023L, tag = "history-2023"))), first$parts)
  expect_error(build_layout(db, 2026, prior = list(parts = extra)),
               "lists history for 2023")
})

test_that("putting the parts back together gives back the database that was split", {
  db <- build_split_db()
  layout <- build_layout(db, 2026, prior = NULL)
  parts_dir <- file.path(dirname(db), "parts")
  dir.create(parts_dir)
  for (p in layout$new_parts) file.copy(p$path, file.path(parts_dir, basename(p$path)))
  rebuilt <- file.path(dirname(db), "rebuilt.db")
  file.copy(layout$current_db, rebuilt)

  expect_equal(assemble_parts(rebuilt, list(parts = layout$parts), parts_dir), 3L)
  expect_equal(snapshot_rows(rebuilt), snapshot_rows(db))
  expect_equal(table_rows(rebuilt, "queue_scrapes"), table_rows(db, "queue_scrapes"))
  expect_equal(table_rows(rebuilt, "queue_history_daily"), table_rows(db, "queue_history_daily"))
  expect_equal(queue_coverage(rebuilt)$queue_snapshots, queue_coverage(db)$queue_snapshots)
})

test_that("a part that is missing or does not match its sha256 stops the assembly", {
  db <- build_split_db()
  layout <- build_layout(db, 2026, prior = NULL)
  parts_dir <- file.path(dirname(db), "parts")
  dir.create(parts_dir)
  rebuilt <- file.path(dirname(db), "rebuilt.db")
  file.copy(layout$current_db, rebuilt)

  expect_error(assemble_parts(rebuilt, list(parts = layout$parts), parts_dir),
               "queue-2024.db.zst from history-2024 is missing")

  for (p in layout$new_parts) file.copy(p$path, file.path(parts_dir, basename(p$path)))
  wrong <- layout$parts
  wrong[[2]]$asset_sha256 <- strrep("0", 64)
  expect_error(assemble_parts(rebuilt, list(parts = wrong), parts_dir),
               "does not match the sha256")
})

test_that("new parts are listed one per line for the publish step", {
  db <- build_split_db()
  layout <- build_layout(db, 2026, prior = NULL)
  out <- file.path(dirname(db), "new-parts.tsv")

  write_new_parts(out, layout$new_parts)

  fields <- strsplit(readLines(out), "\t", fixed = TRUE)
  expect_equal(vapply(fields, `[`, "", 2L), c("history-2024", "history-2025"))
  expect_equal(fields[[1]][4], layout$parts[[1]]$asset_sha256)

  write_new_parts(out, list())
  expect_equal(length(readLines(out)), 0L)
})
