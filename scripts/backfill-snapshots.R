#!/usr/bin/env Rscript

# One-shot import of the raw per-snapshot history from the r-hub/cransays
# archive into queue_snapshots.
#
# WHY THIS EXISTS, AND WHY ONCE
#
# import-history.R has always downloaded this same archive and thrown away all
# but a daily aggregate, so queue_snapshots has only ever reached back to the
# day our own scraper started (2026-03-09). The archive itself holds one row per
# package per scrape going back to 2020-09-12, which is what the wait-time and
# folder-transition views are computed from. Importing it takes those views from
# months of history to years.
#
# It runs once because the source has stopped. r-hub/cransays' Render-dashboard
# workflow last succeeded 2026-08-05T07:27Z and has failed since, and upstream
# issue #104 proposes dropping entries older than a year. This is not an ongoing
# dependency and must not become one: our own scraper is the live source.
#
# THE CLEAN CUT
#
# The archive overlaps our own stream from 2026-03-09 onward. Both describe the
# same scrapes at slightly different times, so a union would put two rows where
# one package sat in one folder and inflate every daily count. Only rows strictly
# older than our earliest snapshot are imported. Nothing already collected is
# touched, and the boundary is read from the database rather than hardcoded.

suppressWarnings(suppressMessages({
  library(RSQLite)
}))

.script_dir <- tryCatch({
  a <- commandArgs(FALSE)
  f <- sub("^--file=", "", grep("^--file=", a, value = TRUE))
  if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f)) else "scripts"
}, error = function(e) "scripts")
source(file.path(.script_dir, "helpers.R"))

args <- commandArgs(trailingOnly = TRUE)
db_path      <- if (length(args) >= 1) args[1] else "queue.db"
history_dir  <- if (length(args) >= 2) args[2] else stop("usage: backfill-snapshots.R <queue.db> <cransays-history-dir>")

stopifnot(file.exists(db_path), dir.exists(history_dir))

`%||%` <- function(x, y) if (is.null(x)) y else x

# The archive's header has changed several times. Every variant carries the five
# things we need, under different names, so normalize rather than branch on a
# declared version that early files do not have.
read_snapshot <- function(path) {
  df <- tryCatch(
    read.csv(path, stringsAsFactors = FALSE, colClasses = "character"),
    error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0L) return(NULL)

  snapshot_time <- if (!is.null(df$snapshot_time)) df$snapshot_time
    else if (!is.null(df$date) && !is.null(df$time)) paste(df$date, df$time)
    else return(NULL)

  out <- data.frame(
    snapshot_time = snapshot_time,
    package       = df$package %||% NA_character_,
    version       = df$version %||% NA_character_,
    folder        = df$folder %||% NA_character_,
    subfolder     = df$subfolder %||% NA_character_,
    submitted_at  = df$submission_time %||% NA_character_,
    stringsAsFactors = FALSE)

  # A row with no folder cannot be placed, and a row with no package is not a
  # package. Both appear in the earliest files.
  out <- out[!is.na(out$folder) & nzchar(out$folder) &
             !is.na(out$package) & nzchar(out$package), , drop = FALSE]
  if (nrow(out) == 0L) return(NULL)

  # Reconcile the archive's folder/subfolder split with our own flat folder.
  out$folder <- cransays_folder(out$folder, out$subfolder)
  out$subfolder <- NULL
  out
}

con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

before <- dbGetQuery(con, "SELECT COUNT(*) n, MIN(snapshot_time) lo, MAX(snapshot_time) hi
                             FROM queue_snapshots")
cat(sprintf("before: %d rows, %s .. %s\n", before$n, before$lo, before$hi))
if (is.na(before$lo)) stop("queue_snapshots is empty; refusing to guess a cut boundary")
cutoff <- before$lo
cat(sprintf("cut: importing only rows strictly older than %s\n", cutoff))

files <- list.files(history_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
cat(sprintf("archive: %d csv files\n", length(files)))

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=OFF")

kept <- 0L; skipped_overlap <- 0L; unreadable <- 0L
batch <- vector("list", 0L); batch_rows <- 0L

flush_batch <- function() {
  if (length(batch) == 0L) return(invisible(NULL))
  df <- do.call(rbind, batch)
  dbWriteTable(con, "queue_snapshots", df, append = TRUE)
  batch <<- vector("list", 0L); batch_rows <<- 0L
  invisible(NULL)
}

dbExecute(con, "BEGIN")
for (i in seq_along(files)) {
  rows <- read_snapshot(files[i])
  if (is.null(rows)) { unreadable <- unreadable + 1L; next }

  n_all <- nrow(rows)
  rows <- rows[rows$snapshot_time < cutoff, , drop = FALSE]
  skipped_overlap <- skipped_overlap + (n_all - nrow(rows))
  if (nrow(rows) == 0L) next

  batch[[length(batch) + 1L]] <- rows
  batch_rows <- batch_rows + nrow(rows)
  kept <- kept + nrow(rows)
  if (batch_rows >= 50000L) flush_batch()

  if (i %% 5000L == 0L) cat(sprintf("  %d/%d files, %d rows kept\n", i, length(files), kept))
}
flush_batch()
dbExecute(con, "COMMIT")

cat(sprintf("imported: %d rows  (skipped %d overlapping, %d unreadable files)\n",
            kept, skipped_overlap, unreadable))

after <- dbGetQuery(con, "SELECT COUNT(*) n, MIN(snapshot_time) lo, MAX(snapshot_time) hi,
                                 COUNT(DISTINCT snapshot_time) s FROM queue_snapshots")
cat(sprintf("after : %d rows (%d snapshots), %s .. %s\n", after$n, after$s, after$lo, after$hi))
stopifnot(after$n == before$n + kept)
