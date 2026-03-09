#!/usr/bin/env Rscript

# Import historical CRAN queue data from the cransays project.
# Source: https://github.com/r-hub/cransays (history branch)
#
# This script downloads all historical CSV snapshots, processes them,
# and stores:
#   1. queue_history_daily - daily aggregated folder counts
#   2. queue_stats - monthly turnaround statistics per folder
#
# Run once to bootstrap historical data. Subsequent runs skip if data exists.

library(RSQLite)

options(timeout = 600)

args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "queue.db"

cat("=== Importing cransays historical data ===\n")
cat("Database:", db_path, "\n")

# --- Check if historical data already exists ---
con <- dbConnect(SQLite(), db_path)

# Create tables if they don't exist
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS queue_history_daily (
    date TEXT NOT NULL,
    folder TEXT NOT NULL,
    package_count INTEGER NOT NULL,
    PRIMARY KEY (date, folder)
  )
")

existing_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM queue_history_daily")$n
if (existing_count > 1000) {
  cat("Historical data already loaded (", existing_count, " rows). Skipping import.\n")
  dbDisconnect(con)
  quit(status = 0)
}

dbDisconnect(con)

# --- Download cransays history ---
zip_url <- "https://github.com/r-hub/cransays/archive/history.zip"
zip_file <- tempfile(fileext = ".zip")
extract_dir <- tempdir()

cat("Downloading cransays history archive...\n")
download.file(zip_url, zip_file, mode = "wb", quiet = TRUE)

cat("Extracting archive...\n")
unzip(zip_file, exdir = extract_dir)

# Find extracted directory
history_dir <- file.path(extract_dir, "cransays-history")
if (!dir.exists(history_dir)) {
  # Try to find it
  dirs <- list.dirs(extract_dir, recursive = FALSE)
  history_dir <- dirs[grepl("cransays", dirs)][1]
}
if (is.na(history_dir) || !dir.exists(history_dir)) {
  stop("Could not find extracted cransays-history directory")
}

csv_files <- list.files(history_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
cat("Found", length(csv_files), "CSV files\n")

if (length(csv_files) == 0) {
  cat("No CSV files found. Exiting.\n")
  quit(status = 1)
}

# --- Process CSV files ---
# The cransays data has evolved through multiple header formats.
# We handle all of them and normalize to a common schema:
#   package, version, snapshot_time, folder, subfolder, submission_time

process_csv <- function(filepath) {
  tryCatch({
    lines <- readLines(filepath, n = 2, warn = FALSE)
    if (length(lines) < 2) return(NULL)

    header <- strsplit(lines[1], ",")[[1]]
    n_cols <- length(header)

    df <- tryCatch(
      read.csv(filepath, stringsAsFactors = FALSE, colClasses = "character"),
      error = function(e) NULL
    )
    if (is.null(df) || nrow(df) == 0) return(NULL)

    # Normalize column names based on format version
    if ("snapshot_time" %in% names(df)) {
      # Modern format (v3+)
      result <- data.frame(
        package = df$package %||% NA_character_,
        version = df$version %||% NA_character_,
        snapshot_time = df$snapshot_time,
        folder = df$folder %||% NA_character_,
        stringsAsFactors = FALSE
      )
    } else if ("date" %in% names(df) && "time" %in% names(df)) {
      # Older format with separate date/time columns
      result <- data.frame(
        package = df$package %||% NA_character_,
        version = df$version %||% NA_character_,
        snapshot_time = paste(df$date, df$time),
        folder = df$folder %||% NA_character_,
        stringsAsFactors = FALSE
      )
    } else {
      # Try to work with whatever columns exist
      result <- data.frame(
        package = if ("package" %in% names(df)) df$package else NA_character_,
        version = if ("version" %in% names(df)) df$version else NA_character_,
        snapshot_time = if ("snapshot_time" %in% names(df)) df$snapshot_time else basename(filepath),
        folder = if ("folder" %in% names(df)) df$folder else NA_character_,
        stringsAsFactors = FALSE
      )
    }

    result <- result[!is.na(result$folder) & nchar(result$folder) > 0, ]
    return(result)
  }, error = function(e) {
    NULL
  })
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# Process in batches to manage memory
cat("Processing CSV files...\n")
batch_size <- 500
n_batches <- ceiling(length(csv_files) / batch_size)

all_daily <- list()

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * batch_size + 1
  end_idx <- min(b * batch_size, length(csv_files))
  batch_files <- csv_files[start_idx:end_idx]

  cat(sprintf("  Batch %d/%d (files %d-%d)...\n", b, n_batches, start_idx, end_idx))

  batch_data <- lapply(batch_files, process_csv)
  batch_data <- batch_data[!vapply(batch_data, is.null, logical(1))]

  if (length(batch_data) == 0) next

  batch_df <- do.call(rbind, batch_data)

  # Extract date from snapshot_time
  batch_df$date <- substr(batch_df$snapshot_time, 1, 10)

  # For each date, take the latest snapshot and compute folder counts
  # Group by date + folder, count packages
  daily <- aggregate(
    package ~ date + folder,
    data = batch_df,
    FUN = length
  )
  names(daily) <- c("date", "folder", "package_count")

  # If multiple snapshots per day, we get counts that are too high.
  # Take the average per day per folder instead.
  # Actually, aggregate already handles this if we first select one snapshot per day.

  # Better approach: for each date, pick the last snapshot_time,
  # then count packages in that snapshot per folder.
  snapshots_per_date <- aggregate(
    snapshot_time ~ date,
    data = batch_df,
    FUN = max
  )

  last_snapshots <- merge(batch_df, snapshots_per_date, by = c("date", "snapshot_time"))
  daily <- aggregate(
    package ~ date + folder,
    data = last_snapshots,
    FUN = length
  )
  names(daily) <- c("date", "folder", "package_count")

  all_daily[[b]] <- daily
}

cat("Combining daily aggregates...\n")
daily_combined <- do.call(rbind, all_daily)

# If a date appears in multiple batches, take the max count
# (later batch has more complete data for that day)
if (nrow(daily_combined) > 0) {
  daily_final <- aggregate(
    package_count ~ date + folder,
    data = daily_combined,
    FUN = max
  )
} else {
  cat("No data processed. Exiting.\n")
  quit(status = 1)
}

cat("Daily history:", nrow(daily_final), "rows spanning",
    min(daily_final$date), "to", max(daily_final$date), "\n")

# --- Compute monthly stats ---
cat("Computing queue statistics...\n")

stats_df <- daily_final
stats_df$month <- substr(stats_df$date, 1, 7)

queue_stats <- aggregate(
  package_count ~ month + folder,
  data = stats_df,
  FUN = function(x) round(mean(x))
)
names(queue_stats) <- c("month", "folder", "total_packages")

# --- Write to database ---
cat("Writing to database...\n")
con <- dbConnect(SQLite(), db_path)
dbExecute(con, "PRAGMA journal_mode=WAL")

# Write daily history (only date, folder, package_count columns)
dbExecute(con, "DELETE FROM queue_history_daily")
dbWriteTable(con, "queue_history_daily",
             daily_final[, c("date", "folder", "package_count")],
             append = TRUE)
cat("Wrote", nrow(daily_final), "rows to queue_history_daily\n")

# Create index
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qhd_date ON queue_history_daily(date)")

# Update queue_stats with historical monthly totals
dbExecute(con, "DROP TABLE IF EXISTS queue_stats")
dbExecute(con, "
  CREATE TABLE queue_stats (
    month TEXT NOT NULL,
    folder TEXT NOT NULL,
    total_packages INTEGER,
    PRIMARY KEY (month, folder)
  )
")
dbWriteTable(con, "queue_stats", queue_stats, append = TRUE)
cat("Wrote", nrow(queue_stats), "rows to queue_stats\n")

dbExecute(con, "ANALYZE")
dbDisconnect(con)

# Cleanup
unlink(zip_file)
unlink(history_dir, recursive = TRUE)

cat("=== Historical import complete ===\n")
