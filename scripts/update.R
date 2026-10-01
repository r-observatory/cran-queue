#!/usr/bin/env Rscript

# CRAN Incoming Queue Scraper
# Scrapes https://cran.r-project.org/incoming/ and writes to SQLite

library(RSQLite)

options(timeout = 60)

# Source the pure helpers (sha256 / integrity core / manifest writer). Resolve
# scripts/ from this file's own path so it works whether invoked as
# `Rscript scripts/update.R` from the repo root or from elsewhere.
.script_dir <- tryCatch({
  a <- commandArgs(FALSE)
  f <- sub("^--file=", "", grep("^--file=", a, value = TRUE))
  if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f)) else "scripts"
}, error = function(e) "scripts")
source(file.path(.script_dir, "helpers.R"))

# --- Configuration ---
args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "queue.db"

cran_incoming_url <- "https://cran.r-project.org/incoming/"

# The previous release's manifest, read before anything here overwrites it. It
# is what this run gets measured against before it is allowed to publish: the
# database travels in the release asset, so a run that ships less than it
# started from cuts that history off for every consumer, permanently.
prior_manifest_path <- file.path(dirname(db_path), MANIFEST_FILENAME)
prior_manifest <- if (file.exists(prior_manifest_path)) {
  tryCatch(jsonlite::fromJSON(prior_manifest_path, simplifyVector = FALSE),
           error = function(e) {
             cat("Previous manifest could not be parsed:", conditionMessage(e), "\n")
             NULL
           })
} else {
  cat("No previous manifest alongside the database; nothing to compare against.\n")
  NULL
}

# --- Helper: fetch page content ---
fetch_page <- function(url) {
  con <- url(url, "r")
  on.exit(close(con))
  paste(readLines(con, warn = FALSE), collapse = "\n")
}

# --- Main ---
cat("Connecting to database:", db_path, "\n")
con <- dbConnect(SQLite(), db_path)

# Set PRAGMAs
dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# Create queue_snapshots table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS queue_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_time TEXT NOT NULL,
    package TEXT NOT NULL,
    version TEXT,
    folder TEXT NOT NULL,
    submitted_at TEXT
  )
")

# Create indexes
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qs_time ON queue_snapshots(snapshot_time)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qs_pkg ON queue_snapshots(package)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qs_folder ON queue_snapshots(folder)")

# Snapshot time in UTC
snapshot_time <- format(Sys.time(), tz = "UTC", usetz = FALSE, format = "%Y-%m-%d %H:%M:%S")
cat("Snapshot time (UTC):", snapshot_time, "\n")

# Fetch the main incoming page
cat("Fetching CRAN incoming index...\n")
main_html <- fetch_page(cran_incoming_url)
folders <- parse_folders(main_html)
cat("Found folders:", paste(folders, collapse = ", "), "\n")

# Scrape each folder, noting for each one whether it was read
scraped <- scrape_folders(folders, fetch_page, cran_incoming_url)
all_entries <- scraped$entries

# Combine and insert
combined <- NULL
if (length(all_entries) > 0) {
  combined <- do.call(rbind, all_entries)
  combined$snapshot_time <- snapshot_time

  dbWriteTable(con, "queue_snapshots", combined, append = TRUE)
  cat("Inserted", nrow(combined), "entries into queue_snapshots\n")
} else {
  cat("No entries found across all folders\n")
}

# --- Record the scrape itself, whatever it found ---
# An empty queue writes no snapshot rows, so without this a clear queue and a
# run that never happened look identical afterwards. CRAN's incoming queue drained
# to single digits in August 2026, so this is not a hypothetical shape.
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS queue_scrapes (
    snapshot_time TEXT PRIMARY KEY,
    package_count INTEGER NOT NULL
  )
")
# Six years of snapshots predate the table; recover what can be recovered once.
# Additive, so the empty scrapes it cannot derive are never disturbed.
backfilled <- backfill_scrapes(con)
if (backfilled > 0L) cat("Recovered", backfilled, "past scrapes into queue_scrapes\n")
record_scrape(con, snapshot_time, if (length(all_entries) > 0) nrow(combined) else 0L)
# Which folders this scrape read, so a scrape that lost one is not taken for a quiet queue.
record_folder_reads(con, snapshot_time, scraped$reads)

# --- Roll the snapshots up into the daily history ---
# import-history.R seeds this table once from cransays and then skips itself
# forever, so without this step the series the site charts stops on the day of
# that bootstrap while the snapshot stream carries on past it.
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS queue_history_daily (
    date TEXT NOT NULL,
    folder TEXT NOT NULL,
    package_count INTEGER NOT NULL,
    PRIMARY KEY (date, folder)
  )
")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qhd_date ON queue_history_daily(date)")
cat("Rolled up", roll_up_daily_history(con), "daily history rows\n")

# --- Roll the snapshots up into one row per submission ---
# The stream describes moments; every question about a submission (how long did
# this package+version sit, which folder did it end in) otherwise has to derive
# the submission list by scanning all 3.55M rows first, 23.0s of a 23.8s query.
# Only this run's packages are recomputed, 1.5s against the published database
# where a full rebuild of the table is a minute or more; update_submissions()
# falls back to that rebuild for the cases where narrowing would be wrong.
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS queue_submissions (
    package TEXT NOT NULL,
    version TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    submitted_at TEXT,
    last_folder TEXT NOT NULL,
    n_observations INTEGER NOT NULL,
    PRIMARY KEY (package, version)
  )
")
cat("Rewrote", update_submissions(con, snapshot_time), "submission rows\n")

# --- CRAN's incoming/archive/, read once a UTC day into its own tables ---
# A failed read never fails the run and leaves no read row, so the next run
# that day tries again.
archive_read <- read_archive_folder(con, snapshot_time, fetch_page, cran_incoming_url)
if (archive_read$status == "ok") {
  cat("Archive folder:", archive_read$listed, "listed,", archive_read$new, "new\n")
}

# --- Compute queue_stats ---
dbExecute(con, "DROP TABLE IF EXISTS queue_stats")
dbExecute(con, "
  CREATE TABLE queue_stats (
    month TEXT,
    folder TEXT,
    total_packages INTEGER,
    PRIMARY KEY (month, folder)
  )
")
dbExecute(con, "
  INSERT INTO queue_stats (month, folder, total_packages)
  SELECT
    substr(snapshot_time, 1, 7) AS month,
    folder,
    COUNT(*) AS total_packages
  FROM queue_snapshots
  GROUP BY substr(snapshot_time, 1, 7), folder
")
cat("Updated queue_stats table\n")

# --- Generate release notes ---
total_packages <- if (length(all_entries) > 0) nrow(combined) else 0L

# Per-folder counts for this snapshot
if (length(all_entries) > 0) {
  folder_counts <- aggregate(package ~ folder, data = combined, FUN = length)
  names(folder_counts) <- c("folder", "count")
  folder_lines <- paste0("- **", folder_counts$folder, "**: ", folder_counts$count, " packages")
} else {
  folder_lines <- "- No packages found"
}

# Total accumulated snapshots
total_snapshots <- dbGetQuery(con, "SELECT COUNT(DISTINCT snapshot_time) AS n FROM queue_snapshots")$n

# DB file size
db_size_bytes <- file.info(db_path)$size
if (db_size_bytes >= 1024 * 1024) {
  db_size <- sprintf("%.1f MB", db_size_bytes / (1024 * 1024))
} else {
  db_size <- sprintf("%.1f KB", db_size_bytes / 1024)
}

release_notes <- paste0(
  "## CRAN Queue Snapshot\n\n",
  "**Snapshot time (UTC):** ", snapshot_time, "\n\n",
  "**Total packages in this snapshot:** ", total_packages, "\n\n",
  "### Per-folder counts\n\n",
  paste(folder_lines, collapse = "\n"), "\n\n",
  folder_reads_notes_line(scraped$reads),
  archive_notes_line(archive_read),
  "**Total accumulated snapshots:** ", total_snapshots, "\n\n",
  "**Database size:** ", db_size, "\n"
)

writeLines(release_notes, "release_notes.md")
cat("Wrote release_notes.md\n")
cat(release_asset_note(), file = "release_notes.md", append = TRUE)

# --- Finalize the database and write the integrity manifest ---
# Checkpoint the WAL back into the main file and close the connection so the
# manifest hashes the exact on-disk bytes of queue.db, with no open handle,
# journal, or -wal sidecar skewing the size/sha256.
dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
dbDisconnect(con)

# complete is DERIVED, not hardcoded. queue.db accumulates hourly snapshots plus
# a one-time historical daily backfill (queue_history_daily) seeded by
# import-history.R; that backfill is the pipeline's genuine bootstrap state.
# complete = full-not-partial is therefore derived from the backfill being
# present. The append-only snapshot stream's recency is reported separately via
# the manifest generated_at + db_sha256 fingerprint, not via complete.

# --- Refuse to publish a database that lost history ---
# The check sits here, after the file is finalized and before the release step
# can run, because a green run that quietly shipped less than it received is the
# failure this pipeline has actually had.
coverage <- queue_coverage(db_path)
violations <- retention_violations(coverage, prior_manifest)
if (length(violations) > 0) {
  cat("\nRefusing to publish: this run would drop history the last release had.\n")
  for (v in violations) cat("  -", v, "\n")
  cat("The previous release still holds it. Do not re-run until the cause is known.\n")
  stop("retention check failed")
}
cat(sprintf("Retention OK: %d snapshots from %s, %d days of history from %s\n",
            coverage$queue_snapshots$rows, coverage$queue_snapshots$min,
            coverage$queue_history_daily$dates, coverage$queue_history_daily$min))


# --- Build the published asset, and refuse to publish one that cannot upload ---
# Compressing is what lets the history keep growing without ever being trimmed
# to fit: the database is about nine times its compressed size, and a release
# goes out every hour. Only the compressed file is uploaded.
zst_path <- compress_asset(db_path)
asset_sizes <- published_asset_sizes(zst_path)
cat(sprintf("Asset: %s %.1f MB -> %s %.1f MB (%.1fx smaller)\n",
            basename(db_path), file.size(db_path) / 1024^2,
            basename(zst_path), asset_sizes[[1]] / 1024^2,
            file.size(db_path) / asset_sizes[[1]]))

oversized <- asset_size_violations(asset_sizes)
if (length(oversized) > 0) {
  cat("\nRefusing to publish: an asset would be rejected by the release-asset cap.\n")
  for (v in oversized) cat("  -", v, "\n")
  cat("Trimming history to fit is not the answer; change what the release carries.\n")
  stop("release asset too large")
}
for (w in asset_size_warnings(asset_sizes)) cat("NOTE:", w, "\n")

complete <- queue_history_complete(db_path)
core <- summary_integrity_core(db_path, complete = complete)
core$coverage <- coverage
core <- c(core, compressed_asset_core(zst_path))
manifest_path <- file.path(dirname(db_path), MANIFEST_FILENAME)
write_manifest(manifest_path, core)
cat(sprintf("Wrote %s (complete=%s, db_bytes=%.0f)\n",
            manifest_path, complete, core$db_bytes))

cat("Done.\n")
