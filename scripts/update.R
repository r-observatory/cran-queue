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

# --- Helper: fetch page content ---
fetch_page <- function(url) {
  con <- url(url, "r")
  on.exit(close(con))
  paste(readLines(con, warn = FALSE), collapse = "\n")
}

# --- Helper: parse subfolder names from the incoming index ---
parse_folders <- function(html) {
  # Match href="foldername/" links (directories end with /)
  m <- gregexpr('href="([^"]+/)"', html)
  matches <- regmatches(html, m)[[1]]
  # Extract folder names (strip href=" and /")
  folders <- sub('href="', '', matches)
  folders <- sub('/"$', '', folders)
  # Exclude parent directory link and "archive"
  folders <- folders[!folders %in% c("..", ".", "archive")]
  # Also exclude any absolute paths

  folders <- folders[!grepl("^/", folders)]
  folders
}

# --- Helper: parse .tar.gz entries from a subfolder page ---
parse_entries <- function(html, folder) {
  # Apache mod_autoindex table format:
  # <td><a href="Pkg_1.0.tar.gz">Pkg_1.0.tar.gz</a></td><td align="right">2026-03-09 14:28  </td>
  # We split by lines and parse each
  lines <- unlist(strsplit(html, "\n"))

  # Pattern: anchor with .tar.gz, then non-digit chars (closing tags), then date+time
  pattern <- '<a href="([^"]+\\.tar\\.gz)">[^<]+</a>[^0-9]*(\\d{4}-\\d{2}-\\d{2})\\s+(\\d{2}:\\d{2})'

  results <- list()
  for (line in lines) {
    m <- regmatches(line, regexec(pattern, line))[[1]]
    if (length(m) == 4) {
      filename <- m[2]
      date_str <- m[3]
      time_str <- m[4]
      submitted_at <- paste0(date_str, " ", time_str)

      # Parse package name and version from filename
      # Pattern: PackageName_Version.tar.gz
      pkg_match <- regmatches(filename, regexec("^(.+)_([^_]+)\\.tar\\.gz$", filename))[[1]]
      if (length(pkg_match) == 3) {
        pkg_name <- pkg_match[2]
        pkg_version <- pkg_match[3]
      } else {
        pkg_name <- sub("\\.tar\\.gz$", "", filename)
        pkg_version <- NA
      }

      results[[length(results) + 1]] <- data.frame(
        package = pkg_name,
        version = pkg_version,
        folder = folder,
        submitted_at = submitted_at,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(results) > 0) {
    do.call(rbind, results)
  } else {
    data.frame(
      package = character(0),
      version = character(0),
      folder = character(0),
      submitted_at = character(0),
      stringsAsFactors = FALSE
    )
  }
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

# Scrape each folder
all_entries <- list()
for (folder in folders) {
  folder_url <- paste0(cran_incoming_url, folder, "/")
  cat("Scraping folder:", folder, "...\n")
  tryCatch({
    folder_html <- fetch_page(folder_url)
    entries <- parse_entries(folder_html, folder)
    if (nrow(entries) > 0) {
      all_entries[[length(all_entries) + 1]] <- entries
      cat("  Found", nrow(entries), "packages\n")
    } else {
      cat("  No packages found\n")
    }
  }, error = function(e) {
    cat("  Error scraping folder", folder, ":", conditionMessage(e), "\n")
  })
}

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
  "**Total accumulated snapshots:** ", total_snapshots, "\n\n",
  "**Database size:** ", db_size, "\n"
)

writeLines(release_notes, "release_notes.md")
cat("Wrote release_notes.md\n")

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
complete <- queue_history_complete(db_path)
core <- summary_integrity_core(db_path, complete = complete)
manifest_path <- file.path(dirname(db_path), MANIFEST_FILENAME)
write_manifest(manifest_path, core)
cat(sprintf("Wrote %s (complete=%s, db_bytes=%.0f)\n",
            manifest_path, complete, core$db_bytes))

cat("Done.\n")
