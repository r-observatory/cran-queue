# scripts/helpers.R: pure helpers for the cran-queue pipeline.
#
# These build the release manifest.json that describes the finalized queue.db so
# a downstream consumer can content-verify the asset it pulls (exact bytes, table
# row counts) and tell a full snapshot apart from a partial/bootstrap one.

MANIFEST_FILENAME <- "manifest.json"

# The historical daily backfill (queue_history_daily) is a one-time bootstrap
# loaded by scripts/import-history.R; that script itself treats the backfill as
# present once the table holds more than this many rows. Kept in sync here so the
# manifest's completeness derivation matches the importer's own gate.
HISTORY_BOOTSTRAP_MIN <- 1000L

#' Compute the lowercase hex SHA-256 of a file's exact on-disk bytes.
#'
#' Uses whatever the runner already provides, in preference order:
#'   1. digest  package        (if installed)
#'   2. openssl package        (if installed)
#'   3. sha256sum (coreutils)  - present on the ubuntu-latest CI runner
#'   4. shasum -a 256 (BSD)    - macOS/local fallback
#' No heavy dependency is declared: CI installs only RSQLite and jsonlite, so the
#' coreutils `sha256sum` path is used there. If a sibling pipeline already
#' declares `digest`, that path wins automatically.
file_sha256 <- function(path) {
  if (requireNamespace("digest", quietly = TRUE)) {
    return(tolower(digest::digest(file = path, algo = "sha256")))
  }
  if (requireNamespace("openssl", quietly = TRUE)) {
    con <- file(path, open = "rb")
    on.exit(close(con), add = TRUE)
    return(tolower(as.character(openssl::sha256(con))))
  }
  sha_tool <- Sys.which("sha256sum")
  if (nzchar(sha_tool)) {
    out <- system2(sha_tool, shQuote(path), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  shasum_tool <- Sys.which("shasum")
  if (nzchar(shasum_tool)) {
    out <- system2(shasum_tool, c("-a", "256", shQuote(path)), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  stop("No SHA-256 backend found (need one of: digest, openssl, sha256sum, shasum)")
}

#' Is queue.db a complete (full-not-partial) dataset?
#'
#' queue.db carries three things: the append-only hourly `queue_snapshots`
#' stream, its derived `queue_stats`, and the one-time historical daily backfill
#' `queue_history_daily` seeded by scripts/import-history.R. The append-only
#' stream is incremental by design (each run adds one snapshot); its recency is
#' reported by the manifest's generated_at, not by `complete`. The genuine
#' partial/bootstrap state is the historical backfill: until it lands, the DB is
#' missing its entire pre-scraper foundation. So completeness is DERIVED from the
#' backfill being present (> HISTORY_BOOTSTRAP_MIN rows, matching
#' import-history.R's own gate) rather than hardcoded TRUE.
queue_history_complete <- function(db_path, min_rows = HISTORY_BOOTSTRAP_MIN) {
  stopifnot(file.exists(db_path))
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  if (!"queue_history_daily" %in% DBI::dbListTables(con)) return(FALSE)
  n <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM queue_history_daily")$n
  isTRUE(n > min_rows)
}

#' Build the integrity / completeness core describing a finalized SQLite file.
#'
#' Returns a named list of TOP-LEVEL manifest fields computed from the exact
#' on-disk bytes of `db_path` (call this only after the file is finalized and
#' its DB connection closed):
#'   * db_filename - basename of the file
#'   * db_bytes    - byte size of the file as a double. Deliberately NOT cast
#'                   to integer: R's integer range is 32-bit and overflows to
#'                   NA (serialized as the string "NA") for files >= ~2 GiB.
#'   * db_sha256   - lowercase hex sha256 of the file's exact bytes
#'   * tables      - named list mapping each user table to its row count
#'   * complete    - passed through by the caller. complete = the DB holds the
#'                   full, non-partial dataset (full-not-partial), NOT freshness:
#'                   freshness is tracked separately via generated_at and the
#'                   db_sha256 fingerprint. cran-queue has a genuine bootstrap
#'                   state (the historical backfill), so the caller DERIVES this
#'                   via queue_history_complete() instead of hardcoding it.
#' Lets a downstream merge content-verify the asset it pulls and confirm the
#' expected tables/rows are present.
summary_integrity_core <- function(db_path, complete = TRUE) {
  stopifnot(file.exists(db_path))

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  tables <- tryCatch({
    tbl_names <- DBI::dbGetQuery(con, "
      SELECT name FROM sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
       ORDER BY name")$name

    stats::setNames(
      lapply(tbl_names, function(t) {
        DBI::dbGetQuery(con, sprintf('SELECT count(*) AS n FROM "%s"', t))$n
      }),
      tbl_names
    )
  }, finally = DBI::dbDisconnect(con))

  # db_bytes/db_sha256 read the raw on-disk file only after the connection
  # above is closed, so no open handle or journal file skews the hash/size.
  list(
    db_filename = basename(db_path),
    db_bytes    = file.size(db_path),
    db_sha256   = file_sha256(db_path),
    tables      = tables,
    complete    = complete
  )
}

#' Write the release manifest.json describing the finalized primary DB.
#'
#' Top-level fields: generated_at plus the integrity/completeness core produced
#' by summary_integrity_core(). `core` is merged as TOP-LEVEL fields (not nested)
#' so a downstream merge can read db_filename/db_bytes/db_sha256/tables/complete
#' directly. generated_at records freshness independently of `complete`.
write_manifest <- function(path, core,
                           generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ",
                                                 tz = "UTC")) {
  obj <- c(list(generated_at = generated_at), core)
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE, null = "null")
  writeLines(json, path)
  invisible(path)
}
