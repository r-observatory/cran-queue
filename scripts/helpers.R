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

#' How far back each accumulating table reaches, not just how big it is.
#'
#' Recorded in the manifest so the NEXT run can check it did not lose anything.
#' Row counts alone are not enough: a snapshot window that slid forward keeps its
#' count while losing its earliest months.
queue_coverage <- function(db_path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  s <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n, MIN(snapshot_time) AS lo,
                                    MAX(snapshot_time) AS hi FROM queue_snapshots")
  h <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n, COUNT(DISTINCT date) AS d,
                                    MIN(date) AS lo, MAX(date) AS hi FROM queue_history_daily")
  list(
    queue_snapshots     = list(rows = as.integer(s$n), min = s$lo, max = s$hi),
    queue_history_daily = list(rows = as.integer(h$n), dates = as.integer(h$d),
                               min = h$lo, max = h$hi)
  )
}

#' What this run would destroy relative to the release it started from.
#'
#' queue.db lives in the release asset, so each run's output is the next run's
#' input: a run that publishes less than it started with cuts that history off
#' for everyone downstream, permanently. On 2026-07-16 a transient API failure
#' did exactly that, dropped 323,063 snapshots, and stayed green, because
#' nothing compared the two.
#'
#' The two tables get different invariants on purpose. queue_snapshots is
#' append-only, so both its row count and its earliest row must hold. The daily
#' history is derived and MAY legitimately shed rows, because rolling a day up
#' from our own snapshots can record fewer folders than the cransays backfill
#' did for that same day; what must hold there is the DAYS covered, and the
#' earliest of them, which is what carries the pre-scraper years.
#'
#' `prior` is the previous release's parsed manifest.json, or NULL for a genuine
#' cold start. Manifests published before this existed carry `tables` but no
#' `coverage`, so the snapshot count falls back to that rather than passing
#' vacuously on precisely the releases it most needs to compare against.
#'
#' Returns a character vector of violations, empty when the run retains
#' everything.
retention_violations <- function(now, prior) {
  if (is.null(prior)) return(character(0))
  out <- character(0)
  usable <- function(x) !is.null(x) && length(x) == 1L && !is.na(x)

  snap_rows <- prior$coverage$queue_snapshots$rows
  if (!usable(snap_rows)) snap_rows <- prior$tables$queue_snapshots
  if (usable(snap_rows) && now$queue_snapshots$rows < as.integer(snap_rows)) {
    out <- c(out, sprintf("queue_snapshots fell from %d rows to %d",
                          as.integer(snap_rows), now$queue_snapshots$rows))
  }

  snap_min <- prior$coverage$queue_snapshots$min
  if (usable(snap_min) && usable(now$queue_snapshots$min) &&
      now$queue_snapshots$min > snap_min) {
    out <- c(out, sprintf("the earliest snapshot moved forward from %s to %s",
                          snap_min, now$queue_snapshots$min))
  }

  days <- prior$coverage$queue_history_daily$dates
  if (usable(days) && now$queue_history_daily$dates < as.integer(days)) {
    out <- c(out, sprintf("queue_history_daily fell from %d days to %d",
                          as.integer(days), now$queue_history_daily$dates))
  }

  day_min <- prior$coverage$queue_history_daily$min
  if (usable(day_min) && usable(now$queue_history_daily$min) &&
      now$queue_history_daily$min > day_min) {
    out <- c(out, sprintf("the earliest day in queue_history_daily moved forward from %s to %s",
                          day_min, now$queue_history_daily$min))
  }

  out
}

# GitHub hard-caps a single release asset at 2 GiB. The merger guards its own
# asset against the same number (data/.github/workflows/merge.yml), so the two
# pipelines fail on the same boundary rather than each discovering it their own
# way.
RELEASE_ASSET_MAX_BYTES <- 2040109465
# Fraction of the cap at which an asset starts saying so. A guard that only
# fires AT the cap reports the problem on the day collection stops, which is far
# too late to change the asset layout.
RELEASE_ASSET_WARN_AT <- 0.8

#' Compress a finalized database into the asset that actually gets published.
#'
#' queue.db is roughly nine times its compressed size, and a release is published
#' every hour, so the uncompressed asset is both the thing that will eventually
#' meet the 2 GiB cap and a standing contributor to the pile of superseded
#' release assets. Compressing is what lets the history keep growing without ever
#' having to be trimmed to fit.
#'
#' Returns the path to the compressed file.
compress_asset <- function(db_path, level = 12L) {
  stopifnot(file.exists(db_path))
  if (!nzchar(Sys.which("zstd"))) stop("zstd not found; cannot build the release asset")
  zst <- paste0(db_path, ".zst")
  status <- system2("zstd", c(sprintf("-%d", level), "-q", "-f", "-T0",
                              shQuote(db_path), "-o", shQuote(zst)))
  if (!identical(status, 0L) || !file.exists(zst)) stop("zstd failed to compress ", db_path)
  zst
}

#' Manifest fields describing the compressed asset a consumer actually downloads.
#'
#' summary_integrity_core() describes the DATABASE. A consumer pulling the
#' compressed asset can only verify these bytes before decompressing, so the
#' asset gets its own size and hash rather than being taken on trust.
compressed_asset_core <- function(zst_path) {
  stopifnot(file.exists(zst_path))
  list(
    asset_filename = basename(zst_path),
    asset_bytes    = file.size(zst_path),
    asset_sha256   = file_sha256(zst_path)
  )
}

#' Assets that would be refused by the release-asset cap.
#'
#' `sizes` is a named vector of bytes keyed by filename. Returns a character
#' vector of violations, empty when every asset fits.
asset_size_violations <- function(sizes, max_bytes = RELEASE_ASSET_MAX_BYTES,
                                  warn_at = RELEASE_ASSET_WARN_AT) {
  over <- sizes[sizes > max_bytes]
  if (length(over) == 0L) return(character(0))
  sprintf("%s is %.0f bytes, over the %.0f-byte release-asset cap",
          names(over), as.numeric(over), max_bytes)
}

#' Assets close enough to the cap to be worth acting on before they hit it.
asset_size_warnings <- function(sizes, max_bytes = RELEASE_ASSET_MAX_BYTES,
                                warn_at = RELEASE_ASSET_WARN_AT) {
  near <- sizes[sizes > max_bytes * warn_at & sizes <= max_bytes]
  if (length(near) == 0L) return(character(0))
  sprintf("%s is at %.0f%% of the release-asset cap (%.0f of %.0f bytes)",
          names(near), 100 * as.numeric(near) / max_bytes, as.numeric(near), max_bytes)
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
