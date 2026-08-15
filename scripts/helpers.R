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

# The folder vocabulary queue_history_daily has carried since the cransays
# backfill. CRAN's incoming area also holds a folder per reviewer, named for
# that reviewer's initials, and the backfill files all of those under "human".
# Anything outside this set is therefore a reviewer and collapses the same way;
# left unmapped, every reviewer would open their own series on the queue chart.
QUEUE_FOLDERS <- c("newbies", "inspect", "pending", "waiting",
                   "pretest", "recheck", "publish", "archive")

#' Record that a scrape happened, and what it found.
#'
#' queue_snapshots holds one row per package per scrape, so a scrape that found
#' an empty queue writes nothing and afterwards cannot be told apart from a
#' scrape that never ran: both have no rows for that moment. That distinction
#' matters to any reading of the series, because a day with no bar should mean
#' "we were not looking" and a day with a zero bar should mean "the queue was
#' clear", and today both render as the day simply not existing.
#'
#' Keyed on the scrape time, so a re-run replaces rather than duplicates.
record_scrape <- function(con, snapshot_time, package_count) {
  DBI::dbExecute(con,
    "INSERT OR REPLACE INTO queue_scrapes (snapshot_time, package_count) VALUES (?, ?)",
    params = list(as.character(snapshot_time), as.integer(package_count)))
  invisible(NULL)
}

#' Reconstruct the scrape record from the snapshots already stored.
#'
#' Six years of snapshots predate this table. Every scrape that found anything
#' can be recovered from them exactly, since each carries its own timestamp.
#'
#' Deliberately additive: a scrape already recorded is left alone. Rebuilding the
#' table from queue_snapshots instead would erase every empty scrape, which is
#' precisely the row this table exists to hold and the one that cannot be derived
#' from snapshots at all.
#'
#' Returns the number of scrapes recorded.
backfill_scrapes <- function(con) {
  n <- DBI::dbExecute(con, "
    INSERT OR IGNORE INTO queue_scrapes (snapshot_time, package_count)
      SELECT snapshot_time, COUNT(*) FROM queue_snapshots GROUP BY snapshot_time")
  as.integer(n)
}

#' Which days in a range were observed at all, and how often.
#'
#' A day is observed when at least one scrape ran on it, whatever it found. The
#' caller renders an unobserved day as a break and an observed-but-empty day as a
#' zero, which are different statements about the queue.
observed_days <- function(con, from, to) {
  days <- DBI::dbGetQuery(con, "
    WITH RECURSIVE cal(d) AS (
      SELECT date(?1)
      UNION ALL SELECT date(d, '+1 day') FROM cal WHERE d < date(?2)
    )
    SELECT cal.d AS date,
           (SELECT COUNT(*) FROM queue_scrapes s
             WHERE date(s.snapshot_time) = cal.d) AS scrapes
      FROM cal ORDER BY cal.d", params = list(as.character(from), as.character(to)))
  days$scrapes <- as.integer(days$scrapes)
  days$observed <- days$scrapes > 0L
  days
}

#' Which folder a cransays archive row belongs in, in OUR vocabulary.
#'
#' The archive and our own scrape record the same fact differently. cransays
#' files a package sitting with a named CRAN reviewer as folder "human" with the
#' reviewer's initials in a separate subfolder column; our scrape has no
#' subfolder and records those initials as the folder itself. Imported verbatim,
#' six years of reviewer work would sit under one "human" bucket while our own
#' months carry the initials, and no query could span the join.
#'
#' So a reviewer row takes its identity from the subfolder, and everything else
#' keeps its folder. Three cases have to be excluded from that rule, all of them
#' present in the archive:
#'   * the subfolder repeats the folder name ("newbies"/"newbies"), an
#'     inconsistency the archive carries throughout;
#'   * the subfolder is absent, empty, the string "NA", or a bare "/";
#'   * the subfolder describes the CHECK rather than a person
#'     ("special/valgrind"), which our scrape does not record at all, so keeping
#'     the parent folder is what makes the two eras comparable.
#'
#' Vectorised: a whole snapshot goes through in one call.
cransays_folder <- function(folder, subfolder) {
  folder <- as.character(folder)
  subfolder <- as.character(subfolder)
  usable <- !is.na(subfolder) & nzchar(subfolder) & subfolder != "NA" &
    subfolder != "/" & subfolder != folder & !grepl("/", subfolder, fixed = TRUE)
  ifelse(folder == "human" & usable, subfolder, folder)
}

#' Roll the hourly snapshot stream up into one row per (day, folder).
#'
#' queue_history_daily is what the site's queue chart reads. It was seeded once
#' by scripts/import-history.R from r-hub/cransays and, until this existed,
#' nothing ever added to it, so the chart's last point stayed frozen on the day
#' of the last bootstrap while the snapshot stream ran on without it.
#'
#' Only the days the snapshot stream actually covers are rewritten. The backfill
#' reaches back to 2020 and we hold no snapshots for those years, so rebuilding
#' the whole table would throw them away.
#'
#' A day is counted from its LAST snapshot. Every run re-lists the entire queue,
#' so summing a day's snapshots would multiply the queue by the number of runs
#' that day. Rewriting the day in progress on every run is also what lets a
#' folder that has emptied since the morning drop back out of that day's counts,
#' which a plain upsert would leave behind.
#'
#' Returns the number of (day, folder) rows written.
roll_up_daily_history <- function(con) {
  folder_slots <- paste(rep("?", length(QUEUE_FOLDERS)), collapse = ", ")
  rows <- DBI::dbGetQuery(con, sprintf("
    WITH last_of_day AS (
      SELECT date(snapshot_time) AS day, MAX(snapshot_time) AS at
        FROM queue_snapshots
       GROUP BY date(snapshot_time)
    )
    SELECT l.day AS date,
           CASE WHEN s.folder IN (%s) THEN s.folder ELSE 'human' END AS folder,
           COUNT(DISTINCT s.package) AS package_count
      FROM queue_snapshots s
      JOIN last_of_day l ON s.snapshot_time = l.at
     GROUP BY 1, 2
     ORDER BY 1, 2", folder_slots), params = as.list(QUEUE_FOLDERS))

  if (nrow(rows) == 0L) return(0L)

  DBI::dbWithTransaction(con, {
    days <- unique(rows$date)
    DBI::dbExecute(con, sprintf(
      "DELETE FROM queue_history_daily WHERE date IN (%s)",
      paste(rep("?", length(days)), collapse = ", ")), params = as.list(days))
    DBI::dbWriteTable(con, "queue_history_daily", rows, append = TRUE)
  })

  nrow(rows)
}

#' Derive one row per submission, for whatever slice of the stream is admitted.
#'
#' queue_snapshots answers what the queue looked like at a MOMENT. Every question
#' about a SUBMISSION (how long did this package+version sit, which folder did it
#' end in) has to derive the submission list first, and that derivation is the
#' whole cost: scanning and grouping the 3.55M-row stream is 23.0s of a 23.8s
#' query that joins the result into CRAN's 162k version table. Reading the same
#' 82,048 submissions from this table is 0.10s, and it costs 9 MB, less than the
#' version table such a query is joined against.
#'
#' Three normalizations, each because the stream spells one fact several ways:
#'
#'   * version. A filename that does not parse into Package_Version.tar.gz has
#'     no version, and the stream has spelled that NULL, "" and "NA" at different
#'     times, so all three are handled. In the published database today every one
#'     of the 684 such rows is a SQL NULL; the other two spellings are carried
#'     for the older shapes rather than because they are currently present.
#'     All become "NA" here, so that (package, version) is a
#'     usable key: a SQLite NULL is never equal to itself in a UNIQUE index, so
#'     left as NULL those rows would grow a fresh duplicate on every hourly
#'     update. The cost is that two unparseable submissions of one package
#'     collapse into a single row; 684 of the stream's 3.55M rows, across 18
#'     packages, are unparseable at all.
#'   * submitted_at. CRAN's own submission time, absent on about a fifth of rows
#'     and spelled NULL, empty, or "NA". Compared as text "NA" sorts below every
#'     real timestamp and would become the submission time of any package it
#'     touched, so the absent spellings are dropped and the earliest surviving
#'     value wins. first_seen sits beside it deliberately: it is only when WE
#'     first saw the submission, and the consumer picks which start-of-life it
#'     wants rather than having one chosen here.
#'   * folder. The same collapse queue_history_daily uses, so a submission last
#'     seen with a named reviewer reports "human" instead of opening an outcome
#'     folder per reviewer. That collapse is not only reviewers: the stream also
#'     carries a few hundred rows filed under check variants (clang14, clang15),
#'     and those land in "human" as well rather than each becoming an outcome.
#'
#' first_seen and last_seen bound the sightings of a (package, version), which is
#' NOT the same as one stay in the queue. A resubmission of the same version
#' reuses the key, so the pair can span several separate stints: specmine.datasets
#' 0.0.2 runs 2021-02-17 to 2026-07-01 on 6 sightings, with 1,959 days of nothing
#' between two of them. 3,698 of the 82,048 submissions have a gap over 7 days
#' between consecutive sightings and 1,228 a gap over 30, so last_seen minus
#' first_seen is calendar distance and not time queued for about 4.5% of rows.
#' A consumer wanting residency has to split the sightings on the gaps, which
#' needs the stream; this table says how to find them, not how long they waited.
#'
#' n_observations counts SCRAPES rather than rows. A package genuinely sits in
#' two folders at once, a copy in waiting while another is in recheck, in 11,742
#' of the stream's package-scrapes; counting rows would report those twice as
#' time spent queued.
#'
#' last_folder is read from the last scrape that saw the submission, and is worth
#' being exact about rather than approximating: measured against CRAN's version
#' table downstream, submissions last seen in newbies never land 37% of the time
#' and those last seen in recheck 2%. For 473 of the 82,048 submissions that last
#' scrape holds two folders that survive the collapse, so no single answer exists
#' (474 differ before it: one submission's last scrape holds two reviewer
#' initials, which both become "human", so that one does have an answer); the
#' last row recorded
#' for that moment is taken, which is stable across rebuilds because rowids in
#' the append-only stream never move, and which invents no ranking between
#' folders that CRAN does not publish.
#'
#' Whether a submission reached CRAN is deliberately not a column here. This
#' repository holds no CRAN version data, and importing some to answer that would
#' put the join in the wrong place: package_version_history and cran_names_all
#' already live downstream, where that flag belongs.
#'
#' `where` restricts which snapshot rows are read. Both build paths share this
#' one query, so the incremental update is the full rebuild narrowed rather than
#' a second implementation that has to be kept agreeing with it.
submissions_query <- function(where = "1") {
  folder_slots <- paste(rep("?", length(QUEUE_FOLDERS)), collapse = ", ")
  sprintf("
    WITH observed AS (
      SELECT s.snapshot_time AS snapshot_time,
             s.package AS package,
             COALESCE(NULLIF(s.version, ''), 'NA') AS version,
             CASE WHEN s.folder IN (%s) THEN s.folder ELSE 'human' END AS folder,
             NULLIF(NULLIF(s.submitted_at, ''), 'NA') AS submitted_at,
             s.rowid AS rid
        FROM queue_snapshots s
       WHERE %s
    ),
    lifetime AS (
      SELECT package, version,
             MIN(snapshot_time) AS first_seen,
             MAX(snapshot_time) AS last_seen,
             MIN(submitted_at) AS submitted_at,
             COUNT(DISTINCT snapshot_time) AS n_observations
        FROM observed
       GROUP BY package, version
    ),
    final_folder AS (
      SELECT package, version, folder,
             ROW_NUMBER() OVER (PARTITION BY package, version
                                    ORDER BY snapshot_time DESC, rid DESC) AS seq
        FROM observed
    )
    INSERT INTO queue_submissions
      (package, version, first_seen, last_seen, submitted_at, last_folder,
       n_observations)
    SELECT l.package, l.version, l.first_seen, l.last_seen, l.submitted_at,
           f.folder, l.n_observations
      FROM lifetime l
      JOIN final_folder f
        ON f.package = l.package AND f.version = l.version AND f.seq = 1",
    folder_slots, where)
}

#' Rebuild every submission from the whole stream.
#'
#' 58s over the published database's 3.55M rows with the file already in page
#' cache and 76s without, and it grows with the stream, so this is not the hourly
#' path; see update_submissions().
#'
#' Returns the number of submissions written.
rebuild_submissions <- function(con) {
  # Create rather than assume, so this works on a database that has never held
  # the table. update.R runs the same DDL first, but the helper is also the
  # recovery path for any consumer building the table itself, and a rebuild that
  # only works when the table is already there is not much of a rebuild.
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS queue_submissions (
      package TEXT NOT NULL,
      version TEXT NOT NULL,
      first_seen TEXT NOT NULL,
      last_seen TEXT NOT NULL,
      submitted_at TEXT,
      last_folder TEXT NOT NULL,
      n_observations INTEGER NOT NULL,
      PRIMARY KEY (package, version)
    )")
  n <- DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, "DELETE FROM queue_submissions")
    DBI::dbExecute(con, submissions_query(), params = as.list(QUEUE_FOLDERS))
  })
  as.integer(n)
}

#' Account for one scrape's packages in the submissions table.
#'
#' Incremental, because the two sides are not close and this runs every hour: a
#' full rebuild reads all 3.55M rows for 58s at best, while accounting for one
#' scrape's packages is 1.5s, both measured against the published database.
#'
#' Neither number is constant, and the incremental one is not proportional to the
#' scrape either. A package that has sat in the queue for years is rescanned in
#' full whenever it turns up in a scrape, and three of them (fritools, climate,
#' factset.protobuf.stachextensions) carry about 28,000 snapshot rows each, which
#' is what most of that 1.5s is. Narrowing to the exact pairs the scrape names
#' rather than their packages saves 2% of the rows read and is not worth having
#' two definitions of what gets rewritten.
#'
#' It recomputes rather than upserts. The affected rows are deleted and derived
#' again from the stream by the same query the full rebuild uses, so first_seen,
#' n_observations and last_folder are never carried across runs and never
#' accumulate: re-running a scrape already accounted for lands on the same answer
#' instead of counting it twice, which is exactly what a counter kept in place
#' would get wrong. Every version of the packages in the scrape is recomputed,
#' not only the pairs the scrape names, which also repairs any row previously
#' written wrong.
#'
#' Two cases still need the whole stream, and both are settled by one query
#' against indexed minima:
#'   * the table is empty, which is the first run after this ships and any
#'     consumer building the table for the first time;
#'   * the stream now reaches further back than the table does, which is exactly
#'     what backfill-snapshots.R leaves behind when it loads six years of older
#'     scrapes; those rows belong to packages this scrape will never name, so a
#'     narrowed update would leave them with a first_seen from after the import.
#'
#' That second test is only sufficient because of how the one importer we have
#' behaves: backfill-snapshots.R keeps only rows strictly older than the earliest
#' snapshot already stored, so every batch it writes STRICTLY LOWERS the stream's
#' minimum and cannot help but be noticed. An importer that filled a hole in the
#' middle, or that tied the minimum rather than lowering it, would move no
#' minimum and its submissions would never be derived. Anything loading rows into
#' queue_snapshots by another route should call rebuild_submissions() itself
#' rather than assume this notices.
#'
#' A run that failed partway is not a third case. queue.db travels in the release
#' asset and only a completed run publishes one, so a scrape whose submissions
#' were never accounted for is discarded with the rest of that run rather than
#' becoming the next run's input.
#'
#' Returns the number of submissions written.
update_submissions <- function(con, snapshot_time = NULL) {
  # A database that has never held the table has nothing to narrow against, and
  # rebuild_submissions() creates it. Every sibling helper tolerates its table
  # being absent and a consumer calling this directly should not have to know
  # that update.R happens to run the CREATE first.
  if (!"queue_submissions" %in% DBI::dbListTables(con)) return(rebuild_submissions(con))
  reach <- DBI::dbGetQuery(con, "
    SELECT (SELECT COUNT(*) FROM queue_submissions) AS built,
           (SELECT MIN(first_seen) FROM queue_submissions) AS built_from,
           (SELECT MIN(snapshot_time) FROM queue_snapshots) AS stream_from")
  if (is.null(snapshot_time) || reach$built == 0L || is.na(reach$built_from) ||
      (!is.na(reach$stream_from) && reach$stream_from < reach$built_from)) {
    return(rebuild_submissions(con))
  }

  scrape_packages <- "SELECT package FROM queue_snapshots WHERE snapshot_time = ?"
  n <- DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, sprintf("DELETE FROM queue_submissions WHERE package IN (%s)",
                                scrape_packages),
                   params = list(as.character(snapshot_time)))
    DBI::dbExecute(con, submissions_query(sprintf("s.package IN (%s)", scrape_packages)),
                   params = c(as.list(QUEUE_FOLDERS), list(as.character(snapshot_time))))
  })
  as.integer(n)
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
  # queue_scrapes is the only record of a scrape that found nothing, and such a
  # row cannot be rebuilt from queue_snapshots, so its reach is worth declaring
  # alongside the others rather than left to be inferred.
  sc <- if ("queue_scrapes" %in% DBI::dbListTables(con)) {
    DBI::dbGetQuery(con, "SELECT COUNT(*) AS n, MIN(snapshot_time) AS lo,
                                 MAX(snapshot_time) AS hi FROM queue_scrapes")
  } else NULL
  # queue_submissions is derived from the stream rather than collected, but it is
  # what every question about a submission reads, and an update narrowed to the
  # wrong slice would leave it short without changing the stream it came from.
  # Declaring its reach is what lets the next run notice that.
  sub <- if ("queue_submissions" %in% DBI::dbListTables(con)) {
    DBI::dbGetQuery(con, "SELECT COUNT(*) AS n, MIN(first_seen) AS lo,
                                 MAX(last_seen) AS hi FROM queue_submissions")
  } else NULL

  out <- list(
    queue_snapshots     = list(rows = as.integer(s$n), min = s$lo, max = s$hi),
    queue_history_daily = list(rows = as.integer(h$n), dates = as.integer(h$d),
                               min = h$lo, max = h$hi)
  )
  if (!is.null(sc)) {
    out$queue_scrapes <- list(rows = as.integer(sc$n), min = sc$lo, max = sc$hi)
  }
  if (!is.null(sub)) {
    out$queue_submissions <- list(rows = as.integer(sub$n), min = sub$lo, max = sub$hi)
  }
  out
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

  scr_rows <- prior$coverage$queue_scrapes$rows
  if (usable(scr_rows) && usable(now$queue_scrapes$rows) &&
      now$queue_scrapes$rows < as.integer(scr_rows)) {
    out <- c(out, sprintf("queue_scrapes fell from %d rows to %d",
                          as.integer(scr_rows), now$queue_scrapes$rows))
  }

  sub_rows <- prior$coverage$queue_submissions$rows
  if (usable(sub_rows) && usable(now$queue_submissions$rows) &&
      now$queue_submissions$rows < as.integer(sub_rows)) {
    out <- c(out, sprintf("queue_submissions fell from %d rows to %d",
                          as.integer(sub_rows), now$queue_submissions$rows))
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
