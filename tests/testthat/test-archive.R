# CRAN's incoming/archive/ holds about four weeks of uploads moved out of the
# queue. It is read once a UTC day into its own tables, never by the hourly
# scrape. The folder records presence only: a file there may still have its
# version in the queue, may be followed by a newer upload, or may be published.

test_that("the hourly scrape still skips archive, the parent link and absolute paths", {
  html <- paste(
    '<tr><td valign="top"><img src="/icons/back.gif" alt="[PARENTDIR]"></td><td><a href="/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>',
    '<tr><td valign="top"><img src="/icons/folder.gif" alt="[DIR]"></td><td><a href="SU/">SU/</a></td><td align="right">2026-09-24 13:07  </td><td align="right">  - </td><td>&nbsp;</td></tr>',
    '<tr><td valign="top"><img src="/icons/folder.gif" alt="[DIR]"></td><td><a href="archive/">archive/</a></td><td align="right">2026-09-30 17:33  </td><td align="right">  - </td><td>&nbsp;</td></tr>',
    '<tr><td valign="top"><img src="/icons/folder.gif" alt="[DIR]"></td><td><a href="inspect/">inspect/</a></td><td align="right">2026-09-30 17:28  </td><td align="right">  - </td><td>&nbsp;</td></tr>',
    '<tr><td><a href="/incoming/">Parent Directory</a></td></tr>',
    '<tr><td><a href="../">..</a></td></tr>',
    sep = "\n")

  expect_equal(parse_folders(html), c("SU", "inspect"))
})

archive_html <- function() {
  paste(readLines(test_path("fixtures", "archive-listing.html")), collapse = "\n")
}

test_that("an archive listing parses to package, version, mtime and size", {
  got <- parse_archive_listing(archive_html())

  expect_equal(got$package, c("ASW", "AlphaSDM", "BoxDensityPlot"))
  expect_equal(got$version, c("1.1.1", "0.2.0", "0.1.0"))
  expect_equal(got$mtime, c("2026-09-18 07:48", "2026-09-23 00:42", "2026-09-15 09:39"))
  expect_equal(got$size_kb, c(20, 2662.4, 8.1))
})

test_that("a file name that does not split keeps the key whole with version 'NA'", {
  html <- gsub("ASW_1.1.1.tar.gz", "ASW.tar.gz", archive_html(), fixed = TRUE)

  got <- parse_archive_listing(html)

  expect_equal(got$package[1], "ASW")
  expect_equal(got$version[1], "NA")
})

test_that("a page that is not the archive index is a failed read, not an empty folder", {
  expect_null(parse_archive_listing(
    "<html><head><title>503 Service Unavailable</title></head><body></body></html>"))
  # Another folder's index, as a redirect would serve it.
  expect_null(parse_archive_listing(
    gsub("/incoming/archive", "/incoming/pretest", archive_html(), fixed = TRUE)))
})

test_that("the archive index with no tarball rows is an empty folder", {
  lines <- readLines(test_path("fixtures", "archive-listing.html"))
  html <- paste(lines[!grepl("compressed.gif", lines, fixed = TRUE)], collapse = "\n")

  got <- parse_archive_listing(html)

  expect_equal(nrow(got), 0L)
  expect_equal(names(got), c("package", "version", "mtime", "size_kb"))
})

archive_db <- function() {
  path <- file.path(tempfile("archive-db-"), "queue.db")
  dir.create(dirname(path))
  DBI::dbConnect(RSQLite::SQLite(), path)
}

listing <- function(package, version = "1.0", mtime = "2026-09-20 10:00", size_kb = 20) {
  data.frame(package = package, version = version, mtime = mtime, size_kb = size_kb,
             stringsAsFactors = FALSE)
}

episodes <- function(con) {
  DBI::dbGetQuery(con, "SELECT package, version, mtime, size_kb, first_seen, last_seen
                          FROM queue_archive_episodes ORDER BY package, version, mtime")
}

reads <- function(con) {
  DBI::dbGetQuery(con, "SELECT read_at, listed FROM queue_archive_reads ORDER BY read_at")
}

test_that("the first read opens every listed file with first_seen = last_seen = read_at", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  new <- record_archive_read(con, listing(c("Aaa", "Bbb")), "2026-10-01 00:12:00")

  expect_equal(new, 2L)
  expect_equal(episodes(con)$first_seen, rep("2026-10-01 00:12:00", 2))
  expect_equal(episodes(con)$last_seen, rep("2026-10-01 00:12:00", 2))
  expect_equal(reads(con), data.frame(read_at = "2026-10-01 00:12:00", listed = 2L,
                                      stringsAsFactors = FALSE))
})

test_that("a later read moves last_seen only, and a file gone from it keeps its last_seen", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  record_archive_read(con, listing(c("Aaa", "Bbb")), "2026-10-01 00:12:00")

  new <- record_archive_read(con, listing("Aaa", size_kb = 99), "2026-10-02 01:05:00")

  expect_equal(new, 0L)
  got <- episodes(con)
  expect_equal(got$first_seen, rep("2026-10-01 00:12:00", 2))
  expect_equal(got$last_seen, c("2026-10-02 01:05:00", "2026-10-01 00:12:00"))
  expect_equal(got$size_kb, c(20, 20))
  expect_equal(nrow(reads(con)), 2L)
})

test_that("the same version uploaded again at another minute is its own episode", {
  # AlphaSDM 0.2.0 was archived from its 00:42 upload while its 13:02 upload sat in newbies.
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  record_archive_read(con, listing("AlphaSDM", "0.2.0", "2026-09-23 00:42"), "2026-10-01 00:12:00")

  new <- record_archive_read(con, listing("AlphaSDM", "0.2.0", c("2026-09-23 00:42", "2026-09-23 13:02")),
                             "2026-10-02 01:05:00")

  expect_equal(new, 1L)
  expect_equal(episodes(con)$mtime, c("2026-09-23 00:42", "2026-09-23 13:02"))
  expect_equal(episodes(con)$first_seen, c("2026-10-01 00:12:00", "2026-10-02 01:05:00"))
})

test_that("an empty listing after a non-empty read writes nothing", {
  # A changed page layout parses to zero rows under a valid title; recording it
  # would leave every open episode looking as if it left the folder at once.
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  record_archive_read(con, listing("Aaa"), "2026-10-01 00:12:00")

  expect_output(new <- record_archive_read(con, listing("Aaa")[0, ], "2026-10-02 01:05:00"),
                "::warning::")

  expect_true(is.na(new))
  expect_equal(nrow(reads(con)), 1L)
  expect_equal(episodes(con)$last_seen, "2026-10-01 00:12:00")
})

test_that("an empty first listing is recorded as a read of zero", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_equal(record_archive_read(con, listing("Aaa")[0, ], "2026-10-01 00:12:00"), 0L)
  expect_equal(reads(con)$listed, 0L)
  expect_equal(nrow(episodes(con)), 0L)
})

test_that("the read row and the episodes are written in one transaction", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  ensure_archive_tables(con)
  DBI::dbExecute(con, "CREATE TRIGGER qae_fail BEFORE INSERT ON queue_archive_episodes
                         BEGIN SELECT RAISE(ABORT, 'episode insert failed'); END")

  expect_error(record_archive_read(con, listing("Aaa"), "2026-10-01 00:12:00"),
               "episode insert failed")

  expect_equal(nrow(reads(con)), 0L)
})

test_that("recording the same read twice changes nothing, and an older read never moves last_seen back", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  record_archive_read(con, listing("Aaa"), "2026-10-02 01:05:00")

  expect_equal(record_archive_read(con, listing("Aaa"), "2026-10-02 01:05:00"), 0L)
  record_archive_read(con, listing("Aaa"), "2026-10-01 00:12:00")

  expect_equal(nrow(episodes(con)), 1L)
  expect_equal(episodes(con)$last_seen, "2026-10-02 01:05:00")
  expect_equal(reads(con)$read_at, c("2026-10-01 00:12:00", "2026-10-02 01:05:00"))
})

test_that("a read is due once per UTC date of the run's snapshot time", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_true(archive_read_due(con, "2026-10-01 00:12:00"))
  record_archive_read(con, listing("Aaa"), "2026-10-01 00:12:00")

  expect_false(archive_read_due(con, "2026-10-01 23:59:59"))
  expect_true(archive_read_due(con, "2026-10-02 00:00:05"))
})

test_that("a read late in the day does not satisfy the run just after midnight UTC", {
  con <- archive_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  record_archive_read(con, listing("Aaa"), "2026-10-01 23:59:59")

  expect_true(archive_read_due(con, "2026-10-02 00:00:01"))
})
