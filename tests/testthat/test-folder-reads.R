# A scrape that lost a folder used to look complete: the fetch error was only
# printed, queue_scrapes stored the partial count, and the daily rollup took it
# as the day's queue. queue_folder_reads records, for every scrape, which
# folders were listed and whether each one was read.

publish_html <- function() {
  paste(readLines(test_path("fixtures", "publish-listing.html")), collapse = "\n")
}

# The real publish page with its tarball row removed and retitled, which is
# what an empty folder's index looks like.
empty_index <- function(folder) {
  lines <- readLines(test_path("fixtures", "publish-listing.html"))
  lines <- lines[!grepl("compressed.gif", lines, fixed = TRUE)]
  gsub("/incoming/publish", paste0("/incoming/", folder), paste(lines, collapse = "\n"), fixed = TRUE)
}

fake_incoming <- function(url) {
  switch(sub("^https://x/", "", url),
    "inspect/" = empty_index("inspect"),
    "publish/" = publish_html(),
    "pretest/" = stop("HTTP error 503"),
    "SU/"      = "<html><title>Service Unavailable</title></html>")
}

folder_db <- function() {
  path <- file.path(tempfile("folder-db-"), "queue.db")
  dir.create(dirname(path))
  DBI::dbConnect(RSQLite::SQLite(), path)
}

folder_reads <- function(con) {
  DBI::dbGetQuery(con, "SELECT snapshot_time, folder, outcome, listed
                          FROM queue_folder_reads ORDER BY snapshot_time, folder")
}

test_that("every folder is read, and each read says whether it succeeded", {
  expect_output(got <- scrape_folders(c("inspect", "publish", "pretest", "SU"),
                                      fake_incoming, "https://x/"),
                "Error scraping folder pretest")

  expect_equal(got$reads$folder, c("inspect", "publish", "pretest", "SU"))
  expect_equal(got$reads$outcome, c("ok", "ok", "error", "not_index"))
  expect_equal(got$reads$listed, c(0L, 1L, NA, 0L))
})

test_that("the snapshot rows are what the scrape collected before", {
  expect_output(got <- scrape_folders(c("inspect", "publish", "pretest"), fake_incoming, "https://x/"))

  expect_equal(length(got$entries), 1L)
  expect_equal(got$entries[[1]],
               data.frame(package = "igraph", version = "2.3.4", folder = "publish",
                          submitted_at = "2026-09-28 20:48", stringsAsFactors = FALSE))
})

test_that("a scrape records one row per folder, keyed by its snapshot time", {
  con <- folder_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  reads <- data.frame(folder = c("inspect", "pretest"), outcome = c("ok", "error"),
                      listed = c(3L, NA), stringsAsFactors = FALSE)

  record_folder_reads(con, "2026-10-01 00:12:00", reads)

  expect_equal(folder_reads(con),
               data.frame(snapshot_time = "2026-10-01 00:12:00", folder = c("inspect", "pretest"),
                          outcome = c("ok", "error"), listed = c(3L, NA),
                          stringsAsFactors = FALSE))
})

test_that("recording a scrape again replaces its rows rather than duplicating them", {
  con <- folder_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  reads <- data.frame(folder = "inspect", outcome = "ok", listed = 3L, stringsAsFactors = FALSE)

  record_folder_reads(con, "2026-10-01 00:12:00", reads)
  reads$listed <- 4L
  record_folder_reads(con, "2026-10-01 00:12:00", reads)

  expect_equal(folder_reads(con)$listed, 4L)
})

test_that("an outcome outside the three is refused by the table", {
  con <- folder_db(); on.exit(DBI::dbDisconnect(con), add = TRUE)
  bad <- data.frame(folder = "inspect", outcome = "maybe", listed = 1L, stringsAsFactors = FALSE)

  expect_error(record_folder_reads(con, "2026-10-01 00:12:00", bad), "CHECK constraint")
})

test_that("the notes name the folders that were not read, and say nothing when all were", {
  reads <- data.frame(folder = c("inspect", "pretest", "SU"),
                      outcome = c("ok", "error", "not_index"), listed = c(0L, NA, 0L),
                      stringsAsFactors = FALSE)

  expect_equal(folder_reads_notes_line(reads),
               "**Folders not read:** pretest (error), SU (not_index)\n\n")
  expect_equal(folder_reads_notes_line(reads[1, ]), "")
})
