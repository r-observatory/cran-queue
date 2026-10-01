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
