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
