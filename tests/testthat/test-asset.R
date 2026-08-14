# The database is published as a release asset, and GitHub hard-caps an asset at
# 2 GiB. Hitting that cap would not degrade gracefully: the upload fails, the run
# fails, and because the next run starts from the last release that DID publish,
# the pipeline would keep collecting and keep failing to publish, quietly, until
# somebody noticed. Compressing the asset is what keeps that far away without
# ever having to drop history to stay under it.

test_that("the compressed asset is described by its own bytes and hash", {
  db <- file.path(tempfile("asset-"), "queue.db")
  dir.create(dirname(db))
  writeLines(strrep("queue data ", 500), db)
  zst <- compress_asset(db)

  core <- compressed_asset_core(zst)

  expect_equal(core$asset_filename, "queue.db.zst")
  expect_equal(core$asset_bytes, file.size(zst))
  expect_equal(core$asset_sha256, file_sha256(zst))
  expect_true(core$asset_bytes < file.size(db))
})

test_that("compressing produces a file that restores to the original bytes", {
  # A compressed asset nobody can decompress back to the exact database is worse
  # than no compression, so the round trip is checked rather than assumed.
  db <- file.path(tempfile("asset-"), "queue.db")
  dir.create(dirname(db))
  writeLines(strrep("queue data ", 500), db)
  before <- file_sha256(db)

  zst <- compress_asset(db)
  restored <- file.path(dirname(db), "restored.db")
  system2("zstd", c("-dq", "-f", shQuote(zst), "-o", shQuote(restored)))

  expect_equal(file_sha256(restored), before)
})

test_that("an asset within the cap raises nothing", {
  expect_equal(asset_size_violations(c(small = 1000), max_bytes = 2000, warn_at = 0.8),
               character(0))
})

test_that("an asset over the cap is refused before anything is published", {
  bad <- asset_size_violations(c("queue.db.zst" = 2500), max_bytes = 2000, warn_at = 0.8)

  expect_true(length(bad) > 0)
  expect_match(paste(bad, collapse = " "), "queue.db.zst")
  expect_match(paste(bad, collapse = " "), "2500")
})

test_that("an asset approaching the cap warns while there is still time to act", {
  # The point of the warning is lead time. A guard that only fires AT the cap
  # tells you on the day the pipeline stops, which is too late to plan a change
  # to the asset layout.
  near <- asset_size_violations(c("queue.db" = 1700), max_bytes = 2000, warn_at = 0.8)

  expect_equal(near, character(0))
  expect_match(paste(asset_size_warnings(c("queue.db" = 1700), max_bytes = 2000, warn_at = 0.8),
                     collapse = " "),
               "85")
})

test_that("every published asset is checked, not just the largest", {
  bad <- asset_size_violations(c("queue.db" = 500, "queue.db.zst" = 2500),
                               max_bytes = 2000, warn_at = 0.8)

  expect_equal(length(bad), 1L)
  expect_match(bad, "queue.db.zst")
})
