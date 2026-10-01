# New releases carry queue.db.zst and manifest.json only. The plain queue.db
# was about eight times the compressed copy on every hourly release and nothing
# reads it: the merger and this pipeline's own prior download prefer the zst.
# Older releases keep both files, so readers must still accept either.

repo_lines <- function(...) readLines(file.path(.repo_root, ...))

# The words of the one `gh release create` command in update.yml.
release_create_words <- function() {
  lines <- repo_lines(".github", "workflows", "update.yml")
  start <- grep("gh release create", lines, fixed = TRUE)
  expect_equal(length(start), 1L)
  end <- start
  while (grepl("\\\\\\s*$", lines[end])) end <- end + 1L
  strsplit(trimws(gsub("\\\\\\s*$", "", paste(lines[start:end], collapse = " "))), "\\s+")[[1]]
}

test_that("a release uploads the compressed database and its manifest, not the plain file", {
  words <- release_create_words()

  expect_true("queue.db.zst" %in% words)
  expect_true("manifest.json" %in% words)
  expect_false("queue.db" %in% words)
})

test_that("a release still becomes Latest, which every reader resolves with no tag", {
  expect_true("--latest" %in% release_create_words())
})

test_that("the prior download prefers the zst and still accepts an older plain-only release", {
  lines <- repo_lines(".github", "workflows", "update.yml")
  zst <- grep("grep -qx 'queue.db.zst'", lines, fixed = TRUE)
  plain <- grep("grep -qx 'queue.db'", lines, fixed = TRUE)

  expect_equal(length(zst), 1L)
  expect_equal(length(plain), 1L)
  expect_lt(zst, plain)
})

test_that("only the uploaded asset is held to the release-asset cap", {
  dir <- tempfile("assets-"); dir.create(dir)
  zst <- file.path(dir, "queue.db.zst")
  writeBin(as.raw(1:10), zst)

  expect_equal(published_asset_sizes(zst), c("queue.db.zst" = 10))
})

test_that("every release's notes say how to get the database now", {
  note <- release_asset_note()

  expect_match(note, "queue.db.zst", fixed = TRUE)
  expect_match(note, "zstd -d queue.db.zst", fixed = TRUE)
  expect_match(note, "no longer attached", fixed = TRUE)
})

test_that("the README downloads the compressed database", {
  readme <- paste(repo_lines("README.md"), collapse = "\n")

  expect_false(grepl("/download/queue.db\"", readme, fixed = TRUE))
  expect_false(grepl("--pattern \"queue.db\"", readme, fixed = TRUE))
  expect_match(readme, "releases/latest/download/queue.db.zst", fixed = TRUE)
})
