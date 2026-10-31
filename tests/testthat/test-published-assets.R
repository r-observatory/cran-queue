# Every hourly release carries queue-current.db.zst and manifest.json; each
# closed year of queue_snapshots is published once, in its own history-YYYY
# release. Releases from before that split carry the whole database as
# queue.db.zst, and older ones a plain queue.db, so the prior download must
# still accept either.

repo_lines <- function(...) readLines(file.path(.repo_root, ...))

# The words of the `gh release create` command in scripts/publish.sh that
# starts with `start`, joining its continuation lines.
release_create_words <- function(start) {
  lines <- repo_lines("scripts", "publish.sh")
  at <- grep(start, lines, fixed = TRUE)
  expect_equal(length(at), 1L)
  end <- at
  while (grepl("\\\\\\s*$", lines[end])) end <- end + 1L
  strsplit(trimws(gsub("\\\\\\s*$", "", paste(lines[at:end], collapse = " "))), "\\s+")[[1]]
}

test_that("the workflow publishes through scripts/publish.sh and downloads through download-prior.sh", {
  lines <- repo_lines(".github", "workflows", "update.yml")

  expect_true(any(grepl("run: bash scripts/publish.sh", lines, fixed = TRUE)))
  expect_true(any(grepl("run: bash scripts/download-prior.sh", lines, fixed = TRUE)))
  expect_false(any(grepl("gh release create", lines, fixed = TRUE)))
})

test_that("an hourly release uploads the current part and its manifest only", {
  words <- release_create_words('gh release create "$TAG"')

  expect_true("queue-current.db.zst" %in% words)
  expect_true("manifest.json" %in% words)
  expect_false(any(c("queue.db", "queue.db.zst") %in% words))
})

test_that("the hourly release still becomes Latest, which every reader resolves with no tag", {
  expect_true("--latest" %in% release_create_words('gh release create "$TAG"'))
})

test_that("a history release can never become Latest", {
  words <- release_create_words('gh release create "$ptag"')

  expect_true("--prerelease" %in% words)
  expect_true("--latest=false" %in% words)
})

test_that("the prior download prefers the yearly layout and still accepts the older ones", {
  lines <- repo_lines("scripts", "download-prior.sh")
  current <- grep("has 'queue-current.db.zst'", lines, fixed = TRUE)
  zst <- grep("has 'queue.db.zst'", lines, fixed = TRUE)
  plain <- grep("has 'queue.db'", lines, fixed = TRUE)

  expect_equal(lengths(list(current, zst, plain)), c(1L, 1L, 1L))
  expect_lt(current, zst)
  expect_lt(zst, plain)
})

test_that("only the uploaded assets are held to the release-asset cap", {
  dir <- tempfile("assets-"); dir.create(dir)
  current <- file.path(dir, "queue-current.db.zst")
  part <- file.path(dir, "queue-2025.db.zst")
  writeBin(as.raw(1:10), current)
  writeBin(as.raw(1:4), part)

  expect_equal(published_asset_sizes(c(current, part)),
               c("queue-current.db.zst" = 10, "queue-2025.db.zst" = 4))
})

test_that("every release's notes say how to get the database now", {
  note <- release_asset_note()

  expect_match(note, "queue-current.db.zst", fixed = TRUE)
  expect_match(note, "history-YYYY", fixed = TRUE)
  expect_match(note, "zstd -d queue-current.db.zst", fixed = TRUE)
})

test_that("the README downloads the current part and puts the history back together", {
  readme <- paste(repo_lines("README.md"), collapse = "\n")

  expect_false(grepl("/download/queue.db\"", readme, fixed = TRUE))
  expect_false(grepl("releases/latest/download/queue.db.zst", readme, fixed = TRUE))
  expect_match(readme, "latest/download/queue-current.db.zst", fixed = TRUE)
  expect_match(readme, "INSERT INTO queue_snapshots SELECT * FROM p.queue_snapshots", fixed = TRUE)
})
