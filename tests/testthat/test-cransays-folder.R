# cransays records a package sitting with a named CRAN reviewer as folder
# "human" with the reviewer's initials in a separate subfolder column. Our own
# scrape has no subfolder: it records the reviewer's initials as the folder
# directly. Importing the archive without reconciling the two would put six
# years of reviewer work under a single "human" bucket while our own months keep
# the initials, and no query could span the join.

test_that("a reviewer folder takes its identity from the subfolder", {
  expect_equal(cransays_folder("human", "UL"), "UL")
  expect_equal(cransays_folder("human", "KL"), "KL")
  expect_equal(cransays_folder("human", "LH"), "LH")
})

test_that("an ordinary folder is used as-is", {
  expect_equal(cransays_folder("newbies", NA_character_), "newbies")
  expect_equal(cransays_folder("waiting", ""), "waiting")
  expect_equal(cransays_folder("pretest", "NA"), "pretest")
})

test_that("a subfolder that merely echoes its folder is not mistaken for a reviewer", {
  # The archive is inconsistent about this: the same folder appears both with an
  # empty subfolder and with the folder name repeated in it.
  expect_equal(cransays_folder("newbies", "newbies"), "newbies")
  expect_equal(cransays_folder("inspect", "inspect"), "inspect")
})

test_that("a human folder with no usable subfolder stays human", {
  # Rather than inventing a reviewer, fall back to the bucket the rollup would
  # have put it in anyway.
  expect_equal(cransays_folder("human", NA_character_), "human")
  expect_equal(cransays_folder("human", ""), "human")
  expect_equal(cransays_folder("human", "NA"), "human")
  expect_equal(cransays_folder("human", "/"), "human")
})

test_that("a special-check subfolder does not displace its parent folder", {
  # pretest/special/valgrind is a property of the check, not a reviewer, and our
  # own scrape does not record it at all. Keeping the parent folder is what makes
  # the two eras comparable.
  expect_equal(cransays_folder("pretest", "special/valgrind"), "pretest")
  expect_equal(cransays_folder("pretest", "special/donttest"), "pretest")
})

test_that("the mapping round-trips through the rollup's own normalization", {
  # This is the property that matters: whatever the era, a reviewer row must end
  # up in the same daily bucket. cransays "human"/"UL" and our own "UL" both have
  # to roll up as "human".
  norm <- function(f) if (f %in% QUEUE_FOLDERS) f else "human"

  expect_equal(norm(cransays_folder("human", "UL")), "human")
  expect_equal(norm("UL"), "human")
  expect_equal(norm(cransays_folder("newbies", NA_character_)), "newbies")
})

test_that("the mapping is vectorised over a whole snapshot", {
  expect_equal(
    cransays_folder(c("human", "newbies", "human", "pretest"),
                    c("UL", NA_character_, "", "special/valgrind")),
    c("UL", "newbies", "human", "pretest"))
})
