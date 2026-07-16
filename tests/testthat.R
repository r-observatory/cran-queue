library(testthat)
source(file.path(getwd(), "scripts", "helpers.R"))
test_dir("tests/testthat", stop_on_failure = TRUE)
