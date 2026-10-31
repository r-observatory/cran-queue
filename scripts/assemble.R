#!/usr/bin/env Rscript
# Put a yearly release back together before a run builds on it:
#   Rscript scripts/assemble.R queue.db manifest.json parts/
# queue.db is that release's queue-current.db.zst, expanded; parts/ holds every
# queue-YYYY.db.zst its manifest lists. The result has to match the manifest's
# coverage exactly, because update.R then holds the run to that same coverage
# and a history assembled short would pass for one the run had lost.

suppressPackageStartupMessages(library(RSQLite))

.script_dir <- tryCatch({
  a <- commandArgs(FALSE)
  f <- sub("^--file=", "", grep("^--file=", a, value = TRUE))
  if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f)) else "scripts"
}, error = function(e) "scripts")
source(file.path(.script_dir, "helpers.R"))

args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 3L)
db_path <- args[[1]]
manifest <- jsonlite::fromJSON(args[[2]], simplifyVector = FALSE)
parts_dir <- args[[3]]

inserted <- assemble_parts(db_path, manifest, parts_dir)

got <- queue_coverage(db_path)$queue_snapshots
want <- manifest$coverage$queue_snapshots
if (is.null(want) || got$rows != as.integer(want$rows) ||
    !identical(got$min, want$min) || !identical(got$max, want$max)) {
  stop(sprintf("assembled history does not match the manifest: %d snapshots %s to %s, manifest says %s %s to %s",
               got$rows, got$min, got$max, want$rows %||% "?", want$min %||% "?", want$max %||% "?"))
}
cat(sprintf("Assembled %d history parts (%d snapshots): %d snapshots from %s to %s\n",
            length(manifest$parts), inserted, got$rows, got$min, got$max))
