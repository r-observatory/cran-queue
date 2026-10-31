# CRAN Queue Monitor

Automated snapshots of the [CRAN incoming queue](https://cran.r-project.org/incoming/) taken every hour. Each snapshot records every package currently sitting in one of the incoming subfolders (inspect, pending, pretest, publish, recheck, waiting, etc.), capturing how long packages wait before appearing on CRAN.

The data is stored in a SQLite database (`queue.db`) and published in GitHub releases, split by year and compressed with zstd:

- Every hourly release carries `queue-current.db.zst`, this year's `queue_snapshots` rows plus every other table whole, beside a `manifest.json` that gives the size and sha256 of each file and lists the history parts.
- Each closed year of `queue_snapshots` is in its own release, `history-YYYY`, as `queue-YYYY.db.zst`. A year's part is written once, by the first run after the year ends, and never changes. Rows keep their ids from the full database.

So one year of snapshots never needs more than one download, and nothing re-uploads history that cannot change. Releases from before the split (until 2026-10-31) carry the whole history as one `queue.db.zst`, and older ones a plain `queue.db`. For every table of every pipeline in one file, use `observatory.db` from [r-observatory/data](https://github.com/r-observatory/data).

## Data Access

Each example downloads the latest release, then the history parts its manifest lists, and puts them together in `queue.db`. Skip the parts loop if you only need the current year.

### CLI

```bash
gh release download --repo r-observatory/cran-queue --pattern "queue-current.db.zst" --pattern "manifest.json"
zstd -d queue-current.db.zst -o queue.db
jq -r '.parts[] | "\(.tag) \(.asset)"' manifest.json | while read -r tag asset; do
  gh release download "$tag" --repo r-observatory/cran-queue --pattern "$asset"
  zstd -d -f "$asset" -o part.db
  sqlite3 queue.db "ATTACH 'part.db' AS p; INSERT INTO queue_snapshots SELECT * FROM p.queue_snapshots; DETACH p;"
done
```

### R

```r
base <- "https://github.com/r-observatory/cran-queue/releases"
download.file(file.path(base, "latest/download/queue-current.db.zst"), "queue-current.db.zst", mode = "wb")
download.file(file.path(base, "latest/download/manifest.json"), "manifest.json", mode = "wb")
system2("zstd", c("-d", "-f", "queue-current.db.zst", "-o", "queue.db"))  # needs the zstd command

library(RSQLite)
con <- dbConnect(SQLite(), "queue.db")
for (p in jsonlite::fromJSON("manifest.json", simplifyVector = FALSE)$parts) {
  download.file(file.path(base, "download", p$tag, p$asset), p$asset, mode = "wb")
  system2("zstd", c("-d", "-f", p$asset, "-o", "part.db"))
  dbExecute(con, "ATTACH 'part.db' AS p")
  dbExecute(con, "INSERT INTO queue_snapshots SELECT * FROM p.queue_snapshots")
  dbExecute(con, "DETACH p")
}
snapshots <- dbReadTable(con, "queue_snapshots")
stats <- dbReadTable(con, "queue_stats")
dbDisconnect(con)
```

### Python

```python
import json
import shutil
import sqlite3
import urllib.request
from compression import zstd  # Python 3.14+; on older versions use the zstandard package

base = "https://github.com/r-observatory/cran-queue/releases"

def fetch(url, compressed, out):
    urllib.request.urlretrieve(url, compressed)
    with zstd.open(compressed) as src, open(out, "wb") as dst:
        shutil.copyfileobj(src, dst)

fetch(f"{base}/latest/download/queue-current.db.zst", "queue-current.db.zst", "queue.db")
urllib.request.urlretrieve(f"{base}/latest/download/manifest.json", "manifest.json")

con = sqlite3.connect("queue.db")
for part in json.load(open("manifest.json"))["parts"]:
    fetch(f"{base}/download/{part['tag']}/{part['asset']}", part["asset"], "part.db")
    con.execute("ATTACH 'part.db' AS p")
    con.execute("INSERT INTO queue_snapshots SELECT * FROM p.queue_snapshots")
    con.commit()
    con.execute("DETACH p")
print(con.execute("SELECT COUNT(*), MIN(snapshot_time) FROM queue_snapshots").fetchone())
con.close()
```

## Schema

### `queue_snapshots`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Primary key (autoincrement) |
| `snapshot_time` | TEXT | UTC timestamp of the snapshot |
| `package` | TEXT | Package name |
| `version` | TEXT | Package version |
| `folder` | TEXT | Incoming subfolder (e.g., inspect, pending, pretest) |
| `submitted_at` | TEXT | Timestamp shown on the CRAN incoming page |

### `queue_stats`

| Column | Type | Description |
|---|---|---|
| `month` | TEXT | Year-month (e.g., 2026-03) |
| `folder` | TEXT | Incoming subfolder |
| `total_packages` | INTEGER | Total package entries observed that month in that folder |

### `queue_archive_episodes`

CRAN's `incoming/archive/` folder, read by the first run of each UTC day. The hourly scrape skips this folder. One row per archived file; a version uploaded more than once has one row per upload.

The folder records presence only and says nothing about why a file is there: its version may still be in the queue, a newer upload may follow, or the version may be published later. No outcome is stored; that is left to be derived downstream.

| Column | Type | Description |
|---|---|---|
| `package` | TEXT | Package name |
| `version` | TEXT | Package version, `NA` when the file name does not split into package and version |
| `mtime` | TEXT | Last-modified time as the listing shows it, `YYYY-MM-DD HH:MM` on CRAN's server clock (Europe/Vienna), the same clock as `submitted_at`. It is the upload time, not the time the file was archived |
| `size_kb` | REAL | Size in kilobytes as the listing rounds it (`2.6M` is 2662.4) |
| `first_seen` | TEXT | UTC time of the first read that listed the file |
| `last_seen` | TEXT | UTC time of the latest read that listed it |

A file was not in the listing at the read before its `first_seen`, so it appeared between those two reads, whose times are in `queue_archive_reads`. For files listed by the earliest read, `first_seen` is only when reading began: they may have been archived earlier.

### `queue_archive_reads`

| Column | Type | Description |
|---|---|---|
| `read_at` | TEXT | UTC time of the read, the snapshot time of the run that made it |
| `listed` | INTEGER | Files the listing held |

A file whose `last_seen` is older than the latest `read_at` has left the folder. A day with no read row was not read, so files that came and went that day are missing.

### `queue_folder_reads`

One row per folder per scrape. Scrapes before this table was added have no rows. A scrape with an `error` or `not_index` row lost that folder, so its count in `queue_scrapes` and the day's `queue_history_daily` row may be short.

| Column | Type | Description |
|---|---|---|
| `snapshot_time` | TEXT | UTC timestamp of the scrape, as in `queue_scrapes` |
| `folder` | TEXT | Incoming subfolder the scrape listed |
| `outcome` | TEXT | `ok`, `error` (the fetch failed) or `not_index` (the page was not that folder's index) |
| `listed` | INTEGER | Tarball rows read from the page, NULL on `error` |

## Update Schedule

The database is updated every hour via GitHub Actions. Each run scrapes the current state of the CRAN incoming queue and appends a new snapshot. The first run of each UTC day also reads `incoming/archive/` into `queue_archive_episodes`. The most recent GitHub release always carries the current year and a manifest naming the `history-YYYY` releases that hold the rest; each run downloads all of them and works on the full history. A `last-updated.txt` file in the repo tracks the last successful run time.

## License

The data is scraped from [CRAN](https://cran.r-project.org/incoming/), which is maintained by the R Foundation. This repository provides the scraping infrastructure and historical snapshots. Please respect CRAN's terms of use.

## Feedback

Found a bug, a wrong number, or a missing package? Report it at [r-observatory/feedback](https://github.com/r-observatory/feedback/issues/new/choose). All feedback about R Observatory, the site, the data, and the pipelines, is tracked in one place.
