# CRAN Queue Monitor

Automated snapshots of the [CRAN incoming queue](https://cran.r-project.org/incoming/) taken every hour. Each snapshot records every package currently sitting in one of the incoming subfolders (inspect, pending, pretest, publish, recheck, waiting, etc.), capturing how long packages wait before appearing on CRAN.

The data is stored in a SQLite database (`queue.db`) and published as a GitHub release.

## Data Access

### CLI

```bash
gh release download latest --repo r-observatory/cran-queue --pattern "queue.db"
```

### R

```r
url <- "https://github.com/r-observatory/cran-queue/releases/latest/download/queue.db"
download.file(url, "queue.db", mode = "wb")

library(RSQLite)
con <- dbConnect(SQLite(), "queue.db")
snapshots <- dbReadTable(con, "queue_snapshots")
stats <- dbReadTable(con, "queue_stats")
dbDisconnect(con)
```

### Python

```python
import urllib.request
import sqlite3

url = "https://github.com/r-observatory/cran-queue/releases/latest/download/queue.db"
urllib.request.urlretrieve(url, "queue.db")

con = sqlite3.connect("queue.db")
cur = con.cursor()
cur.execute("SELECT * FROM queue_snapshots LIMIT 10")
print(cur.fetchall())
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

The database is updated every hour via GitHub Actions. Each run scrapes the current state of the CRAN incoming queue and appends a new snapshot. The first run of each UTC day also reads `incoming/archive/` into `queue_archive_episodes`. The latest database is always available from the most recent GitHub release. A `last-updated.txt` file in the repo tracks the last successful run time.

## License

The data is scraped from [CRAN](https://cran.r-project.org/incoming/), which is maintained by the R Foundation. This repository provides the scraping infrastructure and historical snapshots. Please respect CRAN's terms of use.

## Feedback

Found a bug, a wrong number, or a missing package? Report it at [r-observatory/feedback](https://github.com/r-observatory/feedback/issues/new/choose). All feedback about R Observatory, the site, the data, and the pipelines, is tracked in one place.
