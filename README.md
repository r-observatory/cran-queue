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

## Update Schedule

The database is updated every hour via GitHub Actions. Each run scrapes the current state of the CRAN incoming queue and appends a new snapshot. The latest database is always available from the most recent GitHub release. A `last-updated.txt` file in the repo tracks the last successful run time.

## License

The data is scraped from [CRAN](https://cran.r-project.org/incoming/), which is maintained by the R Foundation. This repository provides the scraping infrastructure and historical snapshots. Please respect CRAN's terms of use.
