# ubctgdb

## Features

| API | What it does |
|-----|--------------|
| `run_sql()` | Query MySQL and return a `pandas.DataFrame`, with transparent 24 h caching. |
| `upload_csv()` | One-shot bulk load of a (potentially very large) CSV via **MySQL Shell parallel importer**. |
| `upload_dataframe()` | Same as `upload_csv`, but starts from a `pandas` DataFrame. |
| `append_csv()` / `append_dataframe()` | Efficient *append-only* (or “upsert”) loader—skips rows already in the destination table. |

Additional niceties:

* Connection pooling via **SQLAlchemy** to avoid reconnect overhead.
* **DiskCache** on-disk DataFrame cache keyed by query hash.
* Shared *null* convention: the sentinel string `\N` becomes Python `NaN`/**`pd.NA`** and SQL `NULL` automatically.

---

## Requirements

* Python ≥ 3.9 (3.9 – 3.12 tested)
* Core libraries  
  `pandas ≥ 2.0`, `SQLAlchemy ≥ 2.0`, `diskcache ≥ 5.0`, `python-dotenv ≥ 1.0`
* Bulk-load extras  
  `pyarrow ≥ 10.0`, `mysqlclient ≥ 2.0` **or** `PyMySQL ≥ 1.0`, `tqdm ≥ 4.0`
* **MySQL Shell ≥ 8.0** available on your `$PATH` for fast imports

---

## Installation

```bash
# 1️⃣  macOS / Linux — make sure MySQL client libs are present
# macOS example:
brew install mysql

# 2️⃣  Install the package (PyPI or GitHub)
pip install git+https://github.com/UBC-Trading-Group/ubctgdb.git
#  – or –
pip install ubctgdb        # when published on PyPI
````

---

## Configuration

Create a `.env` file alongside your scripts or notebooks:

```dotenv
DB_USER=my_username
DB_PASS=super_secret_password
DB_HOST=db.example.com
DB_NAME=ubctg
```

---

## Quick Start — Queries

```python
import ubctgdb as db

# Grab an entire table (cached 24 h)
df_all = db.run_sql("SELECT * FROM consumer_sentiment;")
print(df_all.head())

# Parameterised date range
start, end = "2020-01-01", "2021-12-31"
sql = f"""
SELECT date, umcsent
FROM consumer_sentiment
WHERE date BETWEEN '{start}' AND '{end}'
ORDER BY date;
"""
df_win = db.run_sql(sql)
print(df_win.tail())

# Force a fresh pull (bypass cache)
df_fresh = db.run_sql(sql, refresh=True)
```

---

## Bulk CSV Import

```python
from ubctgdb import upload_csv

upload_csv(
    csv_path      = "/path/to/all_daily.csv",
    table         = "all_daily_prices",
    schema        = "ubctg",         # defaults to DB_NAME if omitted
    header        = None,            # None → auto-detect, True/False to force
    replace_table = True,            # drop & recreate table
    threads       = 12,              # mysqlsh parallel threads
)
```

*Missing*, *empty*, or the strings `NaN`, `NULL`, `na`, `n/a` are normalised to `NULL` on the MySQL side.

---

## DataFrame Import

```python
import pandas as pd
from ubctgdb import upload_dataframe

df = pd.read_csv("clean_prices.csv")
upload_dataframe(
    df,
    table         = "clean_prices",
    replace_table = False,   # CREATE TABLE IF NOT EXISTS …
)
```

Internally, the function writes the DataFrame to a temp CSV and re-uses the same
high-performance MySQL Shell importer as `upload_csv()`.

---

## Incremental “Append-Only” Updates

### 1.  From a CSV file

```python
from ubctgdb import append_csv

append_csv(
    csv_path = "daily_2025-07-06.csv",
    table    = "all_daily_prices",
    key_cols = ["gvkey", "datadate"],     # composite primary key in MySQL
    mode     = "staging",                 # default: bulk-load → INSERT IGNORE
)
```

### 2.  From a DataFrame

```python
from ubctgdb import append_dataframe
append_dataframe(
    df,
    table    = "all_daily_prices",
    key_cols = ["gvkey", "datadate"],
)
```

* `mode="staging"` (default) uses a temporary staging table + `INSERT IGNORE`
  → safest for overlapping data.
* `mode="watermark"` skips rows older than the current `MAX(date)`—ideal for
  strictly append-only log/price feeds.

