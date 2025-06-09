# ubctgdb

A lightweight helper for UBC Trading Group analysts to query MySQL data in Python with local disk-cache and secure credential handling.

---

## Features

* `run_sql()` returns a pandas DataFrame, optionally cached for 24 hours.
* Connection pooling via SQLAlchemy to reduce reconnect overhead.
* Diskcache for transparent on-disk caching keyed by the query hash.
* Credentials loaded from environment variables defined in a `.env` file.

---

## Requirements

* Python 3.9 – 3.12
* pandas ≥ 2.0
* SQLAlchemy ≥ 2.0
* diskcache ≥ 5.0
* tqdm ≥ 4.0
* mysqlclient ≥ 2.0

---

## Installation

1. **macOS users:** install the MySQL client libraries:

   ```bash
   brew install mysql
   ```

2. Install the package directly from GitHub:

   ```bash
   pip install git+https://github.com/UBC-Trading-Group/ubctgdb.git
   ```

---

## Configuration

Create a `.env` file in the same directory as your scripts or notebooks:

```dotenv
DB_USER=my_username
DB_PASS=super_secret_password
DB_HOST=your_database_host
DB_NAME=your_database_name
```

The library loads these variables automatically.

---

## Quick Start

```python
import ubctgdb as db

# Example 1: grab everything
sql_all = '''
SELECT *
FROM Consumer_Sentiment;
'''
cs_all = db.run_sql(sql_all)
print(cs_all.head())

# Example 2: date-bounded query
start, end = '2020-01-01', '2021-12-31'
sql_window = f'''
SELECT date, umcsent
FROM Consumer_Sentiment
WHERE date BETWEEN '{start}' AND '{end}'
ORDER BY date;
'''
cs_window = db.run_sql(sql_window)
print(cs_window.tail())

# Example 3: force a fresh pull (bypass cache)
cs_fresh = db.run_sql(sql_window, refresh=True)
print(cs_fresh.tail())
```

* Use `refresh=True` to bypass the cache and pull fresh data.
* Adjust `chunksize` if you need to stream large result sets in smaller batches.

Cached results are stored in `~/.db_cache/` for 24 hours.
