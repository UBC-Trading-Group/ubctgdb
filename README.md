# ubctgdb

Shared pandas tables in a private **Cloudflare R2** bucket. One Parquet file per table;
no database server, website or version history.

**[Run the example notebook](examples/quickstart.ipynb)** to try every command.

## Install

Python 3.9+, from a local checkout:

```bash
pip install -e .
```

After the branch is pushed, install directly with:

```bash
pip install git+https://github.com/UBC-Trading-Group/ubctgdb.git@codex/r2-storage
```

## Configure

Put these four settings in `.env` in your notebook's working directory or a parent:

```dotenv
R2_ENDPOINT_URL=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET=ubctg-data
```

| Setting | Purpose |
|---|---|
| `R2_ENDPOINT_URL` | Your account's R2 S3 endpoint; use the endpoint Cloudflare provides |
| `R2_ACCESS_KEY_ID` | Identifies your R2 S3 credential |
| `R2_SECRET_ACCESS_KEY` | Authenticates requests; keep secret |
| `R2_BUCKET` | Bucket containing the tables |

**Account API tokens work.** In R2, create an **Account API token** and copy the
resulting **Access Key ID** and **Secret Access Key** into the settings above.
Account tokens belong to the account rather than an individual user. Only a Super
Administrator can create them. See [Cloudflare's instructions](https://developers.cloudflare.com/r2/api/tokens/).

Choose **Object Read only** for readers or **Object Read & Write** for publishers,
scoped to `ubctg-data`. The bucket stays private. A standalone bearer-token value
does not replace the access-key pair in this package.

You do **not** need `token`, `DB_HOST`, `DB_NAME`, `DB_USER` or `DB_PASS` for this version.
Existing `endpoint`, `access_key` and `secret` names also work; use either these or
the `R2_*` names, not both (`R2_*` takes precedence). Existing environment variables
take precedence over `.env`; restart the kernel after changing loaded settings.
Never commit `.env` or put secrets in a notebook.

## Command examples

Run these examples in order. They create two tiny demo tables, separate from your
research data. Outputs below are illustrative: your unique names, dates, file sizes
and some displayed type names may differ.

```python
import tempfile
from pathlib import Path
from uuid import uuid4
import pandas as pd
import ubctgdb as db

# Unique names keep this walkthrough separate from the club's real datasets.
group = "example/" + uuid4().hex[:12]
table = group + "/prices"
copy_table = group + "/prices_copy"
local_files = tempfile.TemporaryDirectory(prefix="ubctgdb-example-")
folder = Path(local_files.name)
prices = pd.DataFrame({"symbol": ["AAA", "BBB"], "price": [10.5, 20.0]})
print(prices.to_string(index=False))
```

### `upload_dataframe()`

Create a shared table from pandas. Returns a summary dictionary.

```python
receipt = db.upload_dataframe(prices, table=table, description="Example prices")
print({key: receipt[key] for key in ["rows", "columns", "description"]})
```

Example output:

```text
{'rows': 2, 'columns': 2, 'description': 'Example prices'}
```

### `list_tables()`

List tables, newest additions first. This example filters to your unique demo group.

```python
tables = db.list_tables(search=group)
print(tables[["table", "rows", "columns", "description"]].to_string(index=False))
```

Example output:

```text
               table  rows  columns    description
example/demo/prices      2        2 Example prices
```

### `describe()`

Inspect summary information and column types without downloading the whole file.

```python
info = db.describe(table)
print({key: info[key] for key in ["rows", "columns", "schema"]})
```

Example output:

```text
{'rows': 2, 'columns': 2, 'schema': {'symbol': 'string', 'price': 'double'}}
```

### `preview()`

Display the first rows using Parquet range reads.

```python
sample = db.preview(table, rows=2)
print(sample.to_string(index=False))
```

Example output:

```text
symbol  price
   AAA   10.5
   BBB   20.0
```

### `read_table()`

Download the current table into pandas. You can select columns to load into memory.

```python
df = db.read_table(table)
print(df.to_string(index=False))
print("Selected columns:")
print(db.read_table(table, columns=["symbol"]).to_string(index=False))
```

Example output:

```text
symbol  price
   AAA   10.5
   BBB   20.0
Selected columns:
symbol
   AAA
   BBB
```

### `download_table()`

Save Parquet locally without loading all rows into pandas. Returns a Path.

```python
path = db.download_table(table, folder / "prices.parquet", overwrite=True)
print(path.name, "exists:", path.exists())
```

Example output:

```text
prices.parquet exists: True
```

### `upload_parquet()`

Publish an existing Parquet file directly, without a pandas conversion.

```python
receipt = db.upload_parquet(path, table=copy_table, description="Example copy")
print({key: receipt[key] for key in ["rows", "columns", "description"]})
```

Example output:

```text
{'rows': 2, 'columns': 2, 'description': 'Example copy'}
```

### Replace an existing table

```python
db.upload_dataframe(prices, table=table, replace_table=True)
```

Returns an upload summary like the one above. Original creation time and description
are preserved unless you supply a new description. Without `replace_table=True`, an
existing name raises `FileExistsError`.

## Notes

- `list_tables()` includes dates and byte sizes. Use `sort_by="updated_at"`, `ascending=True`, or `search="universe"` as needed.
- `preview(rows=...)` returns up to 1,000 rows through range reads; large source row groups may require substantial transfer.
- `read_table(columns=...)` downloads the whole file but loads only selected columns. No persistent cache; save Parquet locally for reuse.
- `download_table()` protects existing files unless `overwrite=True`. Upload receipts and `describe()` are dictionaries; list/preview/read return DataFrames.
- Replacements have no undo. Keep an independent backup and coordinate one writer per table.
- Names allow letters, numbers, `_`, `-`, and `/` groups. Objects live at `tables/<name>.parquet`; DataFrame indexes are not saved.
- The demo creates `table` and `copy_table` in R2. Remove their objects in the dashboard when finished; call `local_files.cleanup()` to remove local demo downloads.
- MySQL, SQL queries, append/upsert and CSV uploads are removed. Read CSV with pandas, then use `upload_dataframe()`.

## Development

```bash
python -m unittest discover -s tests
```
