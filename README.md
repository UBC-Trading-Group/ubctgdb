# ubctgdb

Upload, inspect and download shared pandas tables in a private **Cloudflare R2** bucket.
Each table is one Parquet file. No database server, website or version history.

## Install

Python 3.9+:

```bash
pip install git+https://github.com/UBC-Trading-Group/ubctgdb.git@codex/r2-storage
# From a local checkout instead:
pip install -e .
```

The branch installation works after the branch is pushed to GitHub.

## Configure

Put a `.env` in your notebook's working directory or a parent directory:

```dotenv
R2_ENDPOINT_URL=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET=ubctg-data
```

Use R2 S3 credentials scoped to this bucket: **Object Read only** for readers,
**Object Read & Write** for publishers. No public bucket access is needed.
Existing `endpoint`, `access_key` and `secret` settings also work; `R2_*` names
take precedence. The Cloudflare management `token` is not used. Never commit `.env`.

## Browse and read

```python
import ubctgdb as db

db.list_tables()                       # newest additions first
db.list_tables(search="universe", sort_by="updated_at")
db.describe("universe_fundamentals")    # summary and column types
db.preview("universe_fundamentals", rows=20)

df = db.read_table("universe_fundamentals")
df = db.read_table("universe_fundamentals", columns=["date", "lpermno"])
db.download_table("universe_fundamentals", "data/fundamentals.parquet")
```

`list_tables()` returns a DataFrame; `describe()` returns a dictionary;
`preview()` and `read_table()` return DataFrames; `download_table()` returns a Path.
Listing shows dates, row/column counts, size in bytes and description.
Use `ascending=True` to reverse sorting.

## Upload

```python
db.upload_dataframe(df, table="factor/my_factor", description="Monthly factor values")
db.upload_parquet("data/fundamentals.parquet", table="universe_fundamentals")

# Explicitly replace the current table:
db.upload_dataframe(df, table="factor/my_factor", replace_table=True)
```

Uploads return a summary dictionary. Replacements preserve the original added date
and description unless a new description is supplied. DataFrame indexes are not stored.
Table names allow letters, numbers, `_`, `-`, and `/` for groups.
Objects live at `tables/<name>.parquet`; descriptions and counts travel with the object.

## Keep in mind

- Replacing a table is permanent. Keep an independent backup and coordinate **one writer per table**; simultaneous writes are not locked.
- Downloads use temporary files before replacing local output. Pass `overwrite=True` to replace an existing local file.
- There is no persistent cache: each read downloads fresh data. Save a local Parquet file to reuse it.
- `read_table(columns=...)` still downloads the whole file, but loads only selected columns into pandas. Allow disk space and RAM for your data.
- Previews read Parquet ranges (up to 1,000 rows returned). Large source row groups can still require substantial transfer.
- This is a breaking replacement for the MySQL package. SQL, append/upsert and CSV upload commands are removed. Convert CSV with pandas, then upload the DataFrame.

## Development

```bash
python -m unittest discover -s tests
```
