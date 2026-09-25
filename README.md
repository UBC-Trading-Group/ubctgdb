# ubctgdb

Browse, download and upload club tables.

[Open the example notebook](quickstart.ipynb).

## Install

Install directly from the `r2-storage` branch:

```bash
pip install --upgrade git+https://github.com/UBC-Trading-Group/ubctgdb.git@r2-storage
```

Or run this in a Jupyter notebook:

```python
%pip install --upgrade git+https://github.com/UBC-Trading-Group/ubctgdb.git@r2-storage
```

Restart the notebook kernel after installing.

## .env

Save `.env` beside your notebook:

```dotenv
R2_ENDPOINT_URL=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET=ubctg-data
YOUR_NAME=Alex
```

## Examples

Run in order. `example_prices` is already in R2 and contains three made-up prices.
The upload examples create or replace `example_prices_copy`, a shared practice table.

```python
import ubctgdb as db
```

### Find and inspect

#### List tables

```python
print(db.list_tables())
```

Example output (dates and sizes will vary):

```text
            table  rows        updated_at     size updated_by
0  example_prices     3  2026-09-25 21:53  1.75 KB       Alex
```

#### Describe a table

```python
print(db.describe("example_prices")["schema"])
```

Example output:

```text
{'symbol': 'large_string', 'price': 'double'}
```

#### Preview a table

```python
print(db.preview("example_prices").to_string(index=False))
```

Example output:

```text
symbol  price
   AAA   10.5
   BBB   20.0
   CCC   30.5
```

### Read and download

#### Read into pandas

```python
df = db.read_table("example_prices")
print(df.to_string(index=False))
```

Example output:

```text
symbol  price
   AAA   10.5
   BBB   20.0
   CCC   30.5
```

#### Caching

Repeated reads reuse a local copy after checking R2 for changes.

```python
df = db.read_table("example_prices")
df = db.read_table("example_prices", refresh=True)  # Download again

db.cache_info()                        # Show cached tables and sizes
db.clear_cache("example_prices")       # Clear one
db.clear_cache()                       # Clear all for this bucket
```

The cache uses up to 10 GB across notebooks and buckets on your computer.
Older unused files are removed automatically; larger files are read without keeping a cached copy.
Clearing the cache never deletes shared tables. An internet connection is needed to check for changes.

#### Download a file

```python
path = db.download_table("example_prices", "example_prices.parquet", overwrite=True)
print(path.name)
```

Example output:

```text
example_prices.parquet
```

### Upload and organize

#### Upload a DataFrame

```python
result = db.upload_dataframe(
    df,
    table="example_prices_copy",
    description="Three fictional stock prices for practice.",
    replace_table=True,
)
print(result["table"], result["rows"])
print(db.describe("example_prices_copy")["description"])
```

Example output:

```text
example_prices_copy 3
Three fictional stock prices for practice.
```

#### Upload a Parquet file

```python
result = db.upload_parquet("example_prices.parquet", table="example_prices_copy", replace_table=True)
print(result["table"], result["rows"])
print(db.describe("example_prices_copy")["description"])
```

Example output:

```text
example_prices_copy 3
Three fictional stock prices for practice.
```

#### Folders

Use `/` in table names. Folders are created automatically.

```python
db.upload_dataframe(df, table="practice/prices", replace_table=True)
df = db.read_table("practice/prices")

db.list_tables()                   # All tables in all folders
db.list_tables(folder="practice")  # This folder and its subfolders
```

Use the full name, such as `"practice/prices"`, when reading, renaming or deleting a table.

#### Rename a table

```python
print(db.rename_table("example_prices_copy", "example_prices_renamed"))
```

Example output:

```text
{'old_name': 'example_prices_copy', 'new_name': 'example_prices_renamed'}
```

The new name must be unused. Data and description are preserved; update your notebooks to use the new name.

#### Delete a table

```python
print(db.delete_table("example_prices_renamed"))
```

Example output:

```text
{'table': 'example_prices_renamed', 'deleted': True}
```

Deleting or replacing a table cannot be undone. Only one person should change a given table at a time.
