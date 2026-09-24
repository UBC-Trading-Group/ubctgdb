# ubctgdb

Browse, download and upload club tables.

[Open the example notebook](examples/quickstart.ipynb).

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
```

## Examples

Run in order. `example_prices` is already in R2 and contains three made-up prices.
The upload examples create or replace `example_prices_copy`, a shared practice table.

```python
import ubctgdb as db
```

### List tables

```python
print(db.list_tables()[["table", "rows"]].to_string(index=False))
```

Example output:

```text
                table    rows
       example_prices       3
universe_fundamentals 2331152
universe_price_volume 2331152
   universe_inclusion 2331152
  universe_stock_info   14218
```

### Describe a table

```python
print(db.describe("example_prices")["schema"])
```

Example output:

```text
{'symbol': 'large_string', 'price': 'double'}
```

### Preview a table

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

### Read into pandas

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

### Download a file

```python
path = db.download_table("example_prices", "example_prices.parquet", overwrite=True)
print(path.name)
```

Example output:

```text
example_prices.parquet
```

### Upload a DataFrame

```python
result = db.upload_dataframe(df, table="example_prices_copy", replace_table=True)
print(result["table"], result["rows"])
```

Example output:

```text
example_prices_copy 3
```

### Upload a Parquet file

```python
result = db.upload_parquet("example_prices.parquet", table="example_prices_copy", replace_table=True)
print(result["table"], result["rows"])
```

Example output:

```text
example_prices_copy 3
```

### Rename a table

```python
print(db.rename_table("example_prices_copy", "example_prices_renamed"))
```

Example output:

```text
{'old_name': 'example_prices_copy', 'new_name': 'example_prices_renamed'}
```

The new name must be unused. Data and description are preserved; update your notebooks to use the new name.

### Delete a table

```python
print(db.delete_table("example_prices_renamed"))
```

Example output:

```text
{'table': 'example_prices_renamed', 'deleted': True}
```

Deletion is permanent. These examples remove only the practice copy, keeping `example_prices`.

Use your own table name when uploading real work. `replace_table=True` replaces existing data with no undo. Listing and displayed types may vary slightly.
Coordinate one writer per table, including renames and deletions. Rename copies then deletes;
if deletion fails, both names may remain. Check the names before retrying. These commands require write access.
