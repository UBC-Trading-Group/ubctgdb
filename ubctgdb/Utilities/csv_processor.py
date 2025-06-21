import tkinter as tk
from tkinter import filedialog
from sqlalchemy import text
import pandas as pd
from datetime import datetime
import os

root = tk.Tk()
root.withdraw()


def insert_csv_to_db(conn, table_name, df):
    '''
    insert's csv into database and returns whether it could be inserted
    '''
    df_skipped = df.iloc[1:]  # Skip the first row
    # Prepare the insert statement dynamically
    column_names = df_skipped.columns.tolist()
    placeholders = ", ".join([f":{col}" for col in column_names])
    column_clause = ", ".join(column_names)
    sql_command = f"INSERT INTO {table_name} ({column_clause}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        try:
            conn.execute(text(sql_command), dict(row))
        except Exception:
            print(f"insertion failed for row: {row}")
            abort_csv_upload(df, conn, table_name)
            return False
    return True

def abort_csv_upload(df, conn, table_name):
    '''
    Tries to delete all rows matching the inserted rows from the CSV.
    Only works reliably if CSV uniquely identifies the inserted rows.
    '''
    df_skipped = df.iloc[1:]
    column_names = df_skipped.columns.tolist()

    # Compose WHERE clause: col1 = :col1 AND col2 = :col2 ...
    where_clause = " AND ".join([f"{col} = :{col}" for col in column_names])
    sql = f"DELETE FROM {table_name} WHERE {where_clause}"

    try:
        for _, row in df_skipped.iterrows():
            conn.execute(text(sql), dict(row))
        print("Rollback successful via manual delete.")
    except Exception as e:
        print(f"Manual abort failed: {e}")

def write_csv_to_table(selected_table) -> None:
    '''
    prompts user to open csv and begins write operation
    '''
    file_path = filedialog.askopenfilename()
    file_extenstion = os.path.basename(file_path).split(".")[1].strip()
    print(file_extenstion)

    if file_extenstion != "csv" and file_extenstion != "xlsx":
        print("not supported file type only excel or csv supported")
        return
    
    df = pd.DataFrame(pd.read_csv(file_path) if file_extenstion == "csv" else pd.read_excel(file_path))
    do_csv_work(df, selected_table)

def do_csv_work(df, selected_table) -> None:
    '''
    checks the integrity of the data now before inserting
    '''
    db_instance = DbConn(None)
    curr_connection = db_instance.get_conn()
    if not is_integrity_preserved(df, selected_table, curr_connection):
        print("CSV cannot be parsed into database data types are differing")
        return
    
    try:
        insert_csv_to_db(curr_connection, selected_table, df)
    except Exception:
        print("failed insertion")


def is_integrity_preserved(df, selected_table, conn) -> bool:
  query = f"""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{selected_table}'
    """
  
  #hold a dictionary with conversions
  python_to_sql = {
    "str": ["varchar", "char", "text"],
    "int": ["int", "integer", "bigint", "smallint"],
    "float": ["float", "double", "real", "decimal", "numeric"],
    "bool": ["boolean", "tinyint", "bit"],
    "datetime": ["datetime", "timestamp", "date"],
    }
  
  sql_table_data_types = dict(conn.execute(text(query)).fetchall())

  #means there are no columns defined
  if not any(isinstance(col, str) for col in df.columns):
      return False  

  df_column_types = {col: type(df[col].iloc[1]).__name__ for col in df.columns}


  table_keys = [key.lower().strip() for key in sql_table_data_types.keys()]
  column_keys = [key.lower().strip() for key in df_column_types.keys()]

  print(df_column_types)

  #means headers are not the same lexographically
  if table_keys != column_keys:
      return False
  
  #now check if there are any types that are mismatched
  if not check_for_type_equality(python_to_sql, df, df_column_types, sql_table_data_types):
      return False
 

def check_for_type_equality(conversion_dict, df, py_dict: dict, sql_dict: dict):
    normalized_sql_values = [v.lower() for v in sql_dict.values()]
    
    for col_index, column_type in enumerate(py_dict.values()):
        sql_type = normalized_sql_values[col_index]
        possible_matches = set(conversion_dict.get(column_type.lower(), []))

        if not possible_matches:
            return False

        if column_type.lower() == "str":
            df_str_cols = find_df_cols_of_type("str", df)

            for col in df_str_cols:
                inferred_data_type = infer_str_type(df[col].iloc[1])  # check the second value

                if inferred_data_type != "str" and inferred_data_type not in possible_matches:
                    possible_matches.add(inferred_data_type)

        if sql_type not in possible_matches:
            return False

    return True

def find_df_cols_of_type(target_type: str, df):
    df_cols = []
    for col in df.columns:
        try:
            inferred_type = type(df[col].iloc[1]).__name__
            if inferred_type == target_type:
                df_cols.append(col)
        except IndexError:
            continue 
    return df_cols


def try_parse(param, type_lambda):
    try:
        type_lambda(param)
        return True
    except ValueError:
        return False


#quick chatGPT function to infer data type of string
def infer_str_type(value: str) -> str:
    # Strip surrounding whitespace
    value = value.strip()

    # Handle empty or null-like values
    if value in {"", "null", "none", "NaN"}:
        return "null"

    # Try integer
    if try_parse(value, lambda x: int(x)):
        return "int"
    elif try_parse(value, lambda x: float(x)):
        return "float"

    # Try boolean
    if value.lower() in {"true", "false"}:
        return "bool"

    # Try datetime
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            datetime.strptime(value, fmt)
            return "datetime"
        except ValueError:
            continue

    # Fallback: it's a string
    return "str"