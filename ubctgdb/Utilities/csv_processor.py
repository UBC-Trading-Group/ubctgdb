import tkinter as tk
from tkinter import filedialog
from sqlalchemy import text
import pandas as pd
from datetime import datetime
import os

root = tk.Tk()
root.withdraw()

def write_csv_to_table(selected_table, conn) -> None:
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
    if not is_integrity_preserved(df, selected_table, conn):
        print("CSV cannot be parsed into database (FAILURE)")
        return
    if not insert_csv_to_db(conn, selected_table, df):
        print("Insertion failed (FAILURE)")

    print("Insertion successful (SUCCESS)")

def insert_csv_to_db(conn, table_name, df):
    '''
    insert's csv into database and returns whether it could be inserted
    '''
    # Prepare the insert statement dynamically
    column_names = df.columns.tolist()
    placeholders = ", ".join([f":{col}" for col in column_names])
    column_clause = ", ".join(column_names)
    sql_command = f"INSERT INTO {table_name} ({column_clause}) VALUES ({placeholders})"

    # Get a session to handle transactions properly
    session_factory = conn.get_session()
    with session_factory() as session:
        try:
            for _, row in df.iterrows():
                session.execute(text(sql_command), dict(row))
            session.commit()  # Commit all inserts
            return True
        except Exception as e:
            session.rollback()  # Rollback on error
            print(f"Insertion failed for row: {row}")
            print(f"Error details: {str(e)}")
            print(f"SQL command: {sql_command}")
            print(f"Row data: {dict(row)}")
            return False

def is_integrity_preserved(df, selected_table, conn) -> bool:
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{selected_table}'
        """
    
    session_factory = conn.get_session()
    with session_factory() as session:
        result = session.execute(text(query))
        sql_table_data_types = dict(result.fetchall())

    #means there are no columns defined
    if not any(isinstance(col, str) for col in df.columns):
        print("No columns found in the DataFrame.")
        return False  

    df_column_types = {col: type(df[col].iloc[0]).__name__ for col in df.columns}

    print(f'CSV Column and Type: {df_column_types}')
    print(f'SQL Table Column and Type: {sql_table_data_types}')

    # Compare headers
    if set(sql_table_data_types.keys()) != set(df_column_types.keys()):
        print("Header Mismatch (FAILURE)")
        return False
    
    # Compare value data types
    if not check_for_type_equality(df, df_column_types, sql_table_data_types):
        print("Data Type Mismatch (FAILURE)")
        return False
    
    return True
    
def check_for_type_equality(df, py_dict: dict, sql_dict: dict):
    # Python to SQL type mapping
    conversion_dict = {
        "str": ["varchar", "char", "text"],
        "int": ["int", "integer", "bigint", "smallint"],
        "int64": ["int", "integer", "bigint", "smallint"],
        "float": ["float", "double", "real", "decimal", "numeric"],
        "float64": ["float", "double", "real", "decimal", "numeric"],
        "bool": ["boolean", "tinyint", "bit"],
        "datetime": ["datetime", "timestamp", "date"],
        }
    
    for csv_col, col_type in py_dict.items():
        sql_type = sql_dict[csv_col]

        if col_type.lower() == "str":
            inferred_data_type = infer_str_type(df[csv_col].iloc[0])
            sql_conversion = conversion_dict.get(inferred_data_type, [])
        else:
            sql_conversion = conversion_dict.get(col_type.lower(), [])

        if sql_type not in sql_conversion:
            print(f"Type mismatch for column '{csv_col}': SQL type '{sql_type}' not in CSV Type '{col_type}' : '{sql_conversion}'")
            return False

    return True

def infer_str_type(value: str) -> str:
    '''
    Infers the type of a string value and returns a string representation of the type.
    '''
    def try_parse(param, type_lambda):
        try:
            type_lambda(param)
            return True
        except ValueError:
            return False
        
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