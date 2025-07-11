from ubctgdb.Query.query_factory import QueryFactory
from ubctgdb.Constants.constants import Sector
from ubctgdb.Constants.constants import Ratio
from ubctgdb.Utilities.csv_processor import insert_csv_to_db, is_integrity_preserved
import pandas as pd
import io
from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI()

@app.get("/tdgb-v1/Sectors/")
async def get_sector_ratios(ratio: str):
    if ratio == "pe_ratio":
        return {"result": "PE ratio logic"}
    elif ratio == "roe":
        return {"result": "ROE logic"}
    else:
        raise HTTPException(status_code=400, detail="Invalid ratio")
    
@app.post("/upload-csv/{table_name}")
async def upload_csv_to_table(table_name: str, file: UploadFile = File(...)):
    """
    Upload CSV/Excel file to a specific database table
    """
    # Check file type
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(
            status_code=400, 
            detail="Only CSV and Excel files are supported"
        )
    
    try:
        # Read file content
        contents = await file.read()
        
        # Create DataFrame based on file type
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        else:  # Excel files
            df = pd.read_excel(io.BytesIO(contents))
        
        conn = get_database_connection()  # Replace with your actual connection method
        
        # Check data integrity
        if not is_integrity_preserved(df, table_name, conn):
            raise HTTPException(
                status_code=400,
                detail="CSV cannot be parsed into database - integrity check failed"
            )
        # Insert data
        if not insert_csv_to_db(conn, table_name, df):
            raise HTTPException(
                status_code=500,
                detail="Insertion failed"
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"CSV data successfully uploaded to table {table_name}",
                "rows_inserted": len(df),
                "filename": file.filename
            }
        )
        
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="The uploaded file is empty")
    except pd.errors.ParserError:
        raise HTTPException(status_code=400, detail="Error parsing the file")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process file: {str(e)}"
        )

def get_database_connection():
    from ubctgdb.database_conn import DBConn
    conn = DBConn()
    conn.connect()
    return conn

async def main():
    universe_query = await QueryFactory.create_universe_query(Sector.ENERGY, capacity=10) 
    test = await universe_query.get_universe_metric(Ratio.MarketCap)
    print(test)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

