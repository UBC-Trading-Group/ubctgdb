from ubctgdb.Query.query_factory import QueryFactory
from ubctgdb.Constants.constants import Sector
from ubctgdb.Constants.constants import Ratio
from fastapi import FastAPI, Request, HTTPException
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



async def main():
    universe_query = await QueryFactory.create_universe_query(Sector.ENERGY, capacity=10) 
    test = await universe_query.get_universe_metric(Ratio.MarketCap)
    print(test)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

