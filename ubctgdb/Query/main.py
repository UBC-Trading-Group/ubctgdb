from ubctgdb.Query.query_factory import QueryFactory
from ubctgdb.Constants.constants import Sector
from ubctgdb.Constants.constants import Ratio
from fastapi import FastAPI, Request, HTTPException
import uvicorn
import ubctgdb.Query.queries as Queries
import pandas as pd
import json
import datetime



app = FastAPI()

def get_traits_df_on_day(daily_traits_json, date: str):
    daily_traits_df = pd.DataFrame(json.loads(daily_traits_json))
    daily_traits_df = daily_traits_df.rename(columns={"tic": "Ticker"})
    traits_latest = daily_traits_df[daily_traits_df["datadate"] == date].copy() #second last date, it has more companies
    
    traits_latest["S&P Benchmark Weight"] = traits_latest["MarketCap"] / traits_latest["MarketCap"].sum()
    traits_latest["S&P Benchmark Returns"] = traits_latest["S&P Benchmark Weight"] * traits_latest["returns"]
    traits_latest = traits_latest.drop(columns=["datadate"])
    return traits_latest

@app.get("/tgdb-v1/tickers/traits/")
async def get_traits():
    universe_query = await QueryFactory.create_universe_query(Sector.FINANCIALS, capacity=10)
    daily_traits = await universe_query.get_data(Queries.SqlQuery.TICKER_DAILY_TRAITS_QUERY)
    traits_df = get_traits_df_on_day(daily_traits, "2025-07-03")

    print("df", traits_df)
    return {"status": "success", "data": traits_df}



@app.get("/tgdb-v1/tickers/{gvkey}/traits/")
async def get_traits_for_ticker(gvkey: str):
    universe_query = await QueryFactory.create_universe_query(Sector.FINANCIALS, capacity=10)
    daily_traits = await universe_query.get_data(Queries.SqlQuery.TICKER_DAILY_TRAITS_QUERY)
    traits_df = get_traits_df_on_day(daily_traits, "2025-07-03")
    traits_df = traits_df[traits_df["Ticker"] == gvkey.upper()].copy()
    if len(traits_df) == 0:
        raise HTTPException(status_code=404, detail=f"Ticker {gvkey.upper()} not found")

    print("df", traits_df)
    return {"status": "success", "data": traits_df}

@app.get("/tgdb-v1/industries/")
async def get_industries():
    universe_query = await QueryFactory.create_universe_query(Sector.FINANCIALS, capacity=10)
    #test = await universe_query.init_universe()

    print("Fetching industries and daily traits...")
    industries = await universe_query.get_data(Queries.SqlQuery.INDUSTRY_LIST_QUERY)
    daily_traits = await universe_query.get_data(Queries.SqlQuery.TICKER_DAILY_TRAITS_QUERY)

    industries_df = pd.DataFrame(json.loads(industries))[["Ticker", "Industry"]]
    daily_traits_df = pd.DataFrame(json.loads(daily_traits))
    
    #print("one", industries_df)
    #print("one and half", daily_traits_df)

    daily_traits_df = daily_traits_df.rename(columns={"tic": "Ticker"})
    traits_latest = daily_traits_df[daily_traits_df["datadate"] == "2025-07-03"].copy() #second last date, it has more companies
    
    traits_latest["S&P Benchmark Weight"] = traits_latest["MarketCap"] / traits_latest["MarketCap"].sum()
    traits_latest["S&P Benchmark Returns"] = traits_latest["S&P Benchmark Weight"] * traits_latest["returns"]
    traits_latest = traits_latest.drop(columns=["datadate"])
    
    print("two", traits_latest)


    sp_stock_to_industry = pd.merge(industries_df, traits_latest, on='Ticker')
    #print("three", sp_stock_to_industry) #Optimization: REFACTOR THIS into a single function, returning sp_stock_to_industry

    industry_summary = sp_stock_to_industry[["Industry", "S&P Benchmark Weight", "S&P Benchmark Returns"]].groupby("Industry").sum().reset_index().copy()
    
    industry_summary["Portfolio Weight"] = [0.2, 0.1, 0.1, 0.3, 0.05, 0.05, 0.01, 0.16, 0.03] #HARD CODED PORTFOLIO
    industry_summary["Portfolio Return"] = [0.2, -0.05, 0.06, 1.05, 0.03, 2.78, -0.27, 1.12, -2.34] #HARD CODED PORTFOLIO

    portfolio_return = (industry_summary["Portfolio Weight"] * industry_summary["Portfolio Return"]).sum()
    benchmark_return = (industry_summary["S&P Benchmark Weight"] * industry_summary["S&P Benchmark Returns"]).sum()
    semi_notional_fund = (industry_summary["Portfolio Weight"] * industry_summary["S&P Benchmark Returns"]).sum()
 

    # Asset Allocation, Stock Selection, Interation Effects
    industry_summary["Asset Allocation"] = (industry_summary["Portfolio Weight"] - industry_summary["S&P Benchmark Weight"]) * (industry_summary["S&P Benchmark Returns"] - benchmark_return)
    industry_summary["Stock Selection"] = industry_summary["S&P Benchmark Weight"] * (industry_summary["Portfolio Return"] - industry_summary["S&P Benchmark Returns"])
    industry_summary["Interaction"] = (industry_summary["Portfolio Weight"] - industry_summary["S&P Benchmark Weight"]) * (industry_summary["Portfolio Return"] - industry_summary["S&P Benchmark Returns"])

    print("four", industry_summary)

    return {"status": "success", "data": industry_summary.to_json(orient="records")}

#TODO: collect industry traits eg ticker, marketcap, s&p weight, monthly return, weighted return. see jupyter notebook

@app.get("/tgdb-v1/Sectors/")
async def get_sector_ratios(ratio: str):
    if ratio == "pe_ratio":
        return {"result": "PE ratio logic"}
    elif ratio == "roe":
        return {"result": "ROE logic"}
    else:
        raise HTTPException(status_code=400, detail="Invalid ratio")
    
@app.get("/tgdb-v1/query/")
async def test():
    universe_query = await QueryFactory.create_universe_query(Sector.FINANCIALS, capacity=10)
    #test = await universe_query.init_universe()
    test = await universe_query.get_universe_metric(Ratio.MarketCap)
    
    print(test)
    return {"status": "success", "data": test}

async def main():
    universe_query = await QueryFactory.create_universe_query(Sector.FINANCIALS, capacity=10) 
    test = await universe_query.get_universe_metric(Ratio.MarketCap)
    print(test)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

