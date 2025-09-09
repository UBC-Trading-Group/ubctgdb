from enum import Enum
import ubctgdb.singleton as Singleton

class SqlQuery(str, Enum):
    __metaclass__ = Singleton
    
    RATIO_QUERY = """SELECT Ticker, public_date, :ratio
        FROM Industry_lookup_new iln 
        INNER JOIN universe_monthly_ratios umr ON iln.GVKEY=umr.GVKEY
        WHERE iln.Industry = :industry
        LIMIT 10"""
    UNIVERSE_INIT_QUERY = """SELECT rn.GVKEY, iln.Ticker, rn.`Date`, rn.`Returns`
        FROM Industry_lookup_new iln 
        INNER JOIN Rets_new rn ON iln.GVKEY=rn.GVKEY
        WHERE iln.Industry = :sector
        LIMIT 10"""
    SMALL_TEST = """SELECT rn.GVKEY, iln.Ticker, rn.`Date`, rn.`Returns`
        FROM Industry_lookup_new iln 
        INNER JOIN Rets_new rn ON iln.GVKEY=rn.GVKEY
        WHERE iln.Industry = :sector
        LIMIT 10"""
    INDUSTRY_LIST_QUERY = """SELECT DISTINCT GVKEY, Ticker, Industry
        FROM Industry_lookup_new
        """
    TICKER_DAILY_TRAITS_QUERY = """WITH gvkey_table AS (SELECT gvkey, datadate, returns, cap FROM filtered_daily_prices),
        ticker_gvkey_lookup AS (SELECT tic, gvkey FROM sp500query)

        SELECT DISTINCT
            g.datadate,
            g.returns,
            g.cap as MarketCap,
            t.tic
            FROM ticker_gvkey_lookup t
            JOIN gvkey_table g
            ON t.gvkey = g.gvkey
        """ #can add: prccd, prchd, prcld, prcod, cshoc, cshtrd, vwap as other traits