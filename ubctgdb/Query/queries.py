from enum import Enum
import ubctgdb.singleton as Singleton

class SqlQuery(str, Enum):
    __metaclass__ = Singleton
    
    RATIO_QUERY = """SELECT Ticker, public_date, :ratio
        FROM Industry_lookup_new iln 
        INNER JOIN universe_monthly_ratios umr ON iln.GVKEY=umr.GVKEY
        WHERE iln.Industry = ":industry"
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