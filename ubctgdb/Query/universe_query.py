from ubctgdb.Constants.constants import Sector
from ubctgdb.database_conn import DbConn
from sqlalchemy import text
import asyncio


class UniverseQuery:

    def __init__(self, sector: Sector, universe_capacity=100) -> None:
        self.sector = sector
        self.universe_capacity = universe_capacity

    
    async def get_table_size():
        pass 

    #current hardcode join on main tables
    async def init_universe(self):
        init_query = '''
        SELECT rn.GVKEY, iln.Ticker, rn.`Date`, rn.`Returns`
        FROM Industry_lookup_new iln 
        INNER JOIN Rets_new rn ON iln.GVKEY=rn.GVKEY
        WHERE iln.Industry = :sector
        '''
        conn = await DbConn.connect()
        res = await conn.execute(text(init_query), {"sector": str(self.sector)})



 
    def select(self):
        pass 

    def where(self):
        pass 

    def group_by(self):
        pass 

#test API

test_universe = UniverseQuery(Sector.ENERGY)
asyncio.run(test_universe.init_universe())



