from ubctgdb.Constants.constants import Sector
from ubctgdb.database_conn import DbConn
from sqlalchemy import text
import random
import asyncio


class UniverseQuery:

    def __init__(self, sector: Sector, universe_capacity=100) -> None:
        self.sector = sector
        self.universe_capacity = universe_capacity

    
    async def get_table_size():
        pass 


    async def init_universe_with_resevoir(self):
        init_query = '''
        SELECT rn.GVKEY, iln.Ticker, rn.`Date`, rn.`Returns`
        FROM Industry_lookup_new iln 
        INNER JOIN Rets_new rn ON iln.GVKEY=rn.GVKEY
        WHERE iln.Industry = :sector
        '''
        conn = await DbConn.connect()
        stream = await conn.execute_stream(text(init_query), {})
        reservoir = []
        for i, item in enumerate(stream):
            if i < self.universe_capacity:
                reservoir.append(item)
            else:
                j = random.randrange(i + 1)  # Generate a random index up to current item count
                if j < self.universe_capacity:
                    reservoir[j] = item
        return reservoir  

   
    #current hardcode join on main tables
    async def init_universe(self):
        init_query = '''
        SELECT rn.GVKEY, iln.Ticker, rn.`Date`, rn.`Returns`
        FROM Industry_lookup_new iln 
        INNER JOIN Rets_new rn ON iln.GVKEY=rn.GVKEY
        WHERE iln.Industry = :sector
        '''
        conn = await DbConn.connect()
        generator = await conn.execute_stream(text(init_query), {"sector": str(self.sector)})








#test API

test_universe = UniverseQuery(Sector.ENERGY)
asyncio.run(test_universe.init_universe())



