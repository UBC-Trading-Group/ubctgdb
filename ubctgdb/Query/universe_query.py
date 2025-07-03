from ubctgdb.Constants.constants import Sector
from ubctgdb.database_conn import DBConn
from ubctgdb.Utilities.query_reader import QueryReader
from ubctgdb.Constants.constants import Ratio
from sqlalchemy import text
import random
import pandas as pd


class UniverseQuery:

    def __init__(self, sector: Sector, conn_instance: DBConn, universe_capacity: int) -> None:
        self.sector = sector
        self.universe_capacity = universe_capacity
        self.query_reader = QueryReader()
        self.conn_instance = conn_instance
    
    
    async def get_table_size():
        pass 


    async def init_universe_with_resevoir(self):
        '''
        uses resevoir pooling algorithm to uniformly choose subset of universe query
        '''
        init_query = self.query_reader.get_query_text("universe_init_query")
        sessionmaker = self.conn_instance.get_session()

        async with sessionmaker() as session:
            stream = await session.stream(text(init_query), {"sector": str(self.sector)})
            reservoir, i = [], 0
            async for item in stream:
                if i < self.universe_capacity:
                    reservoir.append(item)
                else:
                    j = random.randrange(i + 1)
                    if j < self.universe_capacity:
                        reservoir[j] = item
                i += 1 
            return pd.DataFrame(reservoir, columns=["GVKey", "Ticker", "Date", "Returns"])  

   
    async def init_universe(self):
        init_query = self.query_reader.get_query_text("universe_init_query")
        return await self.conn_instance.execute(text(init_query), {"sector": str(self.sector)})
    
    async def get_universe_metric(self, ratio: Ratio):
        ratio_query = self.query_reader.get_query_text("ratios_query")
        return await self.conn_instance.execute(text(ratio_query), {"ratio": str(ratio)})