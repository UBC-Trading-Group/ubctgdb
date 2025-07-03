from ubctgdb.Constants.constants import Sector
from ubctgdb.database_conn import DBConn
from ubctgdb.Query.universe_query import UniverseQuery

class QueryFactory:

    @classmethod
    async def create_universe_query(cls, sector: Sector, capacity=100) -> UniverseQuery:
        print(f"created and sector is {str(sector)}")
        conn_inst = await DBConn().connect()
        return UniverseQuery(sector=sector, conn_instance=conn_inst, universe_capacity=capacity)