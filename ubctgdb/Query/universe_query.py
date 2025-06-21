from ubctgdb.Constants.constants import Sector
from ubctgdb.database_conn import DbConn
import pandas as pd

class UniverseQuery:

    def __init__(self, sector: Sector, universe_capacity=100) -> None:
        self.sector = sector
        self.universe_capacity = universe_capacity

    
    async def get_table_size():
        pass 

    #current hardcode join on main tables
    def init_tables(self):
        #simple inner_join long running query
        conn = DbConn().get_conn()
        sql = '''
        SELECT Permno, Returns
        FROM ubctg.Industry_lookup ind, ubctg.Returns r
        WHERE ind.Ticker = r.Ticker AND ind.Industry = %s
        '''
        df = pd.read_sql_query(sql, conn.connection, params=[self.sector])
        print(df)

 
    def select(self):
        pass 

    def where(self):
        pass 

    def group_by(self):
        pass 

#test API
test_universe = UniverseQuery(Sector.ENERGY)
test_universe.init_tables()



