from ubctgdb.Query.queries import SqlQuery
from sqlalchemy import text

class QueryReader:
    def safe_get_query_text(self, query_name):
        query = None

        try:
            query = text(SqlQuery[query_name.name].value)
        except:
            pass
        return query
            

