import os
from pathlib import Path

class QueryReader:
    def __init__(self):
        self.base_path = os.path.join(Path(os.path.dirname(__file__)).parent, "Query", "queries")
    
    def get_query_text(self, query_name):
        path = f"{os.path.join(self.base_path, query_name)}.txt"
        with open(path, "r", encoding="utf-8-sig") as f:
            return f.read()
            

