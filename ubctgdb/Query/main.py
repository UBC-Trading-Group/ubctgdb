from ubctgdb.Query.query_factory import QueryFactory
from ubctgdb.Constants.constants import Sector
import asyncio


async def main():
    universe_query = await QueryFactory.create_universe_query(Sector.ENERGY) 
    test = await universe_query.init_universe_with_resevoir()
    print(test)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"RuntimeError: {e}")