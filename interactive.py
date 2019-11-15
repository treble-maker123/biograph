from sqlalchemy import create_engine

from schema import *

if __name__ == "__main__":
    engine = create_engine(GRAPH_DATABASE_PATH)
