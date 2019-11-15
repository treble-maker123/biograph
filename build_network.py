from sqlalchemy import create_engine
from schema import GRAPH_DATABASE_PATH
from sources.hetio import build_nodes as build_hetio_nodes, build_edges as build_hetio_edges


if __name__ == "__main__":
    engine = create_engine(GRAPH_DATABASE_PATH)

    build_hetio_nodes(engine)
    build_hetio_edges(engine)
