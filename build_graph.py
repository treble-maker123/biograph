from utils import Node, Edge
from utils.graph import build_graph
from utils.hetio import NODES_CHECKPOINT as HETIO_NODES_CHECKPOINT, EDGES_CHECKPOINT as HETIO_EDGE_CHECKPOINT
from utils.logger import log

if __name__ == "__main__":
    nodes = Node.deserialize_bunch(HETIO_NODES_CHECKPOINT)
    edges = Edge.deserialize_bunch(HETIO_EDGE_CHECKPOINT)

    log.info("Building graph...")
    graph = build_graph(nodes, edges)
    log.info("Finished building graph.")
