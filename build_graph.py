from utils.logger import log
from utils.hetio import load_edges, load_nodes
from utils.graph import build_graph


if __name__ == "__main__":
    nodes = load_nodes()
    edges = load_edges()

    log.info("Building graph...")
    graph = build_graph(nodes, edges)
    log.info("Finished building graph.")
