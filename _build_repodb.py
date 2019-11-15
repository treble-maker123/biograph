from utils import log
from utils.repodb import build_nodes, build_edges

if __name__ == "__main__":
    nodes = build_nodes(force_rebuild=False)
    log.info(f"Loaded {len(nodes)} nodes from repoDB checkpoint.")

    edges = build_edges(nodes, force_rebuild=False)
    log.info(f"Loaded {len(edges)} edges from repoDB checkpoint.")
