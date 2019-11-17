from utils.repodb import build_nodes, build_edges
from utils.sources import load_repodb

REPODB_FILE_PATH = "data/repodb.csv"

if __name__ == "__main__":
    repodb = load_repodb(REPODB_FILE_PATH)

    nodes = build_nodes(repodb, force_rebuild=False)
    print(f"Loaded {len(nodes)} nodes from repoDB checkpoint.")

    edges = build_edges(repodb, nodes, force_rebuild=False)
    print(f"Loaded {len(edges)} edges from repoDB checkpoint.")
