from utils.repodb import build_nodes, build_edges
from utils.sources import load_repodb
from utils import load_umls

REPODB_FILE_PATH = "data/repodb.csv"

if __name__ == "__main__":
    repodb = load_repodb(REPODB_FILE_PATH)

    umls = load_umls()

    nodes = build_nodes(repodb, umls=umls)
    print(f"Loaded {len(nodes)} nodes from repoDB checkpoint.")

    edges = build_edges(repodb, nodes)
    print(f"Loaded {len(edges)} edges from repoDB checkpoint.")
