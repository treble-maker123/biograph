from utils.repodb import build_nodes, build_edges
from utils.sources import load_repodb

REPODB_FILE_PATH = "repodb.csv"

if __name__ == "__main__":
    repodb = load_repodb(REPODB_FILE_PATH)
    nodes = build_nodes(repodb, force_rebuild=True)
    edges = build_edges(repodb, nodes, force_rebuild=True)
