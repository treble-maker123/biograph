from utils.ctd import build_edges
from pdb import set_trace
from utils import load_umls


if __name__ == "__main__":
    umls = load_umls()

    edges = build_edges(umls=umls)
