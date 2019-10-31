"""
Builds all of the edges from the het.io data file and saves it to a local directory.

Node types:
    ['Anatomy',
     'Biological Process',
     'Cellular Component',
     'Compound',
     'Disease',
     'Gene',
     'Molecular Function',
     'Pathway',
     'Pharmacologic Class',
     'Side Effect',
     'Symptom']
"""
from utils import load_hetio, load_umls, load_disease_ontology, log
from utils.hetio import build_nodes, build_edges, add_disease_metadata

UMLS_FILE_PATH = "MRCONSO.RRF"
HETIO_FILE_PATH = "integrate/data/hetnet.json.bz2"

force_build = True

if __name__ == "__main__":
    log.info("Building het.io from file.")
    hetio = load_hetio(HETIO_FILE_PATH)
    log.info("Finished building het.io from file.")

    log.info("Initializing Disease Ontology.")
    do = load_disease_ontology()
    log.info("Finished initializing Disease Ontology.")

    umls = load_umls(UMLS_FILE_PATH)

    log.info("Building het.io nodes.")
    hetio_nodes = build_nodes(hetio, force_rebuild=force_build)

    # hetio_nodes = add_compound_metadata(hetio, hetio_nodes, umls)
    hetio_nodes = add_disease_metadata(hetio, hetio_nodes, do)

    log.info("Building het.io edges.")
    hetio_edges = build_edges(hetio, hetio_nodes, force_rebuild=force_build)
