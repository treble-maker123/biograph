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
from utils import log, Node
from utils.hetio import build_nodes, add_compound_metadata, add_disease_metadata
from utils.sources import load_umls, load_disease_ontology

NODES_WITH_METADATA_CHECKPOINT = "outputs/hetio_metadata_nodes.checkpoint.json"

if __name__ == "__main__":
    log.info("Building het.io nodes.")
    hetio_nodes = build_nodes()

    log.info("Loading UMLS metadata.")
    umls_metadata = load_umls()

    log.info("Pulling UMLS metadata for het.io compound nodes.")
    hetio_nodes = add_compound_metadata(hetio_nodes, umls_metadata)

    log.info("Loading disease ontology")
    disease_ontology = load_disease_ontology()

    log.info("Pulling UMLS metadata for het.io disease nodes.")
    hetio_nodes = add_disease_metadata(hetio_nodes, disease_ontology)

    # TODO: ADD METADATA FOR GENE AND PATHWAY

    log.info("Check-pointing het.io nodes with metadata")
    Node.serialize_bunch(hetio_nodes, NODES_WITH_METADATA_CHECKPOINT)
