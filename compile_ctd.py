from utils.logger import log
from tqdm import tqdm
from utils.ctd import build_edges as build_ctd_edges, build_nodes as build_ctd_nodes
from utils import Node, Edge, overlap
from utils.repodb import build_nodes as build_repodb_nodes, build_edges as build_repodb_edges
from utils.sources import load_repodb
from build_repodb_edges import REPODB_FILE_PATH
from pdb import set_trace
from itertools import chain

if __name__ == "__main__":
    log.info("Loading ctd edges")
    ctd_chemical_nodes, ctd_disease_nodes, _, _ = build_ctd_nodes()
    ctd_edges = build_ctd_edges()

    log.info("Loading repodb nodes and edges")
    repodb = load_repodb(REPODB_FILE_PATH)
    repodb_nodes = build_repodb_nodes(repodb)
    repodb_chemical_nodes = list(filter(lambda x: x.kind == "Compound", repodb_nodes))
    repodb_disease_nodes = list(filter(lambda x: x.kind == "Disease", repodb_nodes))
    repodb_edges = build_repodb_edges(repodb, repodb_nodes)

    # drug_overlap = overlap(repodb_chemical_nodes, ctd_chemical_nodes)
    # disease_overlap = overlap(repodb_disease_nodes, ctd_disease_nodes)

    log.info("Writing RepoDB to ctd_test.txt")

    repodb_relations = []
    with open("outputs/ctd_test.txt", "w") as graph_file:
        for repodb_edge in tqdm(repodb_edges):
            drug, disease, kind = repodb_edge.source, repodb_edge.destination, repodb_edge.kind

            ctd_matching_drugs = list(filter(lambda d: d == drug, ctd_chemical_nodes))
            ctd_matching_diseases = list(filter(lambda d: d == disease, ctd_disease_nodes))

            if len(ctd_matching_drugs) > 0 and len(ctd_matching_diseases) > 0:
                try:
                    assert len(ctd_matching_drugs) == 1, "Multiple matching drugs"
                    assert len(ctd_matching_diseases) == 1, "Multiple matching diseases"
                except Exception as e:
                    tqdm.write(f"Skipping: {e}")

                ctd_drug = ctd_matching_drugs[0]
                ctd_disease = ctd_matching_diseases[0]

                head_name, head_kind = ctd_drug.name, "Compound"
                tail_name, tail_kind = ctd_disease.name, "Disease"

                repodb_relations.append((head_name, tail_name))
                line = f"{head_name}:{head_kind}\t{kind}\t{tail_name}:{tail_kind}\n"

                graph_file.write(line)
            else:
                message = ""
                if not len(ctd_matching_drugs) > 0:
                    message += "drug doesn't exist; "
                if not len(ctd_matching_diseases) > 0:
                    message += "disease doesn't exist;"
                tqdm.write(message)

    with open("outputs/ctd_graph.txt", "w") as graph_file:
        for ctd_edge in tqdm(ctd_edges):
            head_name, head_kind = ctd_edge.source.name, ctd_edge.source.kind
            tail_name, tail_kind = ctd_edge.destination.name, ctd_edge.destination.kind
            edge_kind = ctd_edge.kind

            if (edge_kind == "treats" or
                edge_kind == "palliates" or
                edge_kind == "treats_inv" or
                edge_kind == "palliates_inv") and \
                    (head_name, tail_name) in repodb_relations:
                tqdm.write("Treats relation exists in repodb, skipping")
                continue

            line = f"{head_name}:{head_kind}\t{edge_kind}\t{tail_name}:{tail_kind}\n"
            graph_file.write(line)
