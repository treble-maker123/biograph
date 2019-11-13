"""
Edges are from Comparative Toxicgenomic Dataset
http://ctdbase.org/downloads/

Chemical-gene: http://ctdbase.org/reports/CTD_chem_gene_ixns.csv.gz
Chemical–disease: http://ctdbase.org/reports/CTD_chemicals_diseases.csv.gz
Gene–disease: http://ctdbase.org/reports/CTD_genes_diseases.csv.gz (HUGE FILE, 1GB)
Gene–pathway: http://ctdbase.org/reports/CTD_genes_pathways.csv.gz
Disease–pathway: http://ctdbase.org/reports/CTD_diseases_pathways.csv.gz
"""
from pdb import set_trace

import pandas as pd
from tqdm import tqdm

from utils import load_umls, Node, Edge
from utils import log
from utils.graph import GRAPH_NODES_CHECKPOINT, GRAPH_EDGES_CHECKPOINT

UMLS_FILE_PATH = "data/MRCONSO.RRF"
CTD_CHEM_DISEASE_FILE_PATH = "data/CTD_chemicals_diseases.csv"

CHEM_DIS_COLUMNS = [
    "ChemicalName",
    "ChemicalID",
    "CasRN",
    "DiseaseName",
    "DiseaseID",
    "DirectEvidence",
    "InferenceGeneSymbol",
    "InferenceScore",
    "OmimIDs",
    "PubMedIDs"
]

if __name__ == "__main__":
    umls = load_umls()

    log.info(f"Loading Comparative Toxicgenomic Chemical-Disease Dataset.")
    chem_dis = pd.read_csv(CTD_CHEM_DISEASE_FILE_PATH, names=CHEM_DIS_COLUMNS)

    log.info("Loading check-pointed nodes.")
    nodes = Node.deserialize_bunch(GRAPH_NODES_CHECKPOINT)
    log.info("Loading check-pointed edges.")
    edges = Edge.deserialize_bunch(GRAPH_EDGES_CHECKPOINT, nodes)

    # marker/mechanism: A chemical that correlates with a disease (e.g., increased abundance in the brain of
    #     chemical X correlates with Alzheimer disease) or may play a role in the etiology of a disease
    #     (e.g., exposure to chemical X causes lung cancer).
    #
    # therapeutic: A chemical thats has a known or potential therapeutic role in a disease (e.g., chemical X is
    #     used to treat leukemia).
    #
    # het.io has "palliates" and "treats" between chemical and diseases, and therapeutic seems to be a bigger umbrella,
    # will make assumption that "palliates" and "treats" => "treats" for now.
    # TODO: Figure out a better way to handle this.
    therapeutic = chem_dis[chem_dis["DirectEvidence"] == "therapeutic"]
    marker = chem_dis[chem_dis["DirectEvidence"] == "marker/mechanism"]
    combined = pd.concat([therapeutic, marker])

    # TODO: Swap these two later.
    # nodes = add_chemical_nodes(nodes, combined, umls)
    dataframe = combined

    compound_nodes = list(filter(lambda x: x.kind == "Compound", nodes))
    chemical_ids = dataframe["ChemicalID"].unique()

    log.info("Building chemical nodes.")
    for chemical_id in tqdm(chemical_ids):
        chem_df = umls[(umls["CODE"] == chemical_id) & (umls["SAB"] == "MSH")]
        umls_cuis = list(chem_df["CUI"].unique())
        mesh_ids = list(chem_df["CODE"].unique())
        attribute_set = set(umls_cuis + mesh_ids)

        if len(attribute_set) == 0:
            continue

        found = False

        for node in compound_nodes:
            if attribute_set.intersection(node.attributes):  # same node
                log.info("Found existing node, adding metadata and moving on.")
                found = True
                node.add_mesh_id(mesh_ids)
                node.add_cui(umls_cuis)
                break

        if not found:
            log.info("No node with overlapping attributes found, creating new node.")
            chemical_name = dataframe[dataframe["ChemicalID"] == chemical_id]["ChemicalName"].iloc[0]
            # TODO: Figure out the license here!
            node = Node(identifier=umls_cuis[0], name=chemical_name, kind="Compound",
                        sources="Comparative Toxicgenomic Dataset", license="?")

    # filter out the DiseaseID that do not start with "MESH:", there are 5 records that start with "OMIM:"
    disease_ids = dataframe["DiseaseID"].unique()
    disease_ids = list(filter(lambda x: x[:5] == "MESH:", disease_ids))
    disease_ids = list(map(lambda x: x[5:], disease_ids))
    disease_nodes = list(filter(lambda x: x.kind == "Disease", nodes))

    set_trace()
