import math
import os
from typing import List

import pandas as pd
from tqdm import tqdm

from utils import Node
from utils.logger import log

CHEMICAL_COLUMNS = [
    "ChemicalName", "ChemicalID", "CasRN", "Definition", "ParentIDs", "TreeNumbers", "ParentTreeNumbers", "Synonyms",
    "DrugBankIDs"
]
DISEASE_COLUMNS = [
    "DiseaseName", "DiseaseID", "AltDiseaseIDs", "Definition", "ParentIDs", "TreeNumbers", "ParentTreeNumbers",
    "Synonyms", "SlimMappings"
]
GENE_COLUMNS = [
    "GeneSymbol", "GeneName", "GeneID", "AltGeneIDs", "Synonyms", "BioGRIDIDs", "PharmGKBIDs", "UniProtIDs"
]
PATHWAY_COLUMNS = [
    "PathwayName", "PathwayID"
]

CHEMICALS_PATH = "data/CTD_chemicals.csv"
DISEASES_PATH = "data/CTD_diseases.csv"
GENES_PATH = "data/CTD_genes.csv"
PATHWAYS_PATH = "data/CTD_pathways.csv"

CHEMICAL_NODES_CHECKPOINT = "outputs/ctd_chemical_nodes.checkpoint.json"
DISEASE_NODES_CHECKPOINT = "outputs/ctd_disease_nodes.checkpoint.json"
GENE_NODES_CHECKPOINT = "outputs/ctd_gene_nodes.checkpoint.json"
PATHWAY_NODES_CHECKPOINT = "outputs/ctd_pathway_nodes.checkpoint.json"
NODES_CHECKPOINT = "outputs/ctd_nodes.checkpoint.json"


def build_chemical_nodes(**kwargs) -> List[Node]:
    chemicals = pd.read_csv(CHEMICALS_PATH, names=CHEMICAL_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(CHEMICAL_NODES_CHECKPOINT):
            return Node.deserialize_bunch(CHEMICAL_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(chemicals.iterrows()):
        # All of the chemicals start with "MESH:"
        chemical_id = row["ChemicalID"][:5]
        chemical_name = row["ChemicalName"]

        # drug bank ids are delimited by "|"
        drug_bank_ids = row["DrugBankIDs"]

        if type(drug_bank_ids) == float and math.isnan(drug_bank_ids):
            drug_bank_ids = ""

        if "|" in drug_bank_ids:
            drug_bank_ids = drug_bank_ids.split("|")

        node = Node(identifier=chemical_id, name=chemical_name, kind="Compound",
                    sources=["Comparative Toxicgenomic Dataset"], license="?")

        node.add_alt_id(drug_bank_ids, id_type="DRUGBANK")
        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, CHEMICAL_NODES_CHECKPOINT)

    return nodes


def build_disease_nodes(**kwargs) -> List[Node]:
    diseases = pd.read_csv(DISEASES_PATH, names=DISEASE_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(DISEASE_NODES_CHECKPOINT):
            return Node.deserialize_bunch(DISEASE_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(diseases.iterrows()):
        # All of the diseases start with "MESH:" or "OMIM:"
        disease_prefix, disease_id = row["DiseaseID"][:4], row["DiseaseID"][5:]
        disease_name = row["DiseaseName"]

        node = Node(identifier=disease_id, name=disease_name, kind="Disease",
                    sources=["Comparative Toxicgenomic Dataset"], license="?")

        if disease_prefix == "MESH":
            node.add_alt_id(disease_id, id_type="MESH")
        elif disease_prefix == "OMIM":
            node.add_alt_id(disease_id, id_type="OMIM")
        else:
            raise ValueError(f"Unrecognized disease prefix: {disease_prefix}, expecting \"MESH\" or \"OMIM\".")

        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, DISEASE_NODES_CHECKPOINT)

    return nodes


def build_gene_nodes(**kwargs) -> List[Node]:
    genes = pd.read_csv(GENES_PATH, names=GENE_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(GENE_NODES_CHECKPOINT):
            return Node.deserialize_bunch(GENE_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(genes.iterrows()):
        gene_id = row["GeneID"]
        gene_name = row["GeneName"]

        node = Node(identifier=gene_id, name=gene_name, kind="Gene",
                    sources=["Comparative Toxicgenmoic Dataset"], license="?")

        node.add_alt_id(gene_id, id_type="NCBI")
        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, GENE_NODES_CHECKPOINT)

    return nodes


def build_pathway_nodes(**kwargs) -> List[Node]:
    pathways = pd.read_csv(PATHWAYS_PATH, names=PATHWAY_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(PATHWAY_NODES_CHECKPOINT):
            return Node.deserialize_bunch(PATHWAY_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(pathways.iterrows()):
        # Starts with either "REACT" or "KEGG"
        pathway_id = row["PathwayID"]
        pathway_name = row["PathwayName"]

        if pathway_id[:5] == "REACT":
            pathway_id = row["PathwayID"][6:]
            id_type = "REACTOME"
        elif pathway_id[:4] == "KEGG":
            pathway_id = row["PathwayID"][5:]
            id_type = "KEGG"
        else:
            raise ValueError(f"Unrecognized pathway prefix for {pathway_id}")

        node = Node(identifier=pathway_id, name=pathway_name, kind="Pathway",
                    sources=["Comparative Toxicgenomic Dataset"], license="?")

        node.add_alt_id(pathway_id, id_type=id_type)
        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, PATHWAY_NODES_CHECKPOINT)

    return nodes

#
# def _get_umls_attributes(umls: pd.DataFrame, mesh_id: str) -> Tuple[List[str], List[str]]:
#     df = umls[(umls["CODE"] == mesh_id) & (umls["SAB"] == "MSH")]
#     cuis = list(df["CUI"].unique())
#     mesh_ids = list(df["CODE"].unique())
#
#     return cuis, mesh_ids
#
#
# def add_chemical_nodes(graph_nodes: List[Node], dataframe: pd.DataFrame, umls: pd.DataFrame) -> List[Node]:
#     compound_nodes = list(filter(lambda x: x.kind == "Compound", graph_nodes))
#     chemical_ids = dataframe["ChemicalID"].unique()
#
#     log.info("Building chemical nodes.")
#     for chemical_id in tqdm(chemical_ids):
#         umls_cuis, mesh_ids = _get_umls_attributes(umls, chemical_id)
#         attribute_set = set(umls_cuis + mesh_ids)
#
#         target_node = Node.find_node(attribute_set, compound_nodes)
#
#         if target_node is None:
#             log.info("No node with overlapping attributes found, creating new node.")
#             chemical_name = dataframe[dataframe["ChemicalID"] == chemical_id]["ChemicalName"].iloc[0]
#             # TODO: Figure out the license here!
#             new_node = Node(identifier=umls_cuis[0], name=chemical_name, kind="Compound",
#                             sources="Comparative Toxicgenomic Dataset", license="?")
#             graph_nodes.append(new_node)
#         else:
#             log.info("Found existing node, adding metadata and moving on.")
#             target_node.add_cui(umls_cuis)
#             target_node.add_mesh_id(mesh_ids)
#
#     Node.serialize_bunch(graph_nodes, GRAPH_NODES_CHECKPOINT)
#
#     return graph_nodes
#
#
# def add_disease_nodes(graph_nodes: List[Node], dataframe: pd.DataFrame, umls: pd.DataFrame) -> List[Node]:
#     disease_nodes = list(filter(lambda x: x.kind == "Disease", graph_nodes))
#
#     # filter out the DiseaseID that do not start with "MESH:", there are 5 records that start with "OMIM:"
#     disease_ids = dataframe["DiseaseID"].unique()
#     disease_ids = list(filter(lambda x: x[:5] == "MESH:", disease_ids))
#     disease_ids = list(map(lambda x: x[5:], disease_ids))
#
#     log.info("Building disease nodes.")
#     for disease_id in tqdm(disease_ids):
#         umls_cuis, mesh_ids = _get_umls_attributes(umls, disease_id)
#         attribute_set = set(umls_cuis + mesh_ids)
#
#         target_node = Node.find_node(attribute_set, disease_nodes)
#
#         if target_node is None:
#             log.info("No node with overlapping attributes found, creating new node.")
#             chemical_name = dataframe[dataframe["DiseaseID"] == disease_id]["DiseaseName"].iloc[0]
#             # TODO: Figure out the license here!
#             new_node = Node(identifier=umls_cuis[0], name=chemical_name, kind="Disease",
#                             sources="Comparative Toxicgenomic Dataset", license="?")
#             graph_nodes.append(new_node)
#         else:
#             log.info("Found existing node, adding metadata and moving on.")
#             target_node.add_cui(umls_cuis)
#             target_node.add_mesh_id(mesh_ids)
#
#     Node.serialize_bunch(graph_nodes, GRAPH_NODES_CHECKPOINT)
#
#     return graph_nodes
