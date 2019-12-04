"""
Nodes are from Comparative Toxicgenomic Dataset
http://ctdbase.org/downloads/
Chemical vocabulary: http://ctdbase.org/reports/CTD_chemicals.csv.gz
Disease vocabulary: http://ctdbase.org/reports/CTD_diseases.csv.gz
Gene vocabulary: http://ctdbase.org/reports/CTD_genes.csv.gz
Pathway vocabulary: http://ctdbase.org/reports/CTD_pathways.csv.gz
------------------------------------------------------------------------------------------------------------------------
Edges are from Comparative Toxicgenomic Dataset
http://ctdbase.org/downloads/
Chemical-gene: http://ctdbase.org/reports/CTD_chem_gene_ixns.csv.gz
Chemical–disease: http://ctdbase.org/reports/CTD_chemicals_diseases.csv.gz
Chemical-pathway: http://ctdbase.org/reports/CTD_chem_pathways_enriched.csv.gz
Gene–disease: http://ctdbase.org/reports/CTD_genes_diseases.csv.gz (HUGE FILE, 1GB)
Gene–pathway: http://ctdbase.org/reports/CTD_genes_pathways.csv.gz
Disease–pathway: http://ctdbase.org/reports/CTD_diseases_pathways.csv.gz
"""
import math
import os
from typing import *

import pandas as pd
from tqdm import tqdm

from utils import Node, Edge
from utils.logger import log
from pdb import set_trace
from itertools import chain

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

CHEM_GENE_COLUMNS = [
    "ChemicalName", "ChemicalID", "CasRN", "GeneSymbol", "GeneID", "GeneForms", "Organism", "OrganismID", "Interaction",
    "InteractionActions", "PubMedIDs"
]

CHEM_PATHWAY_COLUMNS = [
    "ChemicalName", "ChemicalID", "CasRN", "PathwayName", "PathwayID", "PValue", "CorrectedPValue", "TargetMatchQty",
    "TargetTotalQty", "BackgroundMatchQty", "BackgroundTotalQty"
]

DISEASE_PATHWAY_COLUMNS = [
    "DiseaseName", "DiseaseID", "PathwayName", "PathwayID", "InferenceGeneSymbol"
]

GENE_PATHWAY_COLUMNS = [
    "GeneSymbol", "GeneID", "PathwayName", "PathwayID"
]

CHEM_DISEASE_COLUMNS = [
    "ChemicalName", "ChemicalID", "CasRN", "DiseaseName", "DiseaseID", "DirectEvidence", "InferenceGeneSymbol",
    "InferenceScore", "OmimIDs", "PubMedIDs"
]

GENE_DISEASE_COLUMNS = [
    "GeneSymbol", "GeneID", "DiseaseName", "DiseaseID", "DirectEvidence", "InferenceChemicalName", "InferenceScore",
    "OmimIDs", "PubMedIDs"
]

CHEMICALS_PATH = "data/CTD_chemicals.csv"
DISEASES_PATH = "data/CTD_diseases.csv"
GENES_PATH = "data/CTD_genes.csv"
PATHWAYS_PATH = "data/CTD_pathways.csv"
CHEM_GENE_PATH = "data/CTD_chem_gene_ixns.csv"
CHEM_PATHWAY_PATH = "data/CTD_chem_pathways_enriched.csv"
CHEM_DISEASE_PATH = "data/CTD_chemicals_diseases.csv"
GENE_DISEASE_PATH = "data/CTD_genes_diseases.csv"
GENE_PATHWAY_PATH = "data/CTD_genes_pathways.csv"

CHEMICAL_NODES_CHECKPOINT = "outputs/ctd_chemical_nodes.checkpoint.json"
DISEASE_NODES_CHECKPOINT = "outputs/ctd_disease_nodes.checkpoint.json"
GENE_NODES_CHECKPOINT = "outputs/ctd_gene_nodes.checkpoint.json"
PATHWAY_NODES_CHECKPOINT = "outputs/ctd_pathway_nodes.checkpoint.json"
NODES_CHECKPOINT = "outputs/ctd_nodes.checkpoint.json"

CHEM_GENE_CHECKPOINT = "outputs/ctd_chem_gene_edges.checkpoint.json"
CHEM_DISEASE_CHECKPOINT = "outputs/ctd_chem_disease_edges.checkpoint.json"
GENE_DISEASE_CHECKPOINT = "outputs/ctd_gene_disease_edges.checkpoint.json"
GENE_PATHWAY_CHECKPOINT = "outputs/ctd_gene_pathway_edges.checkpoint.json"
EDGES_CHECKPOINT = "outputs/ctd_edges.checkpoint.json"


def build_nodes(**kwargs) -> Tuple:
    chemical_nodes = build_chemical_nodes(**kwargs)
    disease_nodes = build_disease_nodes(**kwargs)
    gene_nodes = build_gene_nodes(**kwargs)
    pathway_nodes = build_pathway_nodes(**kwargs)

    return chemical_nodes, disease_nodes, gene_nodes, pathway_nodes


def build_edges(**kwargs) -> List[Edge]:
    chemical_nodes, disease_nodes, gene_nodes, pathway_nodes = \
        build_nodes(**kwargs)

    # chemical-gene
    log.info("Building chemical-gene edges...")
    chem_gene_edges = build_chem_gene_edges(chemical_nodes, gene_nodes, **kwargs)

    # chemical-disease
    log.info("Building chemical-disease edges...")
    chem_disease_edges = build_chem_disease_edges(chemical_nodes, disease_nodes, **kwargs)

    # [x] chemical-pathway

    # [x] disease-pathway

    # gene-disease
    log.info("Building gene-disease edges...")
    gene_disease_edges = build_gene_disease_edges(gene_nodes, disease_nodes, **kwargs)

    # gene-pathway
    log.info("Building gene-pathway edges...")
    gene_pathway_edges = build_gene_pathway_edges(gene_nodes, pathway_nodes, **kwargs)

    return [*chem_gene_edges, *chem_disease_edges, *gene_disease_edges, *gene_pathway_edges]


def build_chemical_nodes(**kwargs) -> List[Node]:
    log.info("Building chemical nodes...")

    umls = kwargs.get("umls", None)

    chemicals = pd.read_csv(CHEMICALS_PATH, names=CHEMICAL_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(CHEMICAL_NODES_CHECKPOINT):
            return Node.deserialize_bunch(CHEMICAL_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(chemicals.iterrows()):
        # All of the chemicals start with "MESH:"
        chemical_id = row["ChemicalID"][5:]
        chemical_name = row["ChemicalName"]

        # drug bank ids are delimited by "|"
        drug_bank_ids = row["DrugBankIDs"]

        node = Node(identifier=chemical_id, name=chemical_name, kind="Compound",
                    sources=["CTD"], license="?")

        if type(drug_bank_ids) == float and math.isnan(drug_bank_ids):
            drug_bank_ids = ""

        if "|" in drug_bank_ids:
            drug_bank_ids = drug_bank_ids.split("|")

        node.add_mesh_id(chemical_id)

        if not (drug_bank_ids == ""):
            node.add_drug_bank_id(drug_bank_ids)

        nodes.append(node)

    identifiers = list(set(map(lambda x: x.identifier, nodes)))
    drug_bank_ids = list(chain(*list(map(lambda x: x.drug_bank_ids, nodes))))
    umls = umls[umls["CODE"].isin(identifiers) | umls["CODE"].isin(drug_bank_ids)]

    log.info("Adding UMLS metadata to nodes")

    for node in tqdm(nodes):
        umls_matching_records = umls[(umls["CODE"] == node.identifier) | umls["CODE"].isin(node.drug_bank_ids)]
        if len(umls_matching_records) > 0:
            umls_ids = list(set(umls_matching_records["CUI"]))
            node.add_cui(umls_ids)
        else:
            tqdm.write("No UMLS records.")
            continue

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, CHEMICAL_NODES_CHECKPOINT)

    return nodes


def build_disease_nodes(**kwargs) -> List[Node]:
    log.info("Building disease nodes...")

    umls = kwargs.get("umls", None)

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
                    sources=["CTD"], license="?")

        if disease_prefix == "MESH":
            node.add_mesh_id(disease_id)
        elif disease_prefix == "OMIM":
            pass
        else:
            raise ValueError(f"Unrecognized disease prefix: {disease_prefix}, expecting \"MESH\" or \"OMIM\".")

        nodes.append(node)

    identifiers = list(set(map(lambda x: x.identifier, nodes)))
    umls = umls[(umls["SAB"] == "MSH") & umls["CODE"].isin(identifiers)]

    log.info("Adding UMLS metadata to nodes")

    for node in tqdm(nodes):
        umls_matching_records = umls[umls["CODE"] == node.identifier]
        if len(umls_matching_records) > 0:
            umls_ids = list(set(umls_matching_records["CUI"]))
            mesh_ids = list(set(umls_matching_records["CODE"]))
            node.add_cui(umls_ids)
            node.add_mesh_id(mesh_ids)
        else:
            tqdm.write("No UMLS records.")
            continue

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, DISEASE_NODES_CHECKPOINT)

    return nodes


def build_gene_nodes(**kwargs) -> List[Node]:
    log.info("Building gene nodes...")
    genes = pd.read_csv(GENES_PATH, names=GENE_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(GENE_NODES_CHECKPOINT):
            return Node.deserialize_bunch(GENE_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(genes.iterrows()):
        gene_id = row["GeneID"]
        gene_name = row["GeneSymbol"]

        node = Node(identifier=gene_id, name=gene_name, kind="Gene",
                    sources=["CTD"], license="?")
        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, GENE_NODES_CHECKPOINT)

    return nodes


def build_pathway_nodes(**kwargs) -> List[Node]:
    log.info("Building pathway nodes...")
    pathways = pd.read_csv(PATHWAYS_PATH, names=PATHWAY_COLUMNS)
    nodes = []

    if not kwargs.get("force_rebuild", False):
        if os.path.exists(PATHWAY_NODES_CHECKPOINT):
            return Node.deserialize_bunch(PATHWAY_NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    for idx, row in tqdm(pathways.iterrows()):
        pathway_id = row["PathwayID"]
        pathway_name = row["PathwayName"]

        node = Node(identifier=pathway_id, name=pathway_name, kind="Pathway",
                    sources=["CTD"], license="?")
        nodes.append(node)

    if kwargs.get("save_checkpoint", True):
        log.info("Check-pointing nodes...")
        Node.serialize_bunch(nodes, PATHWAY_NODES_CHECKPOINT)

    return nodes


def build_chem_gene_edges(chem_nodes: List[Node], gene_nodes: List[Node], **kwargs) -> List[Edge]:
    if not kwargs.get("force_rebuild", False):
        if os.path.exists(CHEM_GENE_CHECKPOINT):
            return Edge.deserialize_bunch(CHEM_GENE_CHECKPOINT, [*chem_nodes, *gene_nodes])
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    edges = []
    df = pd.read_csv(CHEM_GENE_PATH, names=CHEM_GENE_COLUMNS)

    chem_dict = {(chem.name, chem.identifier): chem for chem in chem_nodes}
    gene_dict = {(gene.name, gene.identifier): gene for gene in gene_nodes}

    for idx, row in tqdm(df.iterrows()):
        df_chem = row["ChemicalName"], row["ChemicalID"]
        df_gene = row["GeneSymbol"], row["GeneID"]
        try:
            chem_node = chem_dict[df_chem]
            gene_node = gene_dict[df_gene]
        except KeyError as e:
            tqdm.write(f"Key error, skipping: {e}")
            continue

        # http://ctdbase.org/help/ixnQueryHelp.jsp;jsessionid=C6E8588917BC36AE8CB619D199219134#actionType
        relations = list(set(row["InteractionActions"].split("|")))

        for rel in relations:
            edge = Edge(source=chem_node, destination=gene_node, kind=rel, sources=["CTD"])
            edges.append(edge)

    if kwargs.get("save_checkpoint", True):
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, CHEM_GENE_CHECKPOINT)

    return edges


def build_chem_disease_edges(chem_nodes: List[Node], disease_nodes: List[Node], **kwargs) -> List[Edge]:
    if not kwargs.get("force_rebuild", False):
        if os.path.exists(CHEM_DISEASE_CHECKPOINT):
            return Edge.deserialize_bunch(CHEM_DISEASE_CHECKPOINT, [*chem_nodes, *disease_nodes])
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    edges = []
    df = pd.read_csv(CHEM_DISEASE_PATH, names=CHEM_DISEASE_COLUMNS)
    df = df[(df["DirectEvidence"] == "therapeutic") | (df["DirectEvidence"] == "marker/mechanism")]

    chem_dict = {(chem.name, chem.identifier): chem for chem in chem_nodes}
    disease_dict = {(disease.name, disease.identifier): disease for disease in disease_nodes}

    for idx, row in tqdm(df.iterrows()):
        df_chem = row["ChemicalName"], row["ChemicalID"]
        df_disease = row["DiseaseName"], row["DiseaseID"][5:]
        try:
            chem_node = chem_dict[df_chem]
            disease_node = disease_dict[df_disease]
        except KeyError as e:
            tqdm.write(f"Key error, skipping: {e}")
            continue

        relation = row["DirectEvidence"]
        # changing all "therapeutic" labels to "treats" to match repoDB
        relation = "treats" if relation == "therapeutic" else relation

        edge = Edge(source=chem_node, destination=disease_node, kind=relation, sources=["CTD"])
        edges.append(edge)

    if kwargs.get("save_checkpoint", True):
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, CHEM_DISEASE_CHECKPOINT)

    return edges


def build_gene_disease_edges(gene_nodes: List[Node], disease_nodes: List[Node], **kwargs) -> List[Edge]:
    if not kwargs.get("force_rebuild", False):
        if os.path.exists(GENE_DISEASE_CHECKPOINT):
            return Edge.deserialize_bunch(GENE_DISEASE_CHECKPOINT, [*gene_nodes, *disease_nodes])
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    edges = []
    df = pd.read_csv(GENE_DISEASE_PATH, names=GENE_DISEASE_COLUMNS)
    df = df[(df["DirectEvidence"] == "marker/mechanism") |
            (df["DirectEvidence"] == "therapeutic") |
            (df["DirectEvidence"] == "marker/mechanism|therapeutic")]

    gene_dict = {(gene.name, gene.identifier): gene for gene in gene_nodes}
    disease_dict = {(disease.name, disease.identifier): disease for disease in disease_nodes}

    for idx, row in tqdm(df.iterrows()):
        df_gene = row["GeneSymbol"], row["GeneID"]
        df_disease = row["DiseaseName"], row["DiseaseID"][5:]
        try:
            gene_node = gene_dict[df_gene]
            disease_node = disease_dict[df_disease]
        except KeyError as e:
            tqdm.write(f"Key error, skipping: {e}")
            continue

        relation = row["DirectEvidence"]
        edge = Edge(source=gene_node, destination=disease_node, kind=relation, sources=["CTD"])
        edges.append(edge)

    if kwargs.get("save_checkpoint", True):
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, GENE_DISEASE_CHECKPOINT)

    return edges


def build_gene_pathway_edges(gene_nodes: List[Node], pathway_nodes: List[Node], **kwargs) -> List[Edge]:
    if not kwargs.get("force_rebuild", False):
        if os.path.exists(GENE_PATHWAY_CHECKPOINT):
            return Edge.deserialize_bunch(GENE_PATHWAY_CHECKPOINT, [*gene_nodes, *pathway_nodes])
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    edges = []
    df = pd.read_csv(GENE_PATHWAY_PATH, names=GENE_PATHWAY_COLUMNS)

    gene_dict = {(gene.name, gene.identifier): gene for gene in gene_nodes}
    pathway_dict = {(pathway.name, pathway.identifier): pathway for pathway in pathway_nodes}

    for idx, row in tqdm(df.iterrows()):
        df_gene = row["GeneSymbol"], row["GeneID"]
        df_pathway = row["PathwayName"], row["PathwayID"]
        try:
            gene_node = gene_dict[df_gene]
            pathway_node = pathway_dict[df_pathway]
        except KeyError as e:
            tqdm.write(f"Key error, skipping: {e}")
            continue

        relation = "connects"
        edge = Edge(source=gene_node, destination=pathway_node, kind=relation, sources=["CTD"])
        edges.append(edge)

    if kwargs.get("save_checkpoint", True):
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, GENE_PATHWAY_CHECKPOINT)

    return edges
