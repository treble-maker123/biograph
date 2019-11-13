import os
from typing import List

import pandas as pd
from tqdm import tqdm

from utils.edge import Edge
from utils.logger import log
from utils.node import Node

NODES_CHECKPOINT = "outputs/repodb_nodes.checkpoint.json"
EDGES_CHECKPOINT = "outputs/repodb_edges.checkpoint.json"

REPODB_FILE_PATH = "data/repodb.csv"


def build_nodes(**kwargs) -> List[Node]:
    force_rebuild = kwargs.get("force_rebuild", False)
    save_checkpoint = kwargs.get("save_checkpoint", True)

    if not force_rebuild:
        if os.path.exists(NODES_CHECKPOINT):
            return Node.deserialize_bunch(NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    repodb = pd.read_csv(REPODB_FILE_PATH)

    nodes = []

    drug_ids = repodb["drug_id"].unique()
    disease_ids = repodb["ind_id"].unique()

    for drug_id in drug_ids:
        match = repodb[repodb["drug_id"] == drug_id]
        drug_names = match["drug_name"].unique()
        assert len(drug_names) == 1

        node = Node(drug_id, drug_names[0], kind="Compound", sources=["RepoDB"])
        nodes.append(node)

    for disease_id in disease_ids:
        match = repodb[repodb["ind_id"] == disease_id]
        disease_names = match["ind_name"].unique()
        assert len(disease_names) == 1

        node = Node(disease_id, disease_names[0], kind="Disease", sources=["RepoDB"])
        nodes.append(node)

    log.info(f"Built {len(nodes)} nodes from RepoDB.")

    if save_checkpoint:
        log.info("Checkpointing nodes...")
        Node.serialize_bunch(nodes, NODES_CHECKPOINT)

    return nodes


def build_edges(nodes: List[Node], **kwargs) -> List[Edge]:
    """
    According to https://prsinfo.clinicaltrials.gov/definitions.html, we cannot assume that Suspended, Terminated, or
    Withdrawn implies failed trial.
    """
    include_inverse = kwargs.get("include_inverse", False)
    force_rebuild = kwargs.get("force_rebuild", False)
    save_checkpoint = kwargs.get("save_checkpoint", True)

    if not force_rebuild:
        if os.path.exists(EDGES_CHECKPOINT):
            return Edge.deserialize_bunch(EDGES_CHECKPOINT, nodes)
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    repodb = pd.read_csv(REPODB_FILE_PATH)

    edges = []

    node_dict = {n.identifier: n for n in nodes}

    for repodb_edge in tqdm(repodb.iterrows()):
        kind = repodb_edge[1]["status"]

        # skip the non-"Approved" ones
        if kind != "Approved":
            continue

        src_node = node_dict[repodb_edge[1]["drug_id"]]
        dst_node = node_dict[repodb_edge[1]["ind_id"]]
        forward_edge = Edge(src_node, dst_node, kind="treats", sources=["RepoDB"])
        edges.append(forward_edge)

        if include_inverse:
            backward_edge = Edge(dst_node, src_node, kind="treats_inv", sources=["RepoDB"])
            edges.append(backward_edge)

    if save_checkpoint:
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, EDGES_CHECKPOINT)

    return edges
