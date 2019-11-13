import os
import pickle
from typing import List

from networkx import MultiDiGraph

from utils.edge import Edge
from utils.logger import log
from utils.node import Node

GRAPH_NODES_CHECKPOINT = "outputs/graph_nodes.checkpoint.json"
GRAPH_EDGES_CHECKPOINT = "outputs/graph_edges.checkpoint.json"
GRAPH_CHECKPOINT = "outputs/graph.checkpoint"


def build_graph(nodes: List[Node], edges: List[Edge], **kwargs) -> MultiDiGraph:
    force_rebuild = kwargs.get("force_rebuild", False)
    save_checkpoint = kwargs.get("save_checkpoint", True)

    if not force_rebuild:
        if os.path.exists(GRAPH_CHECKPOINT):
            return load_graph()
    else:
        log.info("Graph checkpoint does not exist, building graph.")

    graph = MultiDiGraph()
    graph.add_nodes_from(nodes)

    ebunch = list(map(lambda x: (x.source, x.destination, x), edges))
    graph.add_edges_from(ebunch)

    assert graph.number_of_nodes() == len(nodes)
    assert graph.number_of_edges() == len(edges)

    if save_checkpoint:
        log.info("Checkpointing graph...")
        with open(GRAPH_CHECKPOINT, "wb") as file:
            pickle.dump(graph, file)

    return graph


def load_graph() -> MultiDiGraph:
    if os.path.exists(GRAPH_CHECKPOINT):
        log.info("Loading graph from checkpoint.")
        with open(GRAPH_CHECKPOINT, "rb") as file:
            return pickle.load(file)
    else:
        log.info("No graph checkpoint found, run build_graph.py to generate a graph!")
