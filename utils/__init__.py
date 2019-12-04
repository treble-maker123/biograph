from utils.edge import Edge
from utils.logger import log
from utils.node import Node
from utils.sources import load_umls, load_hetio, load_repodb, load_disease_ontology
from typing import *
from tqdm import tqdm


def overlap(first: List[Node], second: List[Node]) -> int:
    num_overlap = 0

    for fir in tqdm(first):
        for sec in second:
            if fir == sec:
                num_overlap += 1

    return num_overlap
