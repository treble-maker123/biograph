import json
import os
from copy import deepcopy
from typing import Union, List, Dict

from tqdm import tqdm

from utils.node import Node


class Edge(object):
    def __init__(self,
                 source: Node = None,
                 destination: Node = None,
                 kind: str = None,
                 sources: Union[str, List[str]] = None,
                 metadata: Dict[str, object] = None):
        if type(sources) == str:
            sources = [sources]

        if metadata is not None:
            self._metadata = metadata
        else:
            self._metadata = {
                "source": source,
                "destination": destination,
                "kind": kind,
                "sources": sources
            }

    def __eq__(self, other: "Edge") -> bool:
        return hash(self) == hash(other)

    def __hash__(self) -> int:
        return hash((hash(self.source), hash(self.kind), hash(self.destination)))

    def __str__(self) -> str:
        return f"{self.source.name}\t{self.kind}\t{self.destination.name}"

    @property
    def source(self) -> Node:
        return self._metadata["source"]

    @property
    def destination(self) -> Node:
        return self._metadata["destination"]

    @property
    def kind(self) -> str:
        return self._metadata["kind"]

    @property
    def metadata(self) -> Dict[str, object]:
        metadata = deepcopy(self._metadata)
        metadata["source"] = [self.source.identifier, self.source.name, self.source.kind]
        metadata["destination"] = [self.destination.identifier, self.destination.name, self.destination.kind]

        return metadata

    @classmethod
    def serialize_bunch(cls, edges: List['Edge'], output_path: str) -> None:
        metadata_set = list(map(lambda x: x.metadata, edges))
        with open(output_path, "w") as file:
            json.dump(metadata_set, file)

    @classmethod
    def deserialize_bunch(cls, json_path: str, nodes: List[Node]) -> List['Edge']:
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Edge file at {json_path} does not exist!")

        with open(json_path, "r") as file:
            metadata_set = json.load(file)

        node_hash_table = {hash(n): n for n in nodes}
        edges = []

        for metadata in tqdm(metadata_set):
            metadata["source"] = node_hash_table[hash((metadata["source"][1],
                                                       metadata["source"][0],
                                                       metadata["source"][2]))]
            metadata["destination"] = node_hash_table[hash((metadata["destination"][1],
                                                            metadata["destination"][0],
                                                            metadata["destination"][2]))]

            edge = cls(metadata=metadata)
            edges.append(edge)

        return edges
