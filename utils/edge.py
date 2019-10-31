import json
import os
from copy import deepcopy
from typing import Union, List, Dict
from pdb import set_trace

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
        edges = []

        for metadata in metadata_set:
            source_node = list(filter(lambda x:
                                      x.identifier == metadata["source"][0] and
                                      x.name == metadata["source"][1] and
                                      x.kind == metadata["source"][2], nodes))
            destination_node = list(filter(lambda x:
                                           x.identifier == metadata["destination"][0] and
                                           x.name == metadata["destination"][1] and
                                           x.kind == metadata["destination"][2], nodes))

            assert len(source_node) == 1, f"Expecting 1 source node, found {len(source_node)}."
            assert len(destination_node) == 1, f"Expecting 1 destination node, found {len(destination_node)}."

            source_node = source_node[0]
            destination_node = destination_node[0]

            metadata["source"] = source_node
            metadata["destination"] = destination_node

            edge = cls(metadata=metadata)
            edges.append(edge)

        return edges
