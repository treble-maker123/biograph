import json
import os
from copy import deepcopy
from itertools import chain
from typing import List, Union, Set, Dict, Optional

from tqdm import tqdm


class Node(object):
    def __init__(self,
                 identifier: str = "",
                 name: str = "",
                 kind: str = "",
                 sources: Union[str, List[str]] = "",
                 license: str = "",
                 source_url: str = "",
                 metadata: Dict[str, object] = None):
        if type(sources) == str:
            sources = [sources]

        if metadata is not None:
            self._metadata = metadata
        else:
            self._metadata = self._build_metadata()
            self._metadata["identifier"] = identifier
            self._metadata["name"] = name
            self._metadata["kind"] = kind
            self._metadata["sources"] = sources
            self._metadata["liense"] = license
            self._metadata["source_url"] = source_url

    def __eq__(self, other: 'Node') -> bool:
        intersection = self.attributes.intersection(other.attributes)
        return len(intersection) > 0

    def __ne__(self, other: 'Node') -> bool:
        intersection = self.attributes.intersection(other.attributes)
        return len(intersection) <= 0

    def __hash__(self) -> int:
        key = (self._metadata["name"], self._metadata["identifier"], self._metadata["kind"])
        return hash(key)

    def __str__(self) -> str:
        return self._metadata["name"]

    @property
    def metadata(self) -> Dict[str, object]:
        return deepcopy(self._metadata)

    @property
    def attributes(self) -> Set[str]:
        alt_ids = self._metadata["alt_ids"].values()
        return set(chain(*alt_ids))

    @property
    def identifier(self) -> str:
        return self._metadata["identifier"]

    @property
    def name(self) -> str:
        return self._metadata["name"]

    @property
    def kind(self) -> str:
        return self._metadata["kind"]

    @property
    def alt_id_types(self) -> List[str]:
        return list(self._metadata["alt_ids"].keys())

    @property
    def umls_ids(self) -> List[str]:
        return self._metadata["alt_ids"]["umls"]

    @property
    def omim_ids(self) -> List[str]:
        return self._metadata["alt_ids"]["omim"]

    @property
    def mesh_ids(self) -> List[str]:
        return self._metadata["alt_ids"]["mesh"]

    def add_alt_id(self, id_or_ids: Union[str, List[str]], id_type: str):
        assert id_type in self._metadata["alt_ids"].keys(), f"Provided id_type {id_type} does not exist in the " \
                                                            f"metadata, add it to the _build_metadata() method."

        if type(id_or_ids) == list:
            self._metadata["alt_ids"][id_type] += list(map(lambda x: f"{id_type}:{id_or_ids}", id_or_ids))
        else:
            self._metadata["alt_ids"][id_type].append(f"{id_type}:{id_or_ids}")

        self._metadata["alt_ids"][id_type] = list(set(self._metadata["alt_ids"][id_type]))

    def get_alt_id(self, id_type: str) -> List[str]:
        assert id_type in self._metadata["alt_ids"].keys(), f"Provided id_type {id_type} does not exist in the " \
                                                            f"metadata, add it to the _build_metadata() method."

        return self._metadata["alt_ids"][id_type]

    @classmethod
    def serialize_bunch(cls, nodes: List['Node'], output_path: str) -> None:
        metadata_set = list(map(lambda x: x.metadata, nodes))

        with open(output_path, "w") as file:
            json.dump(metadata_set, file)

    @classmethod
    def deserialize_bunch(cls, json_path: str) -> List['Node']:
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Node file at {json_path} does not exist!")

        with open(json_path, "r") as file:
            metadata_set = json.load(file)
        nodes = []

        for metadata in tqdm(metadata_set):
            node = cls(metadata=metadata)

            assert node.identifier is not None
            assert node.name is not None
            assert node.kind is not None

            nodes.append(node)

        return nodes

    @staticmethod
    def find_node(attributes: Set[str], nodes: List["Node"]) -> Optional["Node"]:
        for node in nodes:
            if attributes.intersection(node.attributes):
                return node
            else:
                return None

    @staticmethod
    def _build_metadata() -> Dict[str, Union[object, str, List[str], Dict]]:
        return {
            "identifier": "",
            "name": "",
            "kind": "",
            "sources": [],
            "license": "",
            "source_url": "",
            "alt_ids": {
                "MESH": [],
                "UMLS": [],
                "DRUGBANK": [],
                "OMIM": [],  # disease
                "NCBI": [],  # gene
                "REACTOME": [],  # pathways
                "KEGG": []  # pathways
            }
        }
