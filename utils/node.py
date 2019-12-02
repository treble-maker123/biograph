import json
import os
from copy import deepcopy
from typing import List, Union, Set, Dict
from tqdm import tqdm
import pandas as pd


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

            if "drug_bank_ids" not in self._metadata:
                self._metadata["drug_bank_ids"] = []
        else:
            self._metadata = {
                "identifier": identifier,
                "name": name,
                "kind": kind,
                "sources": sources,
                "license": license,
                "source_url": source_url,
                "mesh_ids": [],
                "umls_cuis": [],
                "drug_bank_ids": []
            }

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
        return set([self.identifier] + self.mesh_ids + self.umls_cuis + self.drug_bank_ids)

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
    def mesh_ids(self) -> List[str]:
        return self._metadata["mesh_ids"]

    @property
    def umls_cuis(self) -> List[str]:
        return self._metadata["umls_cuis"]

    @property
    def drug_bank_ids(self) -> List[str]:
        return self._metadata["drug_bank_ids"]

    def add_mesh_id(self, mesh_id_or_ids: Union[str, List[str]]):
        if type(mesh_id_or_ids) == list:
            self._metadata["mesh_ids"] += mesh_id_or_ids
        else:
            self._metadata["mesh_ids"].append(mesh_id_or_ids)

    def add_cui(self, cui_or_cuis: Union[str, List[str]]):
        if type(cui_or_cuis) == list:
            self._metadata["umls_cuis"] += cui_or_cuis
        else:
            self._metadata["umls_cuis"].append(cui_or_cuis)

    def add_drug_bank_id(self, drug_bank_id_or_ids: Union[str, List[str]]):
        if type(drug_bank_id_or_ids) == list:
            self._metadata["drug_bank_ids"] += drug_bank_id_or_ids
        else:
            self._metadata["drug_bank_ids"].append(drug_bank_id_or_ids)

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
