from typing import List, Union, Set


class Node(object):
    def __init__(self, identifier: str, name: str, kind: str, sources: Union[str, List[str]], license: str,
                 source_url: str):
        if type(sources) == str:
            sources = [sources]

        self._identifier = identifier
        self._name = name
        self._kind = kind
        self._sources = sources
        self._license = license
        self._source_url = source_url

        self._mesh_ids = []
        self._umls_cuis = []

    def __eq__(self, other: 'Node') -> bool:
        intersection = self.attributes.intersection(other.attributes)
        return len(intersection) > 0

    def __ne__(self, other: 'Node') -> bool:
        intersection = self.attributes.intersection(other.attributes)
        return len(intersection) <= 0

    def __hash__(self) -> int:
        key = (self.name, self.identifier, self.kind)
        return hash(key)

    @property
    def attributes(self) -> Set[str]:
        return set([self.identifier] + self.mesh_ids + self.umls_cuis)

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def name(self) -> str:
        return self._name

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def sources(self) -> [str]:
        return self._sources

    @property
    def license(self) -> str:
        return self._license

    @property
    def source_url(self) -> str:
        return self._source_url

    @property
    def mesh_ids(self) -> List[str]:
        return self._mesh_ids

    @property
    def umls_cuis(self) -> List[str]:
        return self._umls_cuis

    def add_mesh_id(self, mesh_id_or_ids: Union[str, List[str]]):
        if type(mesh_id_or_ids) == list:
            self._mesh_ids += mesh_id_or_ids
        else:
            self._mesh_ids.append(mesh_id_or_ids)

    def add_cui(self, cui_or_cuis: Union[str, List[str]]):
        if type(cui_or_cuis) == list:
            self._umls_cuis += cui_or_cuis
        else:
            self._umls_cuis.append(cui_or_cuis)
