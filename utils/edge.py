from typing import Union, List

from utils.node import Node


class Edge(object):
    def __init__(self, source: Node, destination: Node, kind: str, sources: Union[str, List[str]]):
        if type(sources) == str:
            sources = [sources]

        self._src = source
        self._dst = destination
        self._kind = kind
        self._sources = sources

    @property
    def source(self) -> Node:
        return self._src

    @property
    def destination(self) -> Node:
        return self._dst

    @property
    def kind(self) -> str:
        return self._kind
