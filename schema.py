from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from utils import log

GRAPH_DATABASE_PATH = "sqlite:///data/hetnet.sqlite"

Base = declarative_base()


class Node(Base):
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, index=True)
    kind = Column(String, index=True)
    license = Column(String)

    sources = relationship("Source", back_populates="nodes", secondary="source_node_associations")

    out_edges = relationship("Edge", back_populates="head", foreign_keys="Edge.head_id")
    in_edges = relationship("Edge", back_populates="tail", foreign_keys="Edge.tail_id")

    umls_ids = relationship("UMLS", back_populates="node")
    mesh_ids = relationship("MESH", back_populates="node")
    drug_bank_ids = relationship("DrugBank", back_populates="node")
    omim_ids = relationship("OMIM", back_populates="node")
    ncbi_ids = relationship("NCBI", back_populates="node")
    reactome_ids = relationship("Reactome", back_populates="node")
    kegg_ids = relationship("KEGG", back_populates="node")
    disease_ontology_ids = relationship("DiseaseOntology")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<Node(name={self.name}, kind={self.kind}, sources={self.sources})>"


class Edge(Base):
    __tablename__ = "edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    kind = Column(String, index=True)

    sources = relationship("Source", back_populates="edges", secondary="source_edge_associations")

    head_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    head = relationship("Node", back_populates="out_edges", foreign_keys="Edge.head_id")

    tail_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    tail = relationship("Node", back_populates="in_edges", foreign_keys="Edge.tail_id")

    ___table_args__ = (UniqueConstraint("head_id", "tail_id", "kind", name="_edge_uc"), )

    def __str__(self) -> str:
        return f"{self.head} {self.kind} {self.tail}"

    def __repr__(self) -> str:
        return f"<Edge(head={self.head}, tail={self.tail}, kind={self.kind}), sources={self.sources}>"


class SourceNodeAssociation(Base):
    __tablename__ = "source_node_associations"

    id = Column(Integer, primary_key=True, autoincrement=True)

    source_name = Column("source_name", String, ForeignKey("sources.name"), index=True)
    node_name = Column("node_name", String, ForeignKey("nodes.id"), index=True)


class SourceEdgeAssociation(Base):
    __tablename__ = "source_edge_associations"

    id = Column(Integer, primary_key=True, autoincrement=True)

    source_name = Column("source_name", String, ForeignKey("sources.name"), index=True)
    edge_name = Column("edge_name", String, ForeignKey("edges.id"), index=True)


class Source(Base):
    __tablename__ = "sources"

    name = Column(String, primary_key=True)

    nodes = relationship("Node", back_populates="sources", secondary="source_node_associations")
    edges = relationship("Edge", back_populates="sources", secondary="source_edge_associations")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<Source(name={self.name}>"


class UMLS(Base):
    __tablename__ = "umls_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="umls_ids")


class MESH(Base):
    __tablename__ = "mesh_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="mesh_ids")


class DrugBank(Base):
    __tablename__ = "drug_bank_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"),index=True)
    node = relationship("Node", back_populates="drug_bank_ids")


class OMIM(Base):
    """ Disease
    """
    __tablename__ = "omim_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="omim_ids")


class NCBI(Base):
    """ Gene
    """
    __tablename__ = "ncbi_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="ncbi_ids")


class Reactome(Base):
    """ pathways
    """
    __tablename__ = "reactome_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="reactome_ids")


class KEGG(Base):
    """ pathways
    """
    __tablename__ = "kegg_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="kegg_ids")


class DiseaseOntology(Base):
    __tablename__ = "disease_ontology_ids"

    id = Column(String, primary_key=True)

    node_id = Column(Integer, ForeignKey("nodes.id"), index=True)
    node = relationship("Node", back_populates="disease_ontology_ids")


if __name__ == "__main__":
    log.info("Creating database...")
    eng = create_engine(GRAPH_DATABASE_PATH, echo=True)
    Base.metadata.create_all(eng)
    eng.dispose()
