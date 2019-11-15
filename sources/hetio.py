import bz2
import json
import sqlite3 as lite
from typing import Dict

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from schema import GRAPH_DATABASE_PATH
from schema import Node, Edge, Source, DrugBank, UMLS, MESH
from utils import log
from utils.sources import load_disease_ontology
from pdb import set_trace

"""
meta-edges in het.io

['Anatomy', 'Gene', 'downregulates', 'both'],
 ['Anatomy', 'Gene', 'expresses', 'both'],
 ['Anatomy', 'Gene', 'upregulates', 'both'],
 ['Compound', 'Compound', 'resembles', 'both'],
 ['Compound', 'Disease', 'palliates', 'both'],
 ['Compound', 'Disease', 'treats', 'both'],
 ['Compound', 'Gene', 'binds', 'both'],
 ['Compound', 'Gene', 'downregulates', 'both'],
 ['Compound', 'Gene', 'upregulates', 'both'],
 ['Compound', 'Side Effect', 'causes', 'both'],
 ['Disease', 'Anatomy', 'localizes', 'both'],
 ['Disease', 'Disease', 'resembles', 'both'],
 ['Disease', 'Gene', 'associates', 'both'],
 ['Disease', 'Gene', 'downregulates', 'both'],
 ['Disease', 'Gene', 'upregulates', 'both'],
 ['Disease', 'Symptom', 'presents', 'both'],
 ['Gene', 'Biological Process', 'participates', 'both'],
 ['Gene', 'Cellular Component', 'participates', 'both'],
 ['Gene', 'Gene', 'covaries', 'both'],
 ['Gene', 'Gene', 'interacts', 'both'],
 ['Gene', 'Gene', 'regulates', 'forward'],
 ['Gene', 'Molecular Function', 'participates', 'both'],
 ['Gene', 'Pathway', 'participates', 'both'],
 ['Pharmacologic Class', 'Compound', 'includes', 'both']
 
'Anatomy','Biological Process','Cellular Component','Compound','Disease','Gene','Molecular Function',
'Pathway','Pharmacologic Class','Side Effect','Symptom'
"""

HETIO_FILE_PATH = "data/integrate/data/hetnet.json.bz2"

HETIO_SOURCE = "het.io"


def load_hetio() -> Dict:
    with bz2.open(HETIO_FILE_PATH) as file:
        return json.load(file)


def build_nodes(engine: Engine):
    log.info("Loading het.io nodes and edges.")
    hetio = load_hetio()

    log.info("Loading disease ontology metadata.")
    do = load_disease_ontology()

    Session = sessionmaker(bind=engine)
    session = Session()

    log.info(f"Building het.io nodes into the database.")

    for h in tqdm(hetio["nodes"]):
        name, kind = h["name"], h["kind"]
        nodes_query = session.query(Node).filter_by(name=name, kind=kind)

        if len(nodes_query.all()) > 0:
            node = nodes_query.first()
        else:
            node = Node(name=name, kind=kind, license=h["data"].get("license", ""))

        sources = h["data"].get("source", None) or h["data"].get("sources", [])

        # Loading source metadata into the database
        if sources is None:
            sources = ["het.io"]

        if type(sources) == str:
            sources = [sources, "het.io"]

        for source in sources:
            sources_query = session.query(Source).filter_by(name=source)

            if len(sources_query.all()) == 0:
                db_source = Source(name=source)
                session.add(db_source)
            else:
                db_source = sources_query.first()

            node.sources.append(db_source)

        # Loading identifiers for the node
        if h["kind"] == "Compound":
            drug_bank_id = h["identifier"]
            db_drug_bank = session.query(DrugBank).filter_by(id=h["identifier"]).first()  # unique, so first should work

            if db_drug_bank is None:
                db_drug_bank = DrugBank(id=drug_bank_id)
                node.drug_bank_ids.append(db_drug_bank)
        elif h["kind"] == "Disease":
            doid = h["identifier"]

            try:
                xref = do[doid].other["xref"]
            except Exception as _:
                log.info(f"Error looking up {doid}, skipping.")

            umls_cuis = list(filter(lambda x: x[:8] == "UMLS_CUI", xref))
            umls_cuis = list(map(lambda x: x[9:], umls_cuis))

            mesh_ids = list(filter(lambda x: x[:4] == "MESH", xref))
            mesh_ids = list(map(lambda x: x[5:], mesh_ids))

            for umls_cui in umls_cuis:
                db_umls = session.query(UMLS).filter_by(id=umls_cui).first()  # unique, so first should work

                if db_umls is None:
                    db_umls = UMLS(id=umls_cui)
                    node.umls_ids.append(db_umls)

            for mesh_id in mesh_ids:
                db_mesh = session.query(MESH).filter_by(id=mesh_id).first()  # unique, so first should work

                if db_mesh is None:
                    db_mesh = MESH(id=mesh_id)
                    node.mesh_ids.append(db_mesh)

        session.commit()
    session.close()


def build_edges(engine: Engine):
    log.info("Loading het.io nodes and edges.")
    hetio = load_hetio()

    Session = sessionmaker(bind=engine)
    session = Session()

    nodes = list(map(lambda n: (n["identifier"], n["name"], n["kind"]), hetio["nodes"]))
    node_dict = {(n[2], n[0]): n[1] for n in nodes}

    log.info("Loading pandas look-up table.")
    conn = lite.connect(GRAPH_DATABASE_PATH.split("sqlite:///")[1])
    nodes_df = pd.read_sql("SELECT * from nodes", con=conn)
    conn.close()

    log.info(f"Building het.io edges into the database.")

    db_hetio_source = session.query(Source).filter_by(name="het.io").first()

    for h in tqdm(hetio["edges"]):
        head_kind, head_identifier = h["source_id"]
        tail_kind, tail_identifier = h["target_id"]
        head_name = node_dict[(head_kind, head_identifier)]
        tail_name = node_dict[(tail_kind, tail_identifier)]

        head_node_id = nodes_df[(nodes_df["name"] == head_name) & (nodes_df["kind"] == head_kind)]
        tail_node_id = nodes_df[(nodes_df["name"] == tail_name) & (nodes_df["kind"] == tail_kind)]

        if len(head_node_id) != 1:
            tqdm.write(f"Expecting 1 head node, queried {len(head_node_id)}, skipping relation...")
            continue
        else:
            head_node_id = head_node_id.iloc[0]["id"]

        if len(tail_node_id) != 1:
            tqdm.write(f"Expecting 1 tail node, queried {len(tail_node_id)}, skipping relation...")
            continue
        else:
            tail_node_id = tail_node_id.iloc[0]["id"]

        edge_kind = h["kind"]
        existing_edge_kinds = session.query(Edge.kind).filter_by(head_id=head_node_id, tail_id=tail_node_id).all()

        if len(existing_edge_kinds) > 0 and (edge_kind,) in existing_edge_kinds:
            tqdm.write(
                f"Edges of the kind {edge_kind} already exists between {head_node_id} and {tail_node_id}, skipping...")
            continue

        forward_edge = Edge(kind=edge_kind)
        forward_edge.head_id, forward_edge.tail_id = head_node_id, tail_node_id

        forward_edge.sources.append(db_hetio_source)

        if h["direction"] == "both":
            backward_edge = Edge(kind=edge_kind + "_inv")
            backward_edge.head_id, backward_edge.tail_id = tail_node_id, head_node_id
            backward_edge.sources.append(db_hetio_source)

        session.commit()
    session.close()
