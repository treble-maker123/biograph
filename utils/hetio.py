import os
from typing import List, Dict

import pandas as pd
from pronto import Ontology
from tqdm import tqdm

from utils.edge import Edge
from utils.logger import log
from utils.node import Node

NODES_CHECKPOINT = "outputs/hetio_nodes.checkpoint.json"
EDGES_CHECKPOINT = "outputs/hetio_edges.checkpoint.json"


def build_nodes(hetio: Dict, **kwargs) -> List[Node]:
    force_rebuild = kwargs.get("force_rebuild", False)
    save_checkpoint = kwargs.get("save_checkpoint", True)

    if not force_rebuild:
        if os.path.exists(NODES_CHECKPOINT):
            return Node.deserialize_bunch(NODES_CHECKPOINT)
        else:
            log.info("Node checkpoint does not exist, building nodes.")

    nodes = [Node(h["identifier"], h["name"], h["kind"],
                  h["data"].get("source", None) or h["data"].get("sources", []),
                  h["data"].get("license", None),
                  h["data"].get("url", None))
             for h in hetio["nodes"]]

    assert len(nodes) == len(hetio["nodes"])

    umls = kwargs.get("umls", None)
    do = kwargs.get("do", None)

    if umls is not None:
        nodes = add_compound_metadata(hetio, nodes, umls)

    if do is not None:
        nodes = add_disease_metadata(hetio, nodes, do)

    if save_checkpoint:
        log.info("Checkpointing nodes...")
        Node.serialize_bunch(nodes, NODES_CHECKPOINT)

    return nodes


def build_edges(hetio: Dict, nodes: List[Node], **kwargs) -> List[Edge]:
    force_rebuild = kwargs.get("force_rebuild", False)
    save_checkpoint = kwargs.get("save_checkpoint", True)

    if not force_rebuild:
        if os.path.exists(EDGES_CHECKPOINT):
            return Edge.deserialize_bunch(EDGES_CHECKPOINT, nodes)
        else:
            log.info("Edge checkpoint does not exist, building edges.")

    edges = []

    node_dict = {(n.kind, n.identifier): n for n in nodes}

    for hetio_edge in tqdm(hetio["edges"]):
        src_id = hetio_edge["source_id"]
        dst_id = hetio_edge["target_id"]
        src_node = node_dict[(src_id[0], src_id[1])]
        dst_node = node_dict[(dst_id[0], dst_id[1])]
        kind = hetio_edge["kind"]
        direction = hetio_edge["direction"]
        sources = hetio_edge["data"].get("source", None) or hetio_edge["data"].get("sources", [])

        forward = Edge(src_node, dst_node, kind, sources)

        if direction == "forward":
            edges.append(forward)
        elif direction == "both":
            backward = Edge(dst_node, src_node, kind + "_inv", sources)
            edges.append(forward)
            edges.append(backward)

    if save_checkpoint:
        log.info("Checkpointing edges...")
        Edge.serialize_bunch(edges, EDGES_CHECKPOINT)

    assert len(edges) > len(hetio["edges"])

    return edges


def add_anatomy_metadata(hetio: Dict, nodes: List[Node]) -> List[Node]:
    """
    Anatomy (source: Uberon)

        Ontolgoy URL: http://www.obofoundry.org/ontology/uberon.html
        Because pronto.Ontology throws error with the Uberon ontology, directly using the MeSH ID.

        Structure from het.io looks like this,

        {'kind': 'Anatomy',
        'identifier': 'UBERON:0001533',
        'name': 'subclavian artery',
        'data': {'source': 'Uberon',
        'license': 'CC BY 3.0',
        'url': 'http://purl.obolibrary.org/obo/UBERON_0001533',
        'mesh_id': 'D013348'}
    """
    log.info("Loading anatomy metadata.")

    anatomies = list(filter(lambda x: x["kind"] == "Anatomy", hetio["nodes"]))
    assert len(anatomies) > 0

    for a in tqdm(anatomies):
        matching_nodes = list(filter(lambda x: x.identifier == a["identifier"], nodes))
        assert len(matching_nodes) == 1

        node = matching_nodes[0]
        node.add_mesh_id(a["data"]["mesh_id"])
        assert node.mesh_ids == [a["data"]["mesh_id"]]

    log.info("Finished loading anatomy metadata.")

    return nodes


def add_compound_metadata(hetio: Dict, nodes: List[Node], umls: pd.DataFrame) -> List[Node]:
    """
    Compound (source: DrugBank)

        Structure from het.io looks like this,

        {'kind': 'Compound',
        'identifier': 'DB00201',
        'name': 'Caffeine',
        'data': {'license': 'CC BY-NC 4.0',
        'source': 'DrugBank',
        'inchikey': 'InChIKey=RYYVLZVUVIJVGH-UHFFFAOYSA-N',
        'inchi': 'InChI=1S/C8H10N4O2/c1-10-4-9-6-5(10)7(13)12(3)8(14)11(6)2/h4H,1-3H3',
        'url': 'http://www.drugbank.ca/drugs/DB00201'}}
    """
    log.info("Loading compounds metadata.")

    compounds = list(filter(lambda x: x["kind"] == "Compound", hetio["nodes"]))
    assert len(compounds) > 0

    counter = 0

    for compound in tqdm(compounds):
        compound_id = compound["identifier"]
        umls_records = umls[(umls["CODE"] == compound_id) & (umls["SAB"] == "DRUGBANK")]
        umls_cuis = list(umls_records["CUI"].unique())
        # assert len(umls_cuis) > 0  # some compounds do not have UMLS CUI

        if len(umls_cuis) > 0:
            matching_nodes = list(filter(lambda x: x.identifier == compound_id, nodes))
            assert len(matching_nodes) == 1

            node = matching_nodes[0]
            node.add_cui(umls_cuis)
            assert len(node.umls_cuis) == len(umls_cuis)
        else:
            counter += 1

    log.info(f"{counter}/{len(compounds)} compounds do not have matching records in UMLS.")

    log.info("Finished loading compounds metadata.")

    return nodes


def add_disease_metadata(hetio: Dict, nodes: List[Node], do: Ontology) -> List[Node]:
    """
    Disease (source: Disease Ontology)

        Structure from het.io looks like this,
        {'kind': 'Disease',
        'identifier': 'DOID:14227',
        'name': 'azoospermia',
        'data': {'source': 'Disease Ontology',
        'license': 'CC BY 3.0',
        'url': 'http://purl.obolibrary.org/obo/DOID_14227'}}
    """
    log.info("Loading diseases metadata.")

    diseases = list(filter(lambda x: x["kind"] == "Disease", hetio["nodes"]))
    assert len(diseases) > 0

    counter = 0

    for disease in tqdm(diseases):
        disease_id = disease["identifier"]

        try:
            xref = do[disease_id].other["xref"]
        except Exception as e:
            log.info(f"Error looking up {disease_id}, skipping.")
            continue

        umls_cuis = list(filter(lambda x: x[:8] == "UMLS_CUI", xref))
        umls_cuis = list(map(lambda x: x[9:], umls_cuis))

        mesh_ids = list(filter(lambda x: x[:4] == "MESH", xref))
        mesh_ids = list(map(lambda x: x[5:], mesh_ids))

        if len(umls_cuis) + len(mesh_ids) > 0:
            matching_nodes = list(filter(lambda x: x.identifier == disease_id, nodes))
            assert len(matching_nodes) == 1

            node = matching_nodes[0]
            node.add_cui(umls_cuis)
            node.add_mesh_id(mesh_ids)
            assert len(node.umls_cuis) == len(umls_cuis)
            assert len(node.mesh_ids) == len(mesh_ids)
        else:
            counter += 1

    print(f"{counter}/{len(diseases)} diseases do not have UMLS CUI or MeSH ID.")

    log.info("Finished loading diseases  metadata.")

    return nodes

# TODO: Metadata to be added

# ==================================================================================================================
# Biological Process (source: Gene Ontology)
#   Ontology URL: http://www.obofoundry.org/ontology/go.html
#   No MeSH or CUI reference, skipping.
#
# Structure from het.io looks like this,
#   {'kind': 'Biological Process',
#   'identifier': 'GO:0032474',
#   'name': 'otolith morphogenesis',
#   'data': {'source': 'Gene Ontology',
#    'license': 'CC BY 4.0',
#    'url': 'http://purl.obolibrary.org/obo/GO_0032474'}}
# ==================================================================================================================

# ==================================================================================================================
# Cellular Component (source: Gene Ontology)
#   Ontolgoy URL: http://www.obofoundry.org/ontology/uberon.html
#   No MeSH or CUI reference, skipping.
#
#   Structure from het.io looks like this,
#   {'kind': 'Cellular Component',
#   'identifier': 'GO:0000784',
#   'name': 'nuclear chromosome, telomeric region',
#   'data': {'source': 'Gene Ontology',
#    'license': 'CC BY 4.0',
#    'url': 'http://purl.obolibrary.org/obo/GO_0000784'}}
# ==================================================================================================================

# ==================================================================================================================
# Gene (source: Entrez Gene)
#
#   Structure from het.io looks like this,
#   {'kind': 'Gene',
#   'identifier': 5345,
#   'name': 'SERPINF2',
#   'data': {'description': 'serpin peptidase inhibitor, clade F (alpha-2 antiplasmin, pigment epithelium derived ',
#    'source': 'Entrez Gene',
#    'license': 'CC0 1.0',
#    'url': 'http://identifiers.org/ncbigene/5345',
#    'chromosome': '17'}}
# ==================================================================================================================

# ==================================================================================================================
# Molecular Function (source: Gene Ontology)
#
#   Structure from het.io looks like this,
#   {'kind': 'Molecular Function',
#   'identifier': 'GO:0031753',
#   'name': 'endothelial differentiation G-protein coupled receptor binding',
#   'data': {'source': 'Gene Ontology',
#    'license': 'CC BY 4.0',
#    'url': 'http://purl.obolibrary.org/obo/GO_0031753'}}
# ==================================================================================================================

# ==================================================================================================================
# Pathway (source: PID via Pathway Commons, Reactome via Pathway Commons, WikiPathways)
#
#   Structure from het.io looks like this,
#   {'kind': 'Pathway',
#   'identifier': 'PC7_3805',
#   'name': 'FCERI mediated Ca+2 mobilization',
#   'data': {'license': 'CC BY 4.0', 'source': 'Reactome via Pathway Commons'}}
# ==================================================================================================================

# ==================================================================================================================
# Pharmacologic Class (source: FDA via DrugCentral)
#
#   Structure from het.io looks like this,
#   {'kind': 'Pharmacologic Class',
#   'identifier': 'N0000007632',
#   'name': 'Thyroxine',
#   'data': {'class_type': 'Chemical/Ingredient',
#    'source': 'FDA via DrugCentral',
#    'license': 'CC BY 4.0',
#    'url': 'http://purl.bioontology.org/ontology/NDFRT/N0000007632'}}
# ==================================================================================================================

# ==================================================================================================================
# Side Effect (source: UMLS via SIDER 4.1)
#
#   Structure from het.io looks like this,
#   {'kind': 'Side Effect',
#   'identifier': 'C0023448',
#   'name': 'Lymphocytic leukaemia',
#   'data': {'source': 'UMLS via SIDER 4.1',
#    'license': 'CC BY-NC-SA 4.0',
#    'url': 'http://identifiers.org/umls/C0023448'}}
# ==================================================================================================================

# ==================================================================================================================
# Symptom (source: MeSH)
#
#   Structure from het.io looks like this,
#   {'kind': 'Symptom',
#   'identifier': 'D020150',
#   'name': 'Chorea Gravidarum',
#   'data': {'source': 'MeSH',
#    'license': 'CC0 1.0',
#    'url': 'http://identifiers.org/mesh/D020150'}}
# ==================================================================================================================
