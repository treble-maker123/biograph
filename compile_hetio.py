from utils.node import Node
from utils.edge import Edge
from tqdm import tqdm

if __name__ == "__main__":
    print("Loading hetio nodes")
    hetio_nodes = Node.deserialize_bunch("outputs/hetio_nodes.checkpoint.json")
    hetio_edges = Edge.deserialize_bunch("outputs/hetio_edges.checkpoint.json", hetio_nodes)

    print("Loading repodb nodes")
    repodb_nodes = Node.deserialize_bunch("outputs/repodb_nodes.checkpoint.json")
    repodb_edges = Edge.deserialize_bunch("outputs/repodb_edges.checkpoint.json", repodb_nodes)

    print("Filtering het.io nodes")
    hetio_drug_nodes = list(filter(lambda x: x.kind == "Compound", hetio_nodes))
    hetio_disease_nodes = list(filter(lambda x: x.kind == "Disease", hetio_nodes))

    print("Writing repodb to data.txt")

    repodb_relations = []
    with open("outputs/data.txt", "w") as graph_file:
        for repodb_edge in tqdm(repodb_edges):
            drug, disease, kind = repodb_edge.source, repodb_edge.destination, repodb_edge.kind

            hetio_matching_drugs = list(filter(lambda d: d == drug, hetio_drug_nodes))
            hetio_matching_diseases = list(filter(lambda d: d == disease, hetio_disease_nodes))

            if len(hetio_matching_drugs) > 0 and len(hetio_matching_diseases) > 0:
                assert len(hetio_matching_drugs) == 1
                assert len(hetio_matching_diseases) == 1

                hetio_drug = hetio_matching_drugs[0]
                hetio_disease = hetio_matching_diseases[0]

                head_name, head_kind = hetio_drug.name, "Compound"
                tail_name, tail_kind = hetio_disease.name, "Disease"

                repodb_relations.append((head_name, tail_name))
                line = f"{head_name}:{head_kind}\ttreats\t{tail_name}:{tail_kind}\n"

                graph_file.write(line)
            else:
                message = ""
                if not len(hetio_matching_drugs) > 0:
                    message += "drug doesn't exist; "
                if not len(hetio_matching_diseases) > 0:
                    message += "disease doesn't exist;"
                tqdm.write(message)

    print("Writing triplets to graph.txt")
    with open("outputs/graph.txt", "w") as graph_file:
        for hetio_edge in tqdm(hetio_edges):
            head_name, head_kind = hetio_edge.source.name, hetio_edge.source.kind
            tail_name, tail_kind = hetio_edge.destination.name, hetio_edge.destination.kind
            edge_kind = hetio_edge.kind

            if (edge_kind == "treats" or
                edge_kind == "palliates" or
                edge_kind == "treats_inv" or
                edge_kind == "palliates_inv") and \
                    (head_name, tail_name) in repodb_relations:
                tqdm.write("Treats relation exists in repodb, skipping")
                continue

            line = f"{head_name}:{head_kind}\t{edge_kind}\t{tail_name}:{tail_kind}\n"
            graph_file.write(line)
