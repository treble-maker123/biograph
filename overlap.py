from utils import Node
from utils.hetio import NODES_CHECKPOINT as HETIO_NODES_CHECKPOINT
from utils.repodb import NODES_CHECKPOINT as REPODB_NODES_CHECKPOINT
from pdb import set_trace

if __name__ == "__main__":
    hetio_nodes = Node.deserialize_bunch(HETIO_NODES_CHECKPOINT)
    repodb_nodes = Node.deserialize_bunch(REPODB_NODES_CHECKPOINT)

    hetio_drugs = list(filter(lambda x: x.kind == "Compound", hetio_nodes))
    repodb_drugs = list(filter(lambda x: x.kind == "Compound", repodb_nodes))
    hetio_diseases = list(filter(lambda x: x.kind == "Disease", hetio_nodes))
    repodb_diseases = list(filter(lambda x: x.kind == "Disease", repodb_nodes))

    drug_overlap = 0

    for hetio_drug in hetio_drugs:
        for repodb_drug in repodb_drugs:
            if hetio_drug == repodb_drug:
                drug_overlap += 1

    disease_overlap = 0

    for repodb_disease in repodb_diseases:
        for hetio_disease in hetio_diseases:
            if repodb_disease == hetio_disease:
                disease_overlap += 1

    print(f"There are {len(hetio_drugs)} het.io drugs, {len(repodb_drugs)} repoDB drugs, and the overlap is "
          f"{drug_overlap}.")

    print(f"There are {len(hetio_diseases)} het.io diseases, {len(repodb_diseases)} repoDB diseases, and the overlap "
          f"is {disease_overlap}.")
