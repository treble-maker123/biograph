from utils import Node
from utils.logger import log

if __name__ == "__main__":
    first_nodes = Node.deserialize_bunch("outputs/ctd_nodes.checkpoint.json")
    second_nodes = Node.deserialize_bunch("outputs/repodb_nodes.checkpoint.json")

    first_drugs = list(filter(lambda x: x.kind == "Compound", first_nodes))
    second_drugs = list(filter(lambda x: x.kind == "Compound", second_nodes))
    first_diseases = list(filter(lambda x: x.kind == "Disease", first_nodes))
    second_diseases = list(filter(lambda x: x.kind == "Disease", second_nodes))

    drug_overlap = 0

    for hetio_drug in first_drugs:
        for repodb_drug in second_drugs:
            if hetio_drug == repodb_drug:
                drug_overlap += 1

    log.info(f"There are {len(first_drugs)} first drugs, {len(second_drugs)} second drugs, and the overlap is "
             f"{drug_overlap}.")

    disease_overlap = 0

    for repodb_disease in second_diseases:
        for hetio_disease in first_diseases:
            if repodb_disease == hetio_disease:
                disease_overlap += 1

    log.info(
        f"There are {len(first_diseases)} first diseases, {len(second_diseases)} second diseases, and the overlap "
        f"is {disease_overlap}.")
