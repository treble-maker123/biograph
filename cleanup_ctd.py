from pdb import set_trace
from random import shuffle
from math import floor
import json

if __name__ == "__main__":
    graph_path = "outputs/ctd_graph.txt"
    test_path = "outputs/ctd_test.txt"
    entity_vocab_path = "outputs/ctd_repodb/entity_vocab.json"
    relation_vocab_path = "outputs/ctd_repodb/relation_vocab.json"
    output_graph_path = "outputs/ctd_repodb/graph.txt"
    output_train_path = "outputs/ctd_repodb/train.txt"
    output_dev_path = "outputs/ctd_repodb/dev.txt"
    output_test_path = "outputs/ctd_repodb/test.txt"

    with open(graph_path, "r") as file:
        graph = file.readlines()

    with open(test_path, "r") as file:
        test = file.readlines()

    # remove duplicates
    graph = list(set(graph))
    test = list(set(test))

    # get vocab file
    all_triplets = graph + test
    triplets = list(map(lambda x: x.strip().split('\t'), all_triplets))
    head, rel, tail = list(zip(*triplets))
    entities = list(set(head + tail))
    relations = list(set(rel))
    entity_dict = {ent: idx for idx, ent in enumerate(entities)}
    relation_dict = {rel: idx for idx, rel in enumerate(relations)}

    with open(entity_vocab_path, "w") as file:
        json.dump(entity_dict, file)

    with open(relation_vocab_path, "w") as file:
        json.dump(relation_dict, file)

    # train-dev-test split
    train = list(filter(lambda x: x.split("\t")[1] == "treats", graph))
    test_treats = list(filter(lambda x: x.split("\t")[1] == "treats", test))
    test_not_treats = list(filter(lambda x: x.split("\t")[1] == "not_treats", test))

    # randomly splitting triplets between dev and test ensuring proportional distribution of treats and not_treats
    # ideally want a good distribution of nodes in both sets as well, but not everything can go ideally eh?
    shuffle(test_treats)
    shuffle(test_not_treats)

    num_treats_dev = floor(0.4 * len(test_treats))
    num_not_treats_dev = floor(0.4 * len(test_not_treats))

    dev = test_treats[:num_treats_dev] + test_not_treats[:num_not_treats_dev]
    test = test_treats[num_treats_dev:] + test_not_treats[num_not_treats_dev:]

    assert len(dev + test) == len(test_treats + test_not_treats)

    with open(output_graph_path, "w") as file:
        file.writelines(graph)

    with open(output_train_path, "w") as file:
        file.writelines(train)

    with open(output_dev_path, "w") as file:
        file.writelines(dev)

    with open(output_test_path, "w") as file:
        file.writelines(test)
