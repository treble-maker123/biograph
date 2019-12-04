import bz2
import json
from typing import Dict

import pandas as pd
from pronto import Ontology

from utils.logger import log

UMLS_FILE_PATH = "data/MRCONSO.RRF"

def load_umls() -> pd.DataFrame:
    log.info("Loading UMLS file.")
    columns = ["CUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF", "AUI", "SAUI", "SCUI", "SDUI", "SAB", "TTY", "CODE",
               "STR", "SRL", "SUPPRESS", "CVF", "MISC"]

    umls = pd.read_csv(UMLS_FILE_PATH, delimiter="|", names=columns)

    log.info("Finished loading UMLS file.")
    # Only return the English records
    return umls[umls["LAT"] == "ENG"]


def load_hetio(file_path: str) -> Dict:
    with bz2.open(file_path) as file:
        return json.load(file)


def load_repodb(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def load_disease_ontology() -> Ontology:
    return Ontology("http://purl.obolibrary.org/obo/doid.obo")


def load_gene_ontology() -> Ontology:
    return Ontology("http://purl.obolibrary.org/obo/go.obo")
