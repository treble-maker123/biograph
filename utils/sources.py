import pandas as pd
from pronto import Ontology
import os
import sqlite3


from utils.logger import log

UMLS_FILE_PATH = "data/MRCONSO.RRF"
UMLS_DB_NAME = "data/umls.sqlite"


def load_umls() -> str:
    log.info("Loading UMLS file.")

    columns = ["CUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF", "AUI", "SAUI", "SCUI", "SDUI", "SAB", "TTY", "CODE",
               "STR", "SRL", "SUPPRESS", "CVF", "MISC"]

    if not os.path.exists(UMLS_DB_NAME):
        with sqlite3.connect(UMLS_DB_NAME) as conn:
            umls = pd.read_csv(UMLS_FILE_PATH, delimiter="|", names=columns, index_col=False)
            umls.to_sql(name="umls", con=conn, if_exists="replace")

    log.info("Finished loading UMLS file.")

    return UMLS_DB_NAME


def load_disease_ontology() -> Ontology:
    return Ontology("http://purl.obolibrary.org/obo/doid.obo")


def load_gene_ontology() -> Ontology:
    return Ontology("http://purl.obolibrary.org/obo/go.obo")
