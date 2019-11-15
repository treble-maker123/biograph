"""
Nodes are from Comparative Toxicgenomic Dataset
http://ctdbase.org/downloads/

Chemical vocabulary: http://ctdbase.org/reports/CTD_chemicals.csv.gz
Disease vocabulary: http://ctdbase.org/reports/CTD_diseases.csv.gz
Gene vocabulary: http://ctdbase.org/reports/CTD_genes.csv.gz
Pathway vocabulary: http://ctdbase.org/reports/CTD_pathways.csv.gz

------------------------------------------------------------------------------------------------------------------------

Edges are from Comparative Toxicgenomic Dataset
http://ctdbase.org/downloads/

Chemical-gene: http://ctdbase.org/reports/CTD_chem_gene_ixns.csv.gz
Chemical–disease: http://ctdbase.org/reports/CTD_chemicals_diseases.csv.gz
Gene–disease: http://ctdbase.org/reports/CTD_genes_diseases.csv.gz (HUGE FILE, 1GB)
Gene–pathway: http://ctdbase.org/reports/CTD_genes_pathways.csv.gz
Disease–pathway: http://ctdbase.org/reports/CTD_diseases_pathways.csv.gz
"""
from utils import log, Node
from utils.ctd import build_chemical_nodes, build_disease_nodes, build_gene_nodes, build_pathway_nodes, NODES_CHECKPOINT


if __name__ == "__main__":
    log.info("Building chemical nodes.")
    chemical_nodes = build_chemical_nodes()

    log.info("Building disease nodes.")
    disease_nodes = build_disease_nodes()

    log.info("Building gene nodes.")
    gene_nodes = build_gene_nodes()

    log.info("Building pathway nodes.")
    pathway_nodes = build_pathway_nodes()

    log.info("Check-pointing all CTD nodes into one file")
    combined_nodes = chemical_nodes + disease_nodes + gene_nodes + pathway_nodes

    Node.serialize_bunch(combined_nodes, NODES_CHECKPOINT)
