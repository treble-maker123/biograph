import logging
import sys

logging.basicConfig(level=20,
                    handlers=[logging.StreamHandler(sys.stdout)],
                    format='%(asctime)s [%(levelname)s] %(message)s')

log = logging.getLogger("main")
