# Rutas compartidas por todas las etapas del pipeline.
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DATA_RAW = RAIZ / "data" / "raw"
DB_PATH = RAIZ / "data" / "pipeline.duckdb"
RUTA_CONTRATO = RAIZ / "contracts" / "yellow_schema.yaml"