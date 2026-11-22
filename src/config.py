# Rutas compartidas por todas las etapas del pipeline.
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DATA_RAW = RAIZ / "data" / "raw"
DB_PATH = RAIZ / "data" / "pipeline.duckdb"
RUTA_CONTRATO = RAIZ / "contracts" / "yellow_schema.yaml"
RUTA_ZONAS = DATA_RAW / "zones" / "taxi_zone_lookup.csv"
RUTA_REGLAS = RAIZ / "contracts" / "reglas_calidad.yaml"
DATA_CURADO = RAIZ / "data" / "curado"
DIR_SQL = RAIZ / "sql"
DIR_TMP = RAIZ / "data" / ".tmp"
DIR_ESTADO_DLT = RAIZ / "data" / ".dlt"
DIR_LOGS = RAIZ / "data" / "logs"
DIR_DAGSTER = RAIZ / "data" / "dagster_home"
