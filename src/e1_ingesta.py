# Ingesta: dlt extrae los archivos de la TLC y los deja en data/raw; se calcula el hash y se registra todo en ingestion_log.
import argparse
import hashlib
import os
import shutil
import sys
import time
from datetime import datetime
from functools import partial
from pathlib import Path

import dlt
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

from src.config import DATA_RAW, DB_PATH, DIR_ESTADO_DLT, DIR_TMP, RUTA_ZONAS

# Configuración de dlt. Son valores por defecto: una variable de entorno los pisa.
os.environ.setdefault("SCHEMA__NAMING", "direct")             # no cambiar los nombres de las columnas
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")   # no enviar telemetría
os.environ.setdefault("RUNTIME__LOG_LEVEL", "WARNING")        # en Docker se sube a INFO para ver todo lo que hace dlt
os.environ.setdefault("DATA_WRITER__DISABLE_COMPRESSION", "true")   # que el CSV de zonas quede como .csv y no .csv.gz
os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")        # no guardar una segunda copia de lo cargado

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

URL_ZONAS = f"{BASE_URL}/misc/taxi_zone_lookup.csv"

# Dónde escribe dlt cada tabla, relativo a data/. year y month los pone cada corrida.
LAYOUT_VIAJES = "{table_name}/year={year}/month={month}/yellow_tripdata_{year}-{month}.{ext}"
LAYOUT_ZONAS = "{table_name}/taxi_zone_lookup.{ext}"
LOTE_FILAS = 100_000


def armar_url(anio: int, mes: int) -> str:
    return f"{BASE_URL}/trip-data/yellow_tripdata_{anio}-{mes:02d}.parquet"


def armar_ruta(anio: int, mes: int) -> Path:
    nombre = f"yellow_tripdata_{anio}-{mes:02d}.parquet"
    return DATA_RAW / "yellow" / f"year={anio}" / f"month={mes:02d}" / nombre


def calcular_hash(ruta: Path) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        while bloque := f.read(1024 * 1024):
            h.update(bloque)
    return h.hexdigest()


def descargar(url: str, destino: Path) -> tuple[int, str]:
    # Baja la url a destino. Devuelve (bytes descargados, hash sha256).
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_name(destino.name + ".part")

    h = hashlib.sha256()
    total = 0
    with requests.get(url, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        with open(temporal, "wb") as f:
            for bloque in resp.iter_content(chunk_size=1024 * 1024):
                f.write(bloque)
                h.update(bloque)
                total += len(bloque)

    os.replace(temporal, destino)
    return total, h.hexdigest()


def es_transitorio(error: requests.RequestException) -> bool:
    # Un 5xx es problema del servidor, probablemente pasajero.
    if isinstance(error, requests.HTTPError):
        return error.response.status_code >= 500
    # Cortes de conexión, timeouts y descargas interrumpidas.
    return isinstance(error, (
        requests.ConnectionError,
        requests.Timeout,
        requests.exceptions.ChunkedEncodingError,
    ))


def descargar_con_reintentos(url, destino, reintentos=3, espera_base=2):
    for intento in range(reintentos + 1):          # 1 intento + 3 reintentos
        try:
            return descargar(url, destino)
        except requests.RequestException as error:
            if not es_transitorio(error) or intento == reintentos:
                raise
            espera = espera_base * 2 ** intento    # 2, 4, 8 segundos
            print(f"Intento {intento + 1} falló ({error}). Reintento en {espera}s")
            time.sleep(espera)


# --- carga con dlt -------------------------------------------------------------------------------------

def pistas_timestamp(esquema: pa.Schema) -> dict:
    # dlt trata por defecto los timestamps como UTC. Los de la TLC no tienen zona horaria,
    # y si dlt les pone una, DuckDB los corre varias horas al leerlos. Se le avisa que no la tienen.
    return {
        campo.name: {"data_type": "timestamp", "timezone": False}
        for campo in esquema
        if pa.types.is_timestamp(campo.type) and campo.type.tz is None
    }


def marcar_carpeta_raw() -> None:
    # dlt reconoce su carpeta de datos por un archivo 'init'. Si data/raw ya existía sin él (de corridas
    # anteriores, o creada a mano), dlt se cae con FileExistsError al querer crearla. Con el archivo
    # puesto, la adopta como propia y no toca lo que ya hay adentro.
    marca = DATA_RAW / "init"
    if DATA_RAW.exists() and not marca.exists():
        marca.write_text('{"initial_version": 2, "current_version": 2}', encoding="utf-8")


def pipeline_dlt(nombre: str, layout: str, **valores):
    # Un pipeline de dlt que escribe archivos en data/raw con el layout indicado.
    # Cada archivo tiene su propio pipeline, sin estado de corridas anteriores: dlt va acumulando el esquema
    # de todo lo que carga en una tabla, y si el de 2024 compartiera pipeline con el de 2025 saldría con una
    # columna cbd_congestion_fee inventada, llena de nulos.
    shutil.rmtree(DIR_ESTADO_DLT / nombre, ignore_errors=True)
    marcar_carpeta_raw()
    destino = dlt.destinations.filesystem(
        bucket_url=DATA_RAW.parent.as_uri(),
        layout=layout,
        extra_placeholders=valores,
        warn_unsafe_layout_separators=False,
    )
    return dlt.pipeline(
        pipeline_name=nombre,
        pipelines_dir=str(DIR_ESTADO_DLT),
        destination=destino,
        dataset_name=DATA_RAW.name,
        restore_from_destination=False
    )


def correr_dlt(pipeline, recurso, formato: str) -> None:
    # Ejecuta la carga, deja una línea de resumen y limpia el estado local de dlt, que no hace falta guardar.
    inicio = time.time()
    try:
        info = pipeline.run(recurso, loader_file_format=formato)
        tablas = pipeline.last_trace.last_normalize_info.row_counts
        filas = {tabla: n for tabla, n in tablas.items() if not tabla.startswith("_dlt")}
        print(f"   dlt [{pipeline.pipeline_name}]: filas {filas}, load_id {info.loads_ids[0]}, "
              f"{time.time() - inicio:.1f} s")
    finally:
        shutil.rmtree(DIR_ESTADO_DLT / pipeline.pipeline_name, ignore_errors=True)


def cargar_viajes(url: str, destino: Path, anio: int, mes: int) -> dict:
    # Baja el mes (con reintentos) y se lo da a dlt, que lo escribe en data/raw/yellow/year=/month=/.
    temporal = DIR_TMP / destino.name
    bytes_fuente, hash_fuente = descargar_con_reintentos(url, temporal)
    try:
        with pq.ParquetFile(temporal) as origen:
            filas_fuente = origen.metadata.num_rows

            @dlt.resource(name="yellow", write_disposition="append")
            def viajes():
                for lote in origen.iter_batches(batch_size=LOTE_FILAS):   # de a lotes: no entra todo en memoria
                    yield lote

            viajes.apply_hints(columns=pistas_timestamp(origen.schema_arrow))
            pipeline = pipeline_dlt(f"ingesta_yellow_{anio}_{mes:02d}", LAYOUT_VIAJES,
                                    year=anio, month=f"{mes:02d}")
            correr_dlt(pipeline, viajes, "parquet")
    finally:
        temporal.unlink(missing_ok=True)
    return {"bytes_fuente": bytes_fuente, "hash_fuente": hash_fuente, "filas_fuente": filas_fuente}


def cargar_zonas(url: str, destino: Path) -> dict:
    # Igual para la tabla de zonas, que es un CSV chico.
    temporal = DIR_TMP / destino.name
    bytes_fuente, hash_fuente = descargar_con_reintentos(url, temporal)
    try:
        # Sin valores nulos implícitos: 'N/A' es un nombre de zona real, no un dato faltante.
        opciones = pacsv.ConvertOptions(null_values=[], strings_can_be_null=False)
        tabla = pacsv.read_csv(temporal, convert_options=opciones)

        @dlt.resource(name="zones", write_disposition="append")
        def zonas():
            yield tabla

        correr_dlt(pipeline_dlt("ingesta_zonas", LAYOUT_ZONAS), zonas, "csv")
    finally:
        temporal.unlink(missing_ok=True)
    return {"bytes_fuente": bytes_fuente, "hash_fuente": hash_fuente, "filas_fuente": tabla.num_rows}


# --- control: log, idempotencia ------------------------------------------------------------------------

def conectar(ruta_db: Path = DB_PATH):
    # Abre la base y crea el log si todavía no existe.
    ruta_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(ruta_db))
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            archivo       VARCHAR,
            url           VARCHAR,
            filas         BIGINT,
            tamano_bytes  BIGINT,
            sha256        VARCHAR,
            fecha_hora    TIMESTAMP,
            estado        VARCHAR,
            mensaje       VARCHAR
        )
    """)
    return con


def contar_filas(con, ruta: Path) -> int:
    lector = "read_parquet" if ruta.suffix == ".parquet" else "read_csv"
    return con.execute(f"SELECT count(*) FROM {lector}(?)", [str(ruta)]).fetchone()[0]


def registrar(con, archivo, url, filas, tamano_bytes, sha256, estado, mensaje=""):
    con.execute(
        "INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [archivo, url, filas, tamano_bytes, sha256, datetime.now(), estado, mensaje],
    )


def ingestar_archivo(con, url: str, destino: Path, cargar) -> str:
    # Devuelve "omitido" o "descargado". Siempre deja una fila en ingestion_log.
    # 'cargar' es la función que baja el archivo y lo carga con dlt (cargar_viajes o cargar_zonas).
    archivo = destino.name

    ultimo_ok = con.execute(
        "SELECT sha256 FROM ingestion_log "
        "WHERE archivo = ? AND estado = 'ok' ORDER BY fecha_hora DESC LIMIT 1",
        [archivo],
    ).fetchone()

    # Idempotencia: si ya lo tengo y es idéntico a lo que anoté, no hago nada.
    if destino.exists() and ultimo_ok is not None:
        hash_local = calcular_hash(destino)
        if hash_local == ultimo_ok[0]:
            registrar(con, archivo, url, contar_filas(con, destino),
                      destino.stat().st_size, hash_local, "ok",
                      "omitido: ya existía y el hash coincide")
            return "omitido"

    existia = destino.exists()
    try:
        info = cargar(url, destino)
        if not destino.exists():
            raise FileNotFoundError(f"dlt terminó pero no dejó {destino}")
        filas = contar_filas(con, destino)
        if filas != info["filas_fuente"]:
            raise ValueError(f"filas de origen {info['filas_fuente']} distintas de las cargadas {filas}")
    except Exception as error:
        registrar(con, archivo, url, None, None, None, "error", str(error))
        raise

    hash_nuevo = calcular_hash(destino)
    mensaje = (f"cargado con dlt (fuente: {info['bytes_fuente']} bytes, sha256 {info['hash_fuente'][:12]})"
               + (" (reemplazó un archivo previo)" if existia else ""))
    registrar(con, archivo, url, filas, destino.stat().st_size, hash_nuevo, "ok", mensaje)
    return "descargado"


def parsear_mes(texto: str) -> tuple[int, int]:
    # Convierte "2025-03" en (2025, 3). Si el formato está mal, argparse corta con un mensaje claro.
    try:
        anio, mes = (int(x) for x in texto.split("-"))
        if not 1 <= mes <= 12:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"mes inválido '{texto}'. Usá el formato AAAA-MM, por ejemplo 2025-03"
        )
    return anio, mes


def ingestar(con, anio: int, mes: int) -> None:
    # Carga (si hace falta) la tabla de zonas, el mes pedido y el mismo mes del año anterior.
    pedidos = [
        (URL_ZONAS, RUTA_ZONAS, cargar_zonas),
        (armar_url(anio, mes), armar_ruta(anio, mes), partial(cargar_viajes, anio=anio, mes=mes)),
        (armar_url(anio - 1, mes), armar_ruta(anio - 1, mes),
         partial(cargar_viajes, anio=anio - 1, mes=mes)),   # mismo mes del año anterior
    ]
    for url, destino, cargar in pedidos:
        resultado = ingestar_archivo(con, url, destino, cargar)
        print(f"{destino.name}: {resultado}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta de viajes de taxi amarillo (TLC).")
    parser.add_argument("--mes", required=True, type=parsear_mes,
                        help="mes asignado, formato AAAA-MM")
    args = parser.parse_args(argv)
    anio, mes = args.mes

    con = conectar()
    try:
        ingestar(con, anio, mes)
    except Exception as error:
        print(f"ERROR en la ingesta: {error}", file=sys.stderr)
        return 1
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
