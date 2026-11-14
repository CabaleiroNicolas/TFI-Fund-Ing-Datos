# Ingesta: descarga los archivos crudos de la TLC sin modificarlos.
import hashlib
from pathlib import Path
import os
import requests
import time
import duckdb
from datetime import datetime
import argparse
import sys
from src.config import DATA_RAW, DB_PATH, RUTA_ZONAS

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
URL_ZONAS = f"{BASE_URL}/misc/taxi_zone_lookup.csv"


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

def descargar(url: str, destino: Path) -> tuple[int, str]: #bytes, hash
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


def ingestar_archivo(con, url: str, destino: Path) -> str:
    # Devuelve "omitido" o "descargado". Siempre deja una fila en ingestion_log.
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
        tamano, hash_nuevo = descargar_con_reintentos(url, destino)
        filas = contar_filas(con, destino)
    except Exception as error:
        registrar(con, archivo, url, None, None, None, "error", str(error))
        raise

    mensaje = "descargado" + (" (reemplazó un archivo previo)" if existia else "")
    registrar(con, archivo, url, filas, tamano, hash_nuevo, "ok", mensaje)
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
    # Descarga (si hace falta) la tabla de zonas, el mes pedido y el mismo mes del año anterior.
    pedidos = [
        (URL_ZONAS, RUTA_ZONAS),
        (armar_url(anio, mes), armar_ruta(anio, mes)),
        (armar_url(anio - 1, mes), armar_ruta(anio - 1, mes)),   # mismo mes del año anterior
    ]
    for url, destino in pedidos:
        resultado = ingestar_archivo(con, url, destino)
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