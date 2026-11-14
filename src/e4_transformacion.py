# Transformación: arma la capa curada de un mes y la escribe como Parquet particionado (estilo Hive).
import os

from src.config import DATA_CURADO, DIR_SQL
from src.e1_ingesta import armar_ruta
from src.e3_calidad import ErrorReconciliacion

RUTA_SQL_CURADO = DIR_SQL / "transformacion" / "curado.sql"


def ruta_curado(anio: int, mes: int, raiz=DATA_CURADO):
    # Un archivo por partición: <raiz>/year=AAAA/month=M/curado.parquet
    # El mes va sin cero a la izquierda: con "month=03" DuckDB lo lee como texto, con "month=3" como número.
    return raiz / f"year={anio}" / f"month={mes}" / "curado.parquet"


def contar_aceptadas(con, archivo: str) -> int:
    return con.execute("SELECT count(*) FROM aceptadas WHERE archivo_origen = ?", [archivo]).fetchall()[0][0]


def contar_parquet(con, ruta) -> int:
    # Filas de un archivo Parquet, leídas de sus metadatos (no recorre los datos).
    return con.execute(
        "SELECT count(*) FROM read_parquet(?, hive_partitioning = false)", [str(ruta)]
    ).fetchall()[0][0]


def verificar_curado(esperadas: int, escritas: int, archivo: str) -> None:
    # La capa curada tiene que tener tantas filas como aceptadas del mes.
    if esperadas != escritas:
        raise ErrorReconciliacion(
            f"{archivo}: aceptadas {esperadas} distinto de filas curadas {escritas} "
            f"(diferencia {escritas - esperadas})"
        )


def curar_mes(con, anio: int, mes: int, raiz=DATA_CURADO) -> int:
    # Escribe la partición del mes. Se escribe a un archivo .part, se verifica, y recién ahí
    # reemplaza a la partición anterior: si algo falla, la partición vieja queda como estaba.
    # Requiere la tabla 'aceptadas' y la vista 'zonas' en la conexión.
    archivo = armar_ruta(anio, mes).name
    esperadas = contar_aceptadas(con, archivo)
    if esperadas == 0:
        raise ValueError(f"{archivo}: no hay filas aceptadas para curar (¿se corrió la etapa de calidad?)")

    destino = ruta_curado(anio, mes, raiz)
    temporal = destino.with_name(destino.name + ".part")
    destino.parent.mkdir(parents=True, exist_ok=True)

    consulta = RUTA_SQL_CURADO.read_text(encoding="utf-8")
    try:
        con.execute(f"COPY ({consulta}) TO '{temporal}' (FORMAT parquet, COMPRESSION zstd)", [archivo])
        escritas = contar_parquet(con, temporal)
        verificar_curado(esperadas, escritas, archivo)
        os.replace(temporal, destino)
    except BaseException:
        temporal.unlink(missing_ok=True)
        raise
    return escritas


def contar_particiones(con, raiz=DATA_CURADO) -> list:
    # Filas de cada partición de la capa curada: [(year, month, filas), ...]
    if not any(raiz.glob("year=*/month=*/curado.parquet")):
        return []
    patron = str(raiz / "year=*" / "month=*" / "curado.parquet")
    return con.execute("""
        SELECT year, month, count(*)
        FROM read_parquet(?, hive_partitioning = true)
        GROUP BY year, month ORDER BY year, month
    """, [patron]).fetchall()